TODO this doc is WIP, will eventually include lots of interactive examples and diagrams which will make it much easier to follow

Dida is heavily based on the ideas behind [differential dataflow](https://github.com/TimelyDataflow/differential-dataflow/) and is in part an attempt to better explain those ideas.

The goal of this doc is to explain the design constraints that lead to building something like dida and what kinds of problems dida is useful for. We'll begin with a very simple problem and add the design constraints one at a time to show how they direct the design down this path. And we'll stick to the high-level intuition - see [dida.core](../lib/dida/core.zig) for implementation details.

## Problem

The starting point is wanting to __incrementally__ update the results of __structured__ computations over __multisets__.

* A __multiset__ (aka bag) is an unordered collection of elements. Unlike sets, they may contain multiple copies of a given element. The most familiar example of a multiset for most people is probably a SQL table.
* __Structured__ computation means that the computation we care about is written in terms of a graph of multiset operations (map, reduce, join etc) as opposed to be written in an arbitrary general purpose programming language ([cf](https://scattered-thoughts.net/writing/an-opinionated-map-of-incremental-and-streaming-systems#unstructured-vs-structured)).
* __Incrementally__ updating the result means that when the input collections changes slightly (eg some new data arrives) we can calculate the change to the output collections much faster than rerunning the whole computation from scratch.

Structured computation over multisets covers a range of applications including SQL, datalog, dataframes, graph computations, in-database machine learning ([eg](https://arxiv.org/pdf/1703.04780.pdf)), probabalistic programming, CRDTs ([eg](https://martin.kleppmann.com/2018/02/26/dagstuhl-data-consistency.html)), specifying distributed systems ([eg](https://www.youtube.com/watch?v=R2Aa4PivG0g)), generating UI ([eg](https://scattered-thoughts.net/writing/relational-ui/)) and a surprising range of fundamental CS algorithms ([eg](https://dl.acm.org/doi/pdf/10.1145/2902251.2902280)).

Incrementally updating these computations reduces the latency between adding new inputs and seeing the updated outputs. Using tools like [ksqldb](https://ksqldb.io/), [flink](https://github.com/apache/flink) and [materialize](https://materialize.com/), computations that used to be run as batch jobs overnight can now be kept up to date with sub-second latencies using the same resources. And if you squint a bit, a lot of the work that goes into building a web or mobile app today from the backend database to the app server to the frontend UI all kind of looks like a pile of adhoc solutions to one big incremental update problem ([eg](https://martin.kleppmann.com/2015/03/04/turning-the-database-inside-out.html), [eg](https://engineering.fb.com/2020/03/02/data-infrastructure/messenger/), [eg](https://tonsky.me/blog/the-web-after-tomorrow/#always-late)).

So it seems like there could be a lot of value to figuring out how to solve these kinds of problems efficiently and with minimum added complexity.

## Solution

We can represent collections as a list of changes:

``` js
// Add one copy of "alice" to the collection
("alice", +1)

// Remove two copies of "bob" from the collection
("bob", -2)
```

To recover the current value of the collections we just add up all the changes that we've seen so far.

``` js
collections = sum_changes(changes)
```

Next, we have to transform our operations that work on collections into operations that work on lists of changes. A correct transformation must obey this rule:

``` js
assert(
    sum_changes(incremental_operator(changes))
    ==
    batch_operator(sum_changes(changes))
)
```

For some operations this transformation is easy eg `map` can just apply it's map function to the data in each change as usual.

``` js
function incremental_map(f, changes) {
    for (change in changes) {
        let (data, diff) = change;
        yield (f(data), diff);
    }
}
```

Other operations like `sum` have to maintain some internal state to keep track of the changes they've seen so far.

``` js
function incremental_sum(changes) {
    var total = 0;
    for (change in changes) {
        let (data, diff) = change;
        let old_total = total;
        total += data * diff;
        // delete the old total
        yield (old_total, -1);
        // add the new total
        yield (total, 1);
    }
}
```

And finally operations like `join` have to maintain indexes for each input. A simple implementation of an index could just be a list of changes sorted by key.

``` js
function incremental_join(changes) {
    var left_index = new Index();
    var right_index = new Index();
    for (change in changes) {
        let (input_side, (key, value), diff) = change;
        if (input_side == "left") {
            left_index.update(key, value, diff);
            for ((other_value, other_diff) in right_index.lookup(key) {
                yield ((key, value, other_value), diff * other_diff);
            }
        } else {
            right_index.update(key, value, diff);
            for ((other_value, other_diff) in left_index.lookup(key) {
                yield ((key, other_value, value), diff * other_diff);
            }
        }
    }
}
```

This basic model is pretty well understood and has been implemented in a wide range of systems including ksqldb, flink etc.

## Constraint #1 - internally consistent results

The problem with this basic model is that it in many case it produces incorrect outputs. A single change to an input may produce multiple changes to the results of intermediate computations. If these changes are not carefully synchronized then the resulting stream of output changes can be mostly gibberish. (See [internal consistency in streaming systems](https://scattered-thoughts.net/writing/internal-consistency-in-streaming-systems/) for more details.)

To guarantee that the results are consistent we add __timestamps__, __multiversion indexes__ and __frontiers__.

Each change to the input now must include a timestamp.

``` js
// Add one copy of "alice" to the collection at time 12
("alice", 12, +1)

// Remove two copies of "bob" from the collection at time 42
("bob", 42, -2)
```

These timestamps could be actual real world timestamps (eg unix epochs) or they could just be arbitrary integers that we increment every time we make a new change. Their job is just to keep track of which output changes were caused by which input changes.

Previously we could get the state of a collection by summing up all the changes. Now that we have timestamps, we can get the state of a collection as of any point in time T by summing up all the changes which have timestamps <= T. We need to update our rule of incremental operations accordingly:

``` js
assert(
    sum_changes_up_until(time, incremental_operator(changes))
    ==
    batch_operator(sum_changes_up_until(time, changes))
)
```

This means that incremental operations must now calculate the correct timestamp for their outputs. For most operations the output timestamp is the same as the input timestamp:

``` js
function incremental_sum(changes) {
    var total = 0;
    for (change in changes) {
        let (data, time, diff) = change;
        let old_total = total;
        total += data * diff;
        // delete the old total
        yield (old_total, time, -1);
        // add the new total
        yield (total, time, 1);
    }
}
```

But some operations like `join` require a bit more thought:

``` js
function incremental_join(changes) {
    var left_index = new Index();
    var right_index = new Index();
    for (change in changes) {
        let (input_side, (key, value), time, diff) = change;
        if (input_side == "left") {
            left_index.update(key, value, time, diff);
            for ((other_value, other_time, other_diff) in right_index.lookup(key) {
                // max(time, other_time) is the earliest time at which both of these changes are visible
                yield ((key, value, other_value), max(time, other_time), diff * other_diff);
            }
        } else {
            right_index.update(key, value, time, diff);
            for ((other_value, other_time, other_diff) in left_index.lookup(key) {
                // max(time, other_time) is the earliest time at which both of these changes are visible
                yield ((key, other_value, value), max(time, other_time), diff * other_diff);
            }
        }
    }
}
```

Indexes now need to track not only the latest value of the collection, but all the previous values. The easiest way to do this is to keep a list of all changes, sorted by key and timestamp.

Some operations like `sum` can't produce the correct output for a given timestamp until they've seen all the inputs for that timestamp (as opposed to operations like `map` which can emit outputs immediately for each input). To handle this we need to keep track of each operations __frontier__ - the earliest timestamps that might still appear in the output changes for that operation. Whoever is feeding new changes into the inputs is now also responsible for updating the frontier of the inputs, to tell them when they have seen all the changes for a particular timestamp. And as changes flow through the graph we can also update the frontiers of each operation that they pass through.

(Side note: the more common terminology for a frontier is 'watermark', but 'watermark' is also used to refer to a variety of related concepts including per-operation timeouts for windowed operations and handling of late-arriving data, leading to persistent misunderstandings between different groups of people. Also, as we'll see in a moment, frontiers differ from traditional watermarks once we add iterative computations.)

Frontiers are also useful at the output of the system - downstream consumers can watch the frontier to learn when the have seen all the changes to the output for a given timestamp and can now safely act on the result.

With timestamps, multi-version indexes and frontiers we can build systems that are internally consistent. As a bonus the changes, timestamps and frontiers at the output are exactly the information that is required at the input, so we can take multiple such systems with different internal implementations and they can be composed into a single consistent computation so long as they stick to this format. (Materialize are working on a [protocol](https://materialize.com/docs/connect/materialize-cdc/) that encodes this information in a way that is idempotent and confluent, so you don't even need to have a reliable or ordered connection between systems.)

## Constraint #2 - iterative computations

TODO this whole section really needs the interactive examples in order to make any sense :)

Iterative computations show up all over the place eg recursive CTEs in sql, recursive rules in datalog, graph algorithms like pagerank, dataflow analsis in compilers. Without iterative computation our system would be very limited, not even Turing-complete. But combining interative computations with the ability to delete inputs makes for a fundamentally difficult incremental update problem. This problem has been well studied in the context of datalog and there are no completely satisfying solutions

In dida and differential dataflow, iterative computations are expressed by taking a set of starting collections and repeatedly applying some transformation to them.

``` js
function loop(f, collections) {
    while (true) {
        collections = f(collections);
    }
    return collections;
}
```

As written above, this computation would never terminate. But since we're using incremental operations we only have to calculate the change between each iteration. So as long as the collections only change at a finite number of iterations, the incremental version will terminate.

The first impact of adding loops is that timestamps become more complicated. Previously, for each change produced by a node we only had to track which input changes produced it. Now when we're dealing with a change inside the loop we also have to track of which loop iteration produced it. We can do this by adding an extra coordinate to the timestamp.

``` js
// Inserted one copy of "alice" at input time 12 on the 7th interation of the loop
("alice", (12, 7), +1)
```

If we need to nest loops, we just keep adding more coordinates. At the output nodes of each loop we strip off the extra timestamp coordinate so that the rest of the system doesn't see the internal state of the loop.

Previously we could get the state of a collection as of any point in time T by summing up all the changes which have timestamps <= T. But what does `<=` mean when our timestamps have multiple coordinates?

There isn't a unique answer to this. We can choose various different orderings for our timestamps and they will produce different incremental operations. But when calculating the change at time T an incremental operation can only make use of changes with timestamps <= T. So we should choose an ordering that gives as much useful information as possible and removes as much useless information at possible.

Suppose we are looking at, say, the 3rd iteration of a loop for input time 7. If the changes between times are small, then the 3rd iteration at input time 7 will probably look a lot like the 3rd iteration at input time 6. And the 3rd iteration of any loop certainly needs to know about what happened in the 2nd iteration. But it probably can't make use of any information about the 4th iteration at time 6. So we should have `(6,3) < (7,3)` and `(7,2) < (7,3)` but not `(6,4) < (7,3)`. This suggests that the correct rule is that a timestamp T0 is less than or equal to timestamp T1 when all of the coordinates of T0 are less than or equal to the coordinates of T1. I've been calling this the __causal order__, because it mirrors the possible causality between changes.

Apart from this new definition of `<=`, the rule for incremental operations remains unchanged:

``` js
assert(
    sum_changes_up_until(time, incremental_operator(changes))
    ==
    batch_operator(sum_changes_up_until(time, changes))
)
```

This change is enough to make loops just work. Most operations can process changes in any old order and the few that need to wait can rely on frontiers. Frontiers also lets us know when a loop has finished for a given input time - that input time will vanish from the frontier of the loops output nodes once no more changes can be produced.

Frontiers however now become more complicated.

Firstly, suppose some node might produce changes in the future at times `(0,1)` and `(1,0)`. What should the frontier be? Neither of those timestamps is less than the other. So frontiers have to contain a set of earliest timestamps, rather than just a single timestamp.

Secondly, keeping frontiers up to date is harder given that the graph may now contain cycles. We can model the problem by representing frontiers as multisets. The frontier above would be:

``` js
// 1 copy of the timestamp (0,1)
(0,1) => 1
// 1 copy of the timestamp (1,0)
(1,0) => 1
```

The frontier tracks "what are the earliest timestamps that might appear on changes emitted from this node". So to calculate the frontier at a given node, we need to know:

* The frontiers of the nodes immediately upstream, because they might send changes that this node needs to handle.
* The timestamps of any changes that are waiting to be processed at this node.
* If this node is an input node, we also need the user to explicitly tell us what new inputs they might send in the future by setting the input frontier.

Given this information it's easy to calculate the frontier at this node by considering what effect this node has on timestamps. Most nodes don't change timestamps at all, so the output frontier will be the same as the upstream frontier. Nodes like `union` have multiple upstreams so they have to take the minimum of their upstream frontiers. The 'timestamp_increment' node adds 1 to the last coordinate of it's upstream's timestamps, so it must also add 1 to the last coordinate of it's upstream frontier. And so on.

As the input frontiers are advanced and as changes flow around the graph, the frontiers will also change. Recomputing all the frontiers from scratch every time we process a change would be too slow. So we have an incremental maintenance problem that involves iterative computations on multisets. We know how to solve this problem! Just add timestamps and frontiers... uh oh.

This has to bottom out somewhere. We need to find a different solution to the frontier computation than the one that produced it in the first place.

Suppose we just kept a list of changes to frontiers and applied them one by one to downstream frontiers. What could go wrong?

We can get into trouble when the graph of changes in a loop becomes self-supporting. Suppose we have a situation like:

``` js
node A = input
node B = union(A, timestamp_increment(C))
node C = map(f, B)

node A frontier:
(0, 0) => 1

node B frontier:
(0, 0) => 1

node C frontier:
(0, 0) => 1
```

If we advance the input node A it will produce these changes:

```
pending changes:
((0, 0), -1) at A
((1, 0), +1) at A
```

After applying those changes, we'd like to end up with:

```
node A frontier:
(1, 0) => 1

node B frontier:
(1, 0) => 1

node C frontier:
(1, 0) => 1
```

But here is what can happen if we do this naively. First we process `((0,0), -1) at A`. This changes the frontier at A, producing a change that needs to be applied downstream at B.

```
pending changes:
((1, 0), +1) at A
((0, 0), -1) at B

node A frontier:

node B frontier:
(0, 0) => 1

node C frontier:
(0, 0) => 1
```

Suppose we process `((0, 0), -1) at B` at B next. B can see that the frontier from A is now empty, and the frontier from C contains `(0, 0) => 1`. So the frontier at B should update to `(0, 1) => 1` to reflect the fact that message with timestamp `(0,0)` might come from C and pass through the `timestamp_increment`.

```
pending changes:
((1, 0), +1) at A
((0, 0), -1) at C
((0, 1), +1) at C

node A frontier:

node B frontier:
(0, 1) => 1

node C frontier:
(0, 0) => 1
```

Next we handle the changes `((0, 0), -1) at C` and `((0, 1), +1) at C`. This advances the frontier at C and produces a new pending change for B.

```
pending changes:
((1, 0), +1) at A
((0, 1), -1) at B
((0, 2), +1) at B

node A frontier:

node B frontier:
(0, 2) => 1

node C frontier:
(0, 1) => 1
```

By this point, you can probably see where this is going. The inserts and deletes can race each other around this cycle in the graph generating later and later timestamps. As long as the deletes never catch up to the inserts this algorithm won't even terminate, let alone produce the correct frontiers.

One way to avoid this is to always process changes in __causal order__. If a timestamp T0 at operation Op0 could produce a timestamp T2 at operation Op1, then we must handle T1 first. If we can cheaply compute this order then we can just sort the list of outstanding changes in this order and pop off one change at a time. In the example above the deletions are always at earlier timestamps than the insertions, so we process the deletions all the way around the loop first and they catch up to and cancel out the insertions.

The question now is how do we compute the causal order?

In fact, back up, does the causal order even exist? What if we had a timestamp T0 at that operation Op0 that could produce a timestamp T1 at operation Op1, but timestamp T1 at operation Op1 could also produce timestamp T0 at operation Op0? After all, our graph of operations does contain cycles.

The solution to this is to place some constraints on what kinds of graphs we'll allow. The most important constraints are:

* The output changes at any operation in the graph must not contain timestamps which are earlier than the input change that produced them (ie time must not go backwards).
* Any backwards edge in the graph must increment the timestamps of the changes that flow across it (ie time in loops must go forwards).

Together, these two constraints prevent the situation where two changes can both potentially cause each other. (See [Graph.validate](https://github.com/jamii/dida/search?q=%22fn+validate%28self%3A+Graph%22) for the full list of constraints.)

With those constraints in place, we can use the following ordering:

1. Process changes with earlier timestamps first.
2. If two changes have the same timestamp, process the change that is earlier in the graph first.

(The presence of multiple loops actually makes the ordering a little more subtle - see [orderPointstamps](https://github.com/jamii/dida/search?q=%22fn+orderPointstamps%22) for the gory details.)

So long as we process outstanding changes in this order, we can guarantee that our frontier updates algorithm will terminate.

## Constraint 3 - process changes in batches and in parallel

If processing changes in causal order solves the incremental update problem, why didn't we just do that in the first place instead of messing around with frontiers?

The problem is that it can be very slow if our timestamps are fine-grained. If we have 1 million changes at some node that all have the same timestamp, great, we can process them all at once. But if we have 1 million changes with 1 million different timestamps then we have to process them one by one. In data-processing systems, batching is key to achieving good cache locality and amortizing interpreter overhead. If we have to process changes one at a time it will destroy our performance.

Similarly, if we want to shard up this computation and run it across multiple threads, we can't have all the threads hitting synchronization points after every change while they check to make sure that the other threads don't have any earlier changes that should run first.

So instead we separate the two aspects of the problem. The changes to our data can be processed in large batches, in any order, as if they were using coarse-grained timestamps, until they all pile up at operations that are waiting on frontier updates. Then we switch to updating our frontiers with the one-change-at-a-time method, but instead of tracking every timestamp the frontiers only have to track the smallest timestamps in each batch so it's a much smaller problem. And the frontier update problem is also monotonic (frontiers only ever advance) so it's easy to split across multiple threads in a wait-free fashion - each thread just broadcasts it's frontier changes and if other threads don't receive the changes right away then no problem, their frontiers will advance a little slower then they could have but none of their other work is blocked.

I find this solution very elegant. We start with a problem that appears to require a lot of waiting and synchronization. And then we condense the actual essential waiting down into a much smaller problem and run everything else in big batches in whatever order it shows up in.
