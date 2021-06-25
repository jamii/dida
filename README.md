Dida is a (WIP) library for streaming, incremental, iterative, internally-consistent computation on time-varying collections.

The jargon-free version: You write code that manipulates collections using familiar operations like `map`, `join` and `loop`. You run the code on some input and get some output. Then when the input changes, you get changes to the output, much faster than recomputing the whole thing from scratch. (And the outputs will be [correct](https://scattered-thoughts.net/writing/internal-consistency-in-streaming-systems/)!)

If you want to learn how it works, start at [docs/why.md](./docs/why.md) and then read [lib/dida/core.zig](./lib/dida/core.zig).

## Design

Dida is heavily based on [differential dataflow](https://github.com/TimelyDataflow/differential-dataflow/) and is informed by experience using differential dataflow as a backend at [materialize](https://materialize.com/).

Compared to differential dataflow, dida aims to:

* [ ] Be [easier to use and understand](https://scattered-thoughts.net/writing/why-isnt-differential-dataflow-more-popular/). (Differential dataflow aims to be as flexible, extensible and composable as possible, which makes sense for a research platform but can make the code very difficult to follow.)
   * [ ] Tentatively aim to keep the core under 3kloc. (For comparison timely dataflow and differential dataflow total 14-16kloc depending on which components you count).
   * [x] Only implement as much of [timely dataflow](https://github.com/TimelyDataflow/timely-dataflow/) as is needed to support the features in differential dataflow.
   * [x] Only support timestamps which are products of integers. (Differential dataflow supports timestamps which are arbitary lattices, but I haven't yet seen any uses that can't be encoded as products of integers.)
   * [x] Use a simpler progress tracking algorithm which doesn't require path summaries, multiple outputs per node or internal edges within nodes.
   * [x] Store all state associated with the dataflow in a single flat structure for easy introspection.
   * [ ] Optionally log all actions for easier debugging and vizualization.
   * [ ] Provide well-documented default implementations for common tasks (eg writing output to a file).
* [ ] Better support use as an interpreter backend and for binding to other languages.
  * [ ] Split the api into a data-centric runtime-checked core, and a per-binding-language strongly-typed sugar that helps make dataflows correct-by-construction. (The differential dataflow api provides excellent compile-time safety but is hard to map across FFI into a language with a different type system.)
  * [ ] Don't rely on specialization for performance, since it requires compilation and also doesn't work well cross-language. This will require rethinking eg how functions are lifted over batches.
  * [ ] Support storing data inline in indexes when the size is only known at runtime. (Differential dataflow can support this, but materialize currently stores each row in a separate heap allocation even if the row is all fixed-width types.)
  * [ ] Support reference-counted values without adding overhead for non-reference-counted values. This is needed for eg pinning javascript objects but also helps reduce memory usage in string-heavy dataflows. (Materialize could reference-count eg strings but would then pay for the row-by-row Drop impl on all data, not just string data.)
  * [ ] Support being embedded in another event loop. (Differential dataflow can be run a step at a time, but the amount of work per step is not bounded so it can block the event loop for arbitrary amounts of time.)
  * [ ] Support cpu and memory limits. This makes it much easier to safely support live evaluation eg embedding a query repl in a tracing dashboard.
* [ ] Provide interactive graphical debuggers for every component. (Many of the complaints I've heard about differential dataflow are about struggling to understand where state is stored, when things happen, how different ways of writing a computation affect performance etc. Debuggers can answer this question directly, but I suspect will also help by teaching useful mental representations.)
* [ ] Write a short book that uses the debuggers to explain both the theory of differential dataflow and the details of this implementation. (Differential dataflow suffers from having the theory spread across myriad papers with limited space and which each describe different versions of the algorithms.)

## Implementation

* [ ] Core
  * [ ] Data
    * [ ] Treat data as untyped bytes in the core
  * [x] Timestamps
  * [x] Frontiers
  * [ ] Change batches
    * [x] Sort and coalesce changes
    * [ ] Use columnar storage
    * [ ] Only store one copy of each row, regardless of number of timestamps/diffs
  * [ ] Nodes
    * [x] Input
    * [x] Output
    * [ ] Map
      * [x] Basic logic
      * [ ] Replace with general linear operator
    * [x] TimestampPush/Increment/Pop
    * [x] Union
    * [ ] Index
      * [x] Basic logic
      * [x] Merge layers
      * [ ] Merge layers incrementally
      * [ ] Compact layers
      * [ ] Figure out a cheaper way to maintain frontier support?
    * [ ] Distinct
      * [x] Basic logic
      * [x] Semi-efficient implementation using per-row pending timestamps
      * [ ] Figure out a cheaper way to maintain pending timestamps?
      * [ ] Count, Threshold
    * [ ] Join
      * [x] Basic logic
      * [x] Efficient implementation using merge join
    * [x] Reduce
      * [x] Basic logic
      * [ ] Efficient implementation using better index structure
    * [ ] ReduceAbelian
    * [ ] Exchange
  * [x] Graph
    * [x] Validation
  * [ ] Progress tracking
    * [x] Incremental frontier maintenance
    * [ ] Finer-grained change tracking to avoid empty updates
    * [ ] Use a sorted data-structure for `unprocessed_frontier_updates`
  * [ ] Scheduling
    * [x] Schedule work in a correct order
    * [ ] Figure out how to schedule work for best throughput/latency
    * [ ] Ensure that the runtime of `doWork` is roughly bounded
      * [ ] Enforce a maximum batch size and suspend operations that would produce more than one batch
  * [x] Single-threaded cooperative worker
  * [ ] Multi-threaded workers
    * [ ] Expose as state machine for easy simulation
  * [ ] Memory management
* [ ] Testing
  * [ ] Unit test
    * [x] Timestamp ordering / lub
    * [x] Batch builder
    * [x] Frontier move / order
    * [x] Supported frontier update
    * [x] Batch / index queries
    * [x] Index build
    * [ ] ...
    * [ ] Port property testing / fuzzing framework from imp to replace hand-written unit tests
  * [ ] Unit test that known failure modes for graphs don't validate
  * [ ] Test that random graphs either fail to validate or have no paths where `orderPointstamps(start,end) != .lt`
  * [ ] Test that random progress tracking updates never cause frontiers to go backwards
  * [ ] Test that random reorderings of updates to progress tracker have same final result
  * [ ] Test that random reorderings of inputs to dataflows have same final result
  * [ ] Add debug-mode validation to progress tracker, shard
  * [ ] Integration test against problems with known results (eg TPC)
  * [ ] Enable double free, use after free and memory leak detection for all tests
  * [ ] Figure out how to test bindings
    * [ ] Wasm
    * [ ] Node
* [ ] Bindings
  * [ ] Wasm
  * [ ] Node
    * [x] Basic sketch
    * [ ] Type tags
    * [ ] Exceptions
    * [ ] Memory management
    * [ ] Panic handler
    * [ ] Packaging
* [ ] Sugar
  * [ ] Zig
    * [x] Basic sketch
    * [ ] Automatically add Push/Pop nodes as needed
    * [ ] Add index wrappers for indexes behind Push/Pop
  * [ ] Wasm
  * [ ] Node
* [ ] Debuggers / visualization
* [ ] Documentation / book
  * [x] First pass at high-level explanation