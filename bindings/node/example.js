const util = require('util');

var graph_builder = new GraphBuilder();
const subgraph_0 = new Subgraph(0);
const subgraph_1 = graph_builder.addSubgraph(subgraph_0);
const edges = graph_builder.addNode(subgraph_0, new NodeSpec.Input());
const edges_1 = graph_builder.addNode(subgraph_1, new NodeSpec.TimestampPush(edges));
const reach_future = graph_builder.addNode(subgraph_1, new NodeSpec.TimestampIncrement(null));
const reach_index = graph_builder.addNode(subgraph_1, new NodeSpec.Index(reach_future));
const distinct_reach_index = graph_builder.addNode(subgraph_1, new NodeSpec.Distinct(reach_index));
const swapped_edges = graph_builder.addNode(subgraph_1, new NodeSpec.Map(edges_1, input => [input[1], input[0]]));
const swapped_edges_index = graph_builder.addNode(subgraph_1, new NodeSpec.Index(swapped_edges));
const joined = graph_builder.addNode(subgraph_1, new NodeSpec.Join([distinct_reach_index, swapped_edges_index], 1));
const without_middle = graph_builder.addNode(subgraph_1, new NodeSpec.Map(joined, input => [input[3], input[1]]));
const reach = graph_builder.addNode(subgraph_1, new NodeSpec.Union([edges_1, without_middle]));
graph_builder.connectLoop(reach, reach_future);
const reach_out = graph_builder.addNode(subgraph_0, new NodeSpec.TimestampPop(distinct_reach_index));
const out = graph_builder.addNode(subgraph_0, new NodeSpec.Output(reach_out));

const graph = graph_builder.finishAndClear();

var shard = new Shard(graph);
const timestamp0 = new Timestamp([0]);
const timestamp1 = new Timestamp([1]);
const timestamp2 = new Timestamp([2]);

shard.pushInput(edges, new Change(["a", "b"], timestamp0, 1));
shard.pushInput(edges, new Change(["b", "c"], timestamp0, 1));
shard.pushInput(edges, new Change(["b", "d"], timestamp0, 1));
shard.pushInput(edges, new Change(["c", "a"], timestamp0, 1));
shard.pushInput(edges, new Change(["b", "c"], timestamp1, -1));
shard.flushInput(edges);

shard.advanceInput(edges, timestamp1);
while (shard.hasWork()) {
    shard.doWork();
    while (true) {
        const change_batch = shard.popOutput(out);
        if (change_batch == undefined) break;
        console.log(util.inspect(change_batch, false, null, true));
    }
}

console.log("Advancing!");

shard.advanceInput(edges, timestamp2);
while (shard.hasWork()) {
    shard.doWork();
    while (true) {
        const change_batch = shard.popOutput(out);
        if (change_batch == undefined) break;
        console.log(util.inspect(change_batch, false, null, true));
    }
}