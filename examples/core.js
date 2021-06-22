const util = require('util');
const dida = require('../bindings/node/dida.js');

var graph_builder = new dida.GraphBuilder();
const subgraph_0 = new dida.Subgraph(0);
const subgraph_1 = graph_builder.addSubgraph(subgraph_0);
const edges = graph_builder.addNode(subgraph_0, new dida.NodeSpec.Input());
const edges_1 = graph_builder.addNode(subgraph_1, new dida.NodeSpec.TimestampPush(edges));
const reach_future = graph_builder.addNode(subgraph_1, new dida.NodeSpec.TimestampIncrement(null));
const reach_index = graph_builder.addNode(subgraph_1, new dida.NodeSpec.Index(reach_future));
const distinct_reach_index = graph_builder.addNode(subgraph_1, new dida.NodeSpec.Distinct(reach_index));
const swapped_edges = graph_builder.addNode(subgraph_1, new dida.NodeSpec.Map(edges_1, input => [input[1], input[0]]));
const swapped_edges_index = graph_builder.addNode(subgraph_1, new dida.NodeSpec.Index(swapped_edges));
const joined = graph_builder.addNode(subgraph_1, new dida.NodeSpec.Join([distinct_reach_index, swapped_edges_index], 1));
const without_middle = graph_builder.addNode(subgraph_1, new dida.NodeSpec.Map(joined, input => [input[2], input[1]]));
const reach = graph_builder.addNode(subgraph_1, new dida.NodeSpec.Union([edges_1, without_middle]));
graph_builder.connectLoop(reach, reach_future);
const reach_pop = graph_builder.addNode(subgraph_0, new dida.NodeSpec.TimestampPop(distinct_reach_index));
const reach_out = graph_builder.addNode(subgraph_0, new dida.NodeSpec.Output(reach_pop));

const reach_summary = graph_builder.addNode(subgraph_1, new dida.NodeSpec.Reduce(
    distinct_reach_index,
    1,
    "",
    function (reduced_value, row, count) {
        for (var i = 0; i < count; i++) {
            reduced_value += row[1];
        }
        return reduced_value;
    }
));
const reach_summary_out = graph_builder.addNode(subgraph_1, new dida.NodeSpec.Output(reach_summary));

const graph = graph_builder.finishAndReset();

var shard = new dida.Shard(graph);

shard.pushInput(edges, new dida.Change(["a", "b"], [0], 1));
shard.pushInput(edges, new dida.Change(["b", "c"], [0], 1));
shard.pushInput(edges, new dida.Change(["b", "d"], [0], 1));
shard.pushInput(edges, new dida.Change(["c", "a"], [0], 1));
shard.pushInput(edges, new dida.Change(["b", "c"], [1], -1));
shard.flushInput(edges);

shard.advanceInput(edges, [1]);
while (shard.hasWork()) {
    shard.doWork();
    while (true) {
        const change_batch = shard.popOutput(reach_out);
        if (change_batch == undefined) break;
        console.log(util.inspect(change_batch, false, null, true));
    }
    while (true) {
        const change_batch = shard.popOutput(reach_summary_out);
        if (change_batch == undefined) break;
        console.log(util.inspect(change_batch, false, null, true));
    }
}

console.log("Advancing!");

shard.advanceInput(edges, [2]);
while (shard.hasWork()) {
    shard.doWork();
    while (true) {
        const change_batch = shard.popOutput(reach_out);
        if (change_batch == undefined) break;
        console.log(util.inspect(change_batch, false, null, true));
    }
    while (true) {
        const change_batch = shard.popOutput(reach_summary_out);
        if (change_batch == undefined) break;
        console.log(util.inspect(change_batch, false, null, true));
    }
}