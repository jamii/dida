const util = require('util');

var graph_builder = new GraphBuilder();
const subgraph_0 = new Subgraph(0);
const subgraph_1 = graph_builder.addSubgraph(subgraph_0);
const edges = graph_builder.addNode(subgraph_0, new NodeSpec.Input());
const edges_1 = graph_builder.addNode(subgraph_1, new NodeSpec.TimestampPush(edges));
const reach_future = graph_builder.addNode(subgraph_1, new NodeSpec.TimestampIncrement(null));
const reach_index = graph_builder.addNode(subgraph_1, new NodeSpec.Index(reach_future));
const distinct_reach_index = graph_builder.addNode(subgraph_1, new NodeSpec.Distinct(reach_index));
const swapped_edges = graph_builder.addNode(subgraph_1, new NodeSpec.Map(edges_1, input => new Row([input.values[1], input.values[0]])));
const swapped_edges_index = graph_builder.addNode(subgraph_1, new NodeSpec.Index(swapped_edges));
const joined = graph_builder.addNode(subgraph_1, new NodeSpec.Join([distinct_reach_index, swapped_edges_index], 1));
const without_middle = graph_builder.addNode(subgraph_1, new NodeSpec.Map(joined, input => new Row([input.values[3], input.values[1]])));
const reach = graph_builder.addNode(subgraph_1, new NodeSpec.Union([edges_1, without_middle]));
graph_builder.connectLoop(reach, reach_future);
const reach_out = graph_builder.addNode(subgraph_0, new NodeSpec.TimestampPop(distinct_reach_index));
const out = graph_builder.addNode(subgraph_0, new NodeSpec.Output(reach_out));

// TODO graph should have a wrapper object
const graph = graph_builder.finishAndClear();

var shard = new Shard(graph);
const timestamp0 = new Timestamp([0]);
const timestamp1 = new Timestamp([1]);
const timestamp2 = new Timestamp([2]);

// TODO this seems silly
const a = new Value.String("a");
const b = new Value.String("b");
const c = new Value.String("c");
const d = new Value.String("d");

const ab = new Row([a, b]);
const bc = new Row([b, c]);
const cd = new Row([b, d]);
const ca = new Row([c, a]);
shard.pushInput(edges, new Change(ab, timestamp0, 1));
shard.pushInput(edges, new Change(bc, timestamp0, 1));
shard.pushInput(edges, new Change(cd, timestamp0, 1));
shard.pushInput(edges, new Change(ca, timestamp0, 1));
shard.pushInput(edges, new Change(bc, timestamp1, -1));
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