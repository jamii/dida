const dida = require('./dida.node');

function GraphBuilder() {
    this.external = dida.GraphBuilder_init().external;
}
GraphBuilder.prototype.addSubgraph = function addSubgraph(arg0) {
    return dida.GraphBuilder_addSubgraph(this, arg0);
}
GraphBuilder.prototype.addNode = function addNode(arg0, arg1) {
    return dida.GraphBuilder_addNode(this, arg0, arg1);
}
GraphBuilder.prototype.connectLoop = function connectLoop(arg0, arg1) {
    return dida.GraphBuilder_connectLoop(this, arg0, arg1);
}
GraphBuilder.prototype.finishAndClear = function finishAndClear() {
    return dida.GraphBuilder_finishAndClear(this, );
}

function Graph(arg0, arg1, arg2) {
    this.external = dida.Graph_init(arg0, arg1, arg2).external;
}
Graph.prototype.validate = function validate() {
    return dida.Graph_validate(this, );
}

function Shard(arg0) {
    this.external = dida.Shard_init(arg0).external;
}
Shard.prototype.pushInput = function pushInput(arg0, arg1) {
    return dida.Shard_pushInput(this, arg0, arg1);
}
Shard.prototype.flushInput = function flushInput(arg0) {
    return dida.Shard_flushInput(this, arg0);
}
Shard.prototype.advanceInput = function advanceInput(arg0, arg1) {
    return dida.Shard_advanceInput(this, arg0, arg1);
}
Shard.prototype.hasWork = function hasWork() {
    return dida.Shard_hasWork(this, );
}
Shard.prototype.doWork = function doWork() {
    return dida.Shard_doWork(this, );
}
Shard.prototype.popOutput = function popOutput(arg0) {
    return dida.Shard_popOutput(this, arg0);
}

function Change(row, timestamp, diff, ) {
    this.row = row;
    this.timestamp = timestamp;
    this.diff = diff;
}

function ChangeBatch(lower_bound, changes, ) {
    this.lower_bound = lower_bound;
    this.changes = changes;
}

function Subgraph(id, ) {
    this.id = id;
}

function Node(id, ) {
    this.id = id;
}

const NodeSpec = {
    Input: function () {
        this.tag = "Input";
        this.payload = undefined;
    },
    Map: function () {
        this.tag = "Map";
        this.payload = new MapSpec(arguments[0], arguments[1]);
    },
    Index: function () {
        this.tag = "Index";
        this.payload = new IndexSpec(arguments[0]);
    },
    Join: function () {
        this.tag = "Join";
        this.payload = new JoinSpec(arguments[0], arguments[1]);
    },
    Output: function () {
        this.tag = "Output";
        this.payload = new OutputSpec(arguments[0]);
    },
    TimestampPush: function () {
        this.tag = "TimestampPush";
        this.payload = new TimestampPushSpec(arguments[0]);
    },
    TimestampIncrement: function () {
        this.tag = "TimestampIncrement";
        this.payload = new TimestampIncrementSpec(arguments[0]);
    },
    TimestampPop: function () {
        this.tag = "TimestampPop";
        this.payload = new TimestampPopSpec(arguments[0]);
    },
    Union: function () {
        this.tag = "Union";
        this.payload = new UnionSpec(arguments[0]);
    },
    Distinct: function () {
        this.tag = "Distinct";
        this.payload = new DistinctSpec(arguments[0]);
    },
};

function MapSpec(input, mapper, ) {
    this.input = input;
    this.mapper = mapper;
}

function JoinSpec(inputs, key_columns, ) {
    this.inputs = inputs;
    this.key_columns = key_columns;
}

function TimestampPushSpec(input, ) {
    this.input = input;
}

function TimestampPopSpec(input, ) {
    this.input = input;
}

function TimestampIncrementSpec(input, ) {
    this.input = input;
}

function IndexSpec(input, ) {
    this.input = input;
}

function UnionSpec(inputs, ) {
    this.inputs = inputs;
}

function DistinctSpec(input, ) {
    this.input = input;
}

function OutputSpec(input, ) {
    this.input = input;
}

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

shard.pushInput(edges, new Change(["a", "b"], [0], 1));
shard.pushInput(edges, new Change(["b", "c"], [0], 1));
shard.pushInput(edges, new Change(["b", "d"], [0], 1));
shard.pushInput(edges, new Change(["c", "a"], [0], 1));
shard.pushInput(edges, new Change(["b", "c"], [1], -1));
shard.flushInput(edges);

shard.advanceInput(edges, [1]);
while (shard.hasWork()) {
    shard.doWork();
    while (true) {
        const change_batch = shard.popOutput(out);
        if (change_batch == undefined) break;
        console.log(util.inspect(change_batch, false, null, true));
    }
}

console.log("Advancing!");

shard.advanceInput(edges, [2]);
while (shard.hasWork()) {
    shard.doWork();
    while (true) {
        const change_batch = shard.popOutput(out);
        if (change_batch == undefined) break;
        console.log(util.inspect(change_batch, false, null, true));
    }
}