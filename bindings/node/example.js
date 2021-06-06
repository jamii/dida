var graph_builder = new GraphBuilder();
const subgraph_0 = new Subgraph(0);
const subgraph_1 = graph_builder.addSubgraph(subgraph_0);
const edges = graph_builder.addNode(subgraph_0, new NodeSpec.Input());
const edges_1 = graph_builder.addNode(subgraph_1, new NodeSpec.TimestampPush(edges));
const reach_future = graph_builder.addNode(subgraph_1, new NodeSpec.TimestampIncrement(null));
const reach_index = graph_builder.addNode(subgraph_1, new NodeSpec.Index(reach_future));
const distinct_reach_index = graph_builder.addNode(subgraph_1, new NodeSpec.Distinct(reach_index));
const swapped_edges = graph_builder.addNode(subgraph_1, new NodeSpec.Map(edges_1, input => [input[1], input[0]]));
//const swapped_edges_index = graph_builder.addNode(subgraph_1, .{ .Index = .{ .input = swapped_edges } });
//const joined = graph_builder.addNode(subgraph_1, .{
    //.Join = .{
        //.inputs = .{
            //distinct_reach_index,
            //swapped_edges_index,
        //},
        //.key_columns = 1,
    //},
//});
//const without_middle = graph_builder.addNode(subgraph_1, .{
    //.Map = .{
        //.input = joined,
        //.function = (struct {
            //fn drop_middle(input: dida.Row) error{OutOfMemory}!dida.Row {
                //var output_values = allocator.alloc(dida.Value, 2);
                //output_values[0] = input.values[3];
                //output_values[1] = input.values[1];
                //return dida.Row{ .values = output_values };
            //}
        //}).drop_middle,
    //},
//});
//const reach = graph_builder.addNode(subgraph_1, .{ .Union = .{ .inputs = .{ edges_1, without_middle } } });
//graph_builder.node_specs.items[reach_future.id].TimestampIncrement.input = reach;
//const reach_out = graph_builder.addNode(subgraph_0, .{ .TimestampPop = .{ .input = distinct_reach_index } });
//const out = graph_builder.addNode(subgraph_0, .{ .Output = .{ .input = reach_out } });
//
//const graph = graph_builder.finishAndClear();
//
//var shard = dida.Shard.init(allocator, graph);
//const timestamp0 = dida.Timestamp{ .coords = &[_]u64{0} };
//const timestamp1 = dida.Timestamp{ .coords = &[_]u64{1} };
//const timestamp2 = dida.Timestamp{ .coords = &[_]u64{2} };
//
//const ab = dida.Row{ .values = &[_]dida.Value{ .{ .String = "a" }, .{ .String = "b" } } };
//const bc = dida.Row{ .values = &[_]dida.Value{ .{ .String = "b" }, .{ .String = "c" } } };
//const cd = dida.Row{ .values = &[_]dida.Value{ .{ .String = "b" }, .{ .String = "d" } } };
//const ca = dida.Row{ .values = &[_]dida.Value{ .{ .String = "c" }, .{ .String = "a" } } };
//shard.pushInput(edges, .{ .row = ab, .diff = 1, .timestamp = timestamp0 });
//shard.pushInput(edges, .{ .row = bc, .diff = 1, .timestamp = timestamp0 });
//shard.pushInput(edges, .{ .row = cd, .diff = 1, .timestamp = timestamp0 });
//shard.pushInput(edges, .{ .row = ca, .diff = 1, .timestamp = timestamp0 });
//shard.pushInput(edges, .{ .row = bc, .diff = -1, .timestamp = timestamp1 });
//shard.flushInput(edges);
//
//shard.advanceInput(edges, timestamp1);
//while (shard.hasWork()) {
    //// dida.common.dump(shard);
    //shard.doWork();
    //
    //while (shard.popOutput(out)) |change_batch| {
        //dida.common.dump(change_batch);
    //}
//}
//
//std.debug.print("Advancing!\n", .{});
//
//shard.advanceInput(edges, timestamp2);
//while (shard.hasWork()) {
    //// dida.common.dump(shard);
    //shard.doWork();
    //
    //while (shard.popOutput(out)) |change_batch| {
        //dida.common.dump(change_batch);
    //}
//}
//
//// dida.common.dump(shard);