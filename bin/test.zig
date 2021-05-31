const std = @import("std");
const dida = @import("../lib/dida.zig");

var gpa = std.heap.GeneralPurposeAllocator(.{
    .safety = true,
    .never_unmap = true,
}){};
var arena = std.heap.ArenaAllocator.init(&gpa.allocator);
const allocator = &arena.allocator;

pub fn main() !void {
    defer {
        arena.deinit();
        _ = gpa.detectLeaks();
    }

    var graph_builder = dida.GraphBuilder.init(allocator);
    const subgraph_0 = dida.Subgraph{ .id = 0 };
    const subgraph_1 = try graph_builder.addSubgraph(subgraph_0);

    const edges = try graph_builder.addNode(subgraph_0, .Input);
    const edges_1 = try graph_builder.addNode(subgraph_1, .{ .TimestampPush = .{ .input = edges } });
    const reach_future = try graph_builder.addNode(subgraph_1, .{ .TimestampIncrement = .{ .input = null } });
    const reach_index = try graph_builder.addNode(subgraph_1, .{ .Index = .{ .input = reach_future } });
    const distinct_reach_index = try graph_builder.addNode(subgraph_1, .{ .Distinct = .{ .input = reach_index } });
    const swapped_edges = try graph_builder.addNode(subgraph_1, .{
        .Map = .{
            .input = edges_1,
            .function = (struct {
                fn swap(input: dida.Row) error{OutOfMemory}!dida.Row {
                    var output_values = try allocator.alloc(dida.Value, 2);
                    output_values[0] = input.values[1];
                    output_values[1] = input.values[0];
                    return dida.Row{ .values = output_values };
                }
            }).swap,
        },
    });
    const swapped_edges_index = try graph_builder.addNode(subgraph_1, .{ .Index = .{ .input = swapped_edges } });
    const joined = try graph_builder.addNode(subgraph_1, .{
        .Join = .{
            .inputs = .{
                distinct_reach_index,
                swapped_edges_index,
            },
            .key_columns = 1,
        },
    });
    const without_middle = try graph_builder.addNode(subgraph_1, .{
        .Map = .{
            .input = joined,
            .function = (struct {
                fn drop_middle(input: dida.Row) error{OutOfMemory}!dida.Row {
                    var output_values = try allocator.alloc(dida.Value, 2);
                    output_values[0] = input.values[3];
                    output_values[1] = input.values[1];
                    return dida.Row{ .values = output_values };
                }
            }).drop_middle,
        },
    });
    const reach = try graph_builder.addNode(subgraph_1, .{ .Union = .{ .inputs = .{ edges_1, without_middle } } });
    graph_builder.node_specs.items[reach_future.id].TimestampIncrement.input = reach;
    const reach_out = try graph_builder.addNode(subgraph_0, .{ .TimestampPop = .{ .input = distinct_reach_index } });
    const out = try graph_builder.addNode(subgraph_0, .{ .Output = .{ .input = reach_out } });

    const graph = try graph_builder.finishAndClear();

    var shard = try dida.Shard.init(allocator, graph);
    const timestamp1 = dida.Timestamp{ .coords = &[_]u64{1} };
    const timestamp2 = dida.Timestamp{ .coords = &[_]u64{2} };

    const ab = dida.Row{ .values = &[_]dida.Value{ .{ .String = "a" }, .{ .String = "b" } } };
    const bc = dida.Row{ .values = &[_]dida.Value{ .{ .String = "b" }, .{ .String = "c" } } };
    const cd = dida.Row{ .values = &[_]dida.Value{ .{ .String = "c" }, .{ .String = "d" } } };
    const ca = dida.Row{ .values = &[_]dida.Value{ .{ .String = "c" }, .{ .String = "a" } } };
    try shard.pushInput(edges, .{ .row = ab, .diff = 1, .timestamp = timestamp1 });
    try shard.pushInput(edges, .{ .row = bc, .diff = 1, .timestamp = timestamp1 });
    try shard.pushInput(edges, .{ .row = cd, .diff = 1, .timestamp = timestamp1 });
    try shard.pushInput(edges, .{ .row = ca, .diff = 1, .timestamp = timestamp1 });
    try shard.flushInput(edges);
    try shard.advanceInput(edges, timestamp2);

    while (shard.hasWork()) {
        // dida.common.dump(shard);
        try shard.doWork();

        while (shard.popOutput(out)) |change_batch| {
            dida.common.dump(change_batch);
        }
    }

    // dida.common.dump(shard);
}
