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

    const edges = try graph_builder.addNode(.Input);
    const edges_1 = try graph_builder.addNode(.{ .TimestampPush = .{ .input = .{ .node = edges, .output_port = 0 } } });
    const reach_in = try graph_builder.addNode(.{ .Union = .{ .input1 = .{ .node = edges_1, .output_port = 0 }, .input2 = null } });
    const reach_index = try graph_builder.addNode(.{ .Index = .{ .input = .{ .node = reach_in, .output_port = 0 } } });
    const swapped_edges = try graph_builder.addNode(.{
        .Map = .{
            .input = .{ .node = edges_1, .output_port = 0 },
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
    const swapped_edges_index = try graph_builder.addNode(.{ .Index = .{ .input = .{ .node = swapped_edges, .output_port = 0 } } });
    const joined = try graph_builder.addNode(.{
        .Join = .{
            .inputs = .{
                .{ .node = reach_index, .output_port = 0 },
                .{ .node = swapped_edges_index, .output_port = 0 },
            },
            .key_columns = 1,
        },
    });
    const without_middle = try graph_builder.addNode(.{
        .Map = .{
            .input = .{ .node = joined, .output_port = 0 },
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
    const back = try graph_builder.addNode(.{ .TimestampIncrement = .{ .input = .{ .node = without_middle, .output_port = 0 } } });
    graph_builder.node_specs.items[reach_in.id].Union.input2 = .{ .node = back, .output_port = 0 };
    const reach_out = try graph_builder.addNode(.{ .TimestampPop = .{ .input = .{ .node = without_middle, .output_port = 0 } } });
    const out = try graph_builder.addNode(.{ .Output = .{ .input = .{ .node = reach_out, .output_port = 0 } } });

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
        try shard.doWork();

        while (shard.popOutput(out)) |change| {
            dida.common.dump(change);
        }
    }
}
