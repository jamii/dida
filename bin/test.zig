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

    const edges = try graph_builder.add_node(.Input);
    const edges_1 = try graph_builder.add_node(.{ .TimestampPush = .{ .input = .{ .node = edges, .output_port = 0 } } });
    const reach_in = try graph_builder.add_node(.{ .Union = .{ .input1 = .{ .node = edges_1, .output_port = 0 }, .input2 = null } });
    const reach_index = try graph_builder.add_node(.{ .Index = .{ .input = .{ .node = reach_in, .output_port = 0 } } });
    const swapped_edges = try graph_builder.add_node(.{
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
    const swapped_edges_index = try graph_builder.add_node(.{ .Index = .{ .input = .{ .node = swapped_edges, .output_port = 0 } } });
    const joined = try graph_builder.add_node(.{
        .Join = .{
            .inputs = .{
                .{ .node = reach_index, .output_port = 0 },
                .{ .node = swapped_edges_index, .output_port = 0 },
            },
            .key_columns = 1,
        },
    });
    const without_middle = try graph_builder.add_node(.{
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
    const back = try graph_builder.add_node(.{ .TimestampIncrement = .{ .input = .{ .node = without_middle, .output_port = 0 } } });
    graph_builder.node_specs.items[reach_in.id].Union.input2 = .{ .node = back, .output_port = 0 };
    const reach_out = try graph_builder.add_node(.{ .TimestampPop = .{ .input = .{ .node = without_middle, .output_port = 0 } } });
    const out = try graph_builder.add_node(.{ .Output = .{ .input = .{ .node = reach_out, .output_port = 0 } } });

    const graph = graph_builder.finish_and_clear();

    var worker = try dida.Worker.init(allocator, graph);
    const timestamp1 = dida.Timestamp{ .coords = &[_]u64{1} };

    const ab = dida.Row{ .values = &[_]dida.Value{ .{ .String = "a" }, .{ .String = "b" } } };
    const bc = dida.Row{ .values = &[_]dida.Value{ .{ .String = "b" }, .{ .String = "c" } } };
    const cd = dida.Row{ .values = &[_]dida.Value{ .{ .String = "c" }, .{ .String = "d" } } };
    const ca = dida.Row{ .values = &[_]dida.Value{ .{ .String = "c" }, .{ .String = "a" } } };
    try worker.push_input(edges, .{ .row = ab, .diff = 1, .timestamp = timestamp1 });
    try worker.push_input(edges, .{ .row = bc, .diff = 1, .timestamp = timestamp1 });
    try worker.push_input(edges, .{ .row = cd, .diff = 1, .timestamp = timestamp1 });
    try worker.push_input(edges, .{ .row = ca, .diff = 1, .timestamp = timestamp1 });

    while (worker.has_work()) {
        try worker.do_work();

        while (worker.pop_output(out)) |change| {
            dida.common.dump(change);
        }
    }
}
