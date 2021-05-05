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
    const foo = try graph_builder.add_node(.Input);
    const bar = try graph_builder.add_node(.Input);
    const bar_inc = try graph_builder.add_node(.{
        .Map = .{
            .input = .{ .node = bar, .output_port = 0 },
            .function = (struct {
                fn inc(input: dida.Row) error{OutOfMemory}!dida.Row {
                    var output_values = try std.mem.dupe(allocator, dida.Value, input.values);
                    output_values[1].Number += 1;
                    return dida.Row{ .values = output_values };
                }
            }).inc,
        },
    });
    const foo_index = try graph_builder.add_node(.{ .Index = .{ .input = .{ .node = foo, .output_port = 0 } } });
    const bar_inc_index = try graph_builder.add_node(.{ .Index = .{ .input = .{ .node = bar_inc, .output_port = 0 } } });
    const foobar = try graph_builder.add_node(.{
        .Join = .{
            .inputs = .{
                .{ .node = foo_index, .output_port = 0 },
                .{ .node = bar_inc_index, .output_port = 0 },
            },
            .key_columns = 1,
        },
    });
    const out = try graph_builder.add_node(.{ .Output = .{ .input = .{ .node = foobar, .output_port = 0 } } });

    const graph = graph_builder.finish_and_clear();

    var worker = try dida.Worker.init(allocator, graph);
    const timestamp = dida.Timestamp{ .coords = &[_]u64{1} };

    const foo1 = dida.Row{ .values = &[_]dida.Value{ .{ .String = "alice" }, .{ .Number = 1 } } };
    const foo2 = dida.Row{ .values = &[_]dida.Value{ .{ .String = "bob" }, .{ .Number = 2 } } };
    try worker.push_input(foo, .{ .row = foo1, .diff = 1, .timestamp = timestamp });
    try worker.push_input(foo, .{ .row = foo2, .diff = 1, .timestamp = timestamp });

    const bar1 = dida.Row{ .values = &[_]dida.Value{ .{ .String = "alice" }, .{ .Number = 42 } } };
    const bar2 = dida.Row{ .values = &[_]dida.Value{ .{ .String = "eve" }, .{ .Number = 71 } } };
    try worker.push_input(bar, .{ .row = bar1, .diff = 1, .timestamp = timestamp });
    try worker.push_input(bar, .{ .row = bar2, .diff = 1, .timestamp = timestamp });

    while (worker.has_work()) {
        try worker.do_work();
    }

    while (worker.pop_output(out)) |change| {
        dida.common.dump(change);
    }
}
