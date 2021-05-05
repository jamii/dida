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
    const bar_inc = graph_builder.add_node(.{
        .Map = .{
            .input = bar,
            .function = (struct {
                fn inc(input: dida.Row) error{OutOfMemory}!dida.Row {
                    var output = try std.mem.dupe(allocator, dida.Value, input);
                    output[1].Number += 1;
                    return output;
                }
            }).inc,
        },
    });
    const key = (struct {
        fn key(input: dida.Row) dida.Row {
            return input[0..1];
        }
    }).key;
    const foobar = try graph_builder.add_node(.{
        .Join = .{
            .left_input = foo,
            .right_input = bar,
            .left_key_function = key,
            .right_key_function = key,
        },
    });

    const graph = graph_builder.finish_and_clear();

    std.debug.print("Hello world!", .{});
}
