const std = @import("std");
const dida = @import("../lib/dida.zig");

var gpa = std.heap.GeneralPurposeAllocator(.{
    .safety = true,
    .never_unmap = true,
}){};
const allocator = &gpa.allocator;

pub fn main() !void {
    defer {
        _ = gpa.detectLeaks();
    }

    var graph_builder = dida.GraphBuilder.init(allocator);
    defer graph_builder.deinit();
    const foo = try graph_builder.add_node(.Input);
    const bar = try graph_builder.add_node(.Input);
    const bar_inc = graph_builder.add_node(.{
        .Map = .{
            .input = bar,
            .function = (struct {
                fn inc(input: dida.Datum) error{OutOfMemory}!dida.Datum {
                    var output = try std.mem.dupe(allocator, u8, input);
                    output[1] += 1;
                    return output;
                }
            }).inc,
        },
    });
    const key = (struct {
        fn key(input: dida.Datum) dida.Datum {
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
    defer graph.deinit();

    std.debug.print("Hello world!", .{});
}
