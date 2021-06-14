const std = @import("std");
const dida = @import("../../lib/dida.zig");

var gpa = std.heap.GeneralPurposeAllocator(.{
    .safety = true,
    .never_unmap = true,
}){};
var arena = std.heap.ArenaAllocator.init(&gpa.allocator);
const allocator = &arena.allocator;

pub fn main() !void {
    var sugar = dida.sugar.Sugar.init(allocator);

    const edges = sugar.input();
    const loop = sugar.loop();
    _ = loop.importNode(loop.exportNode(loop.importNode(edges)));

    sugar.build();
}
