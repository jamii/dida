pub const std = @import("std");
pub const dida = @import("../../core/dida.zig");

pub const types_with_js_constructors = .{
    dida.GraphBuilder,
    dida.Graph,
    dida.Shard,

    dida.Change,
    dida.ChangeBatch,
    dida.Subgraph,
    dida.Node,
    dida.NodeSpec,
    dida.NodeSpec.MapSpec,
    dida.NodeSpec.JoinSpec,
    dida.NodeSpec.TimestampPushSpec,
    dida.NodeSpec.TimestampPopSpec,
    dida.NodeSpec.TimestampIncrementSpec,
    dida.NodeSpec.IndexSpec,
    dida.NodeSpec.UnionSpec,
    dida.NodeSpec.DistinctSpec,
    dida.NodeSpec.OutputSpec,
};

pub fn hasJsConstructor(comptime T: type) bool {
    for (types_with_js_constructors) |T2| {
        if (T == T2) return true;
    }
    return false;
}

pub const SerdeStrategy = enum {
    External,
    Value,
};
pub fn serdeStrategy(comptime T: type) SerdeStrategy {
    return switch (T) {
        dida.GraphBuilder,
        *dida.GraphBuilder,
        dida.Graph,
        *dida.Graph,
        *const dida.Graph,
        dida.Shard,
        *dida.Shard,
        *const dida.Shard,
        => .External,

        void,
        bool,
        usize,
        isize,
        []const usize,
        dida.Timestamp,
        dida.Frontier,
        f64,
        []const u8,
        dida.Value,
        std.meta.TagType(dida.Value),
        []const dida.Value,
        dida.Row,
        dida.Change,
        []dida.Change,
        dida.ChangeBatch,
        ?dida.ChangeBatch,
        dida.Subgraph,
        []const dida.Subgraph,
        dida.Node,
        ?dida.Node,
        [2]dida.Node,
        dida.NodeSpec,
        []const dida.NodeSpec,
        std.meta.TagType(dida.NodeSpec),
        dida.NodeSpec.MapSpec,
        *dida.NodeSpec.MapSpec.Mapper,
        dida.NodeSpec.JoinSpec,
        dida.NodeSpec.TimestampPushSpec,
        dida.NodeSpec.TimestampPopSpec,
        dida.NodeSpec.TimestampIncrementSpec,
        dida.NodeSpec.IndexSpec,
        dida.NodeSpec.UnionSpec,
        dida.NodeSpec.DistinctSpec,
        dida.NodeSpec.OutputSpec,
        => .Value,

        else => @compileError("No SerdeStrategy for " ++ @typeName(T)),
    };
}

var gpa = std.heap.GeneralPurposeAllocator(.{
    .safety = true,
    .never_unmap = true,
}){};
pub const allocator = &gpa.allocator;
