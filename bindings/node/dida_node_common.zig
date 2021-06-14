pub const std = @import("std");
pub const dida = @import("../../lib/dida.zig");

pub const types_with_js_constructors = .{
    dida.core.GraphBuilder,
    dida.core.Graph,
    dida.core.Shard,

    dida.core.Change,
    dida.core.ChangeBatch,
    dida.core.Subgraph,
    dida.core.Node,
    dida.core.NodeSpec,
    dida.core.NodeSpec.MapSpec,
    dida.core.NodeSpec.JoinSpec,
    dida.core.NodeSpec.TimestampPushSpec,
    dida.core.NodeSpec.TimestampPopSpec,
    dida.core.NodeSpec.TimestampIncrementSpec,
    dida.core.NodeSpec.IndexSpec,
    dida.core.NodeSpec.UnionSpec,
    dida.core.NodeSpec.DistinctSpec,
    dida.core.NodeSpec.OutputSpec,
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
        dida.core.GraphBuilder,
        *dida.core.GraphBuilder,
        dida.core.Graph,
        *dida.core.Graph,
        *const dida.core.Graph,
        dida.core.Shard,
        *dida.core.Shard,
        *const dida.core.Shard,
        => .External,

        void,
        bool,
        usize,
        isize,
        []const usize,
        dida.core.Timestamp,
        dida.core.Frontier,
        f64,
        []const u8,
        dida.core.Value,
        std.meta.TagType(dida.core.Value),
        []const dida.core.Value,
        dida.core.Row,
        dida.core.Change,
        []dida.core.Change,
        dida.core.ChangeBatch,
        ?dida.core.ChangeBatch,
        dida.core.Subgraph,
        []const dida.core.Subgraph,
        dida.core.Node,
        ?dida.core.Node,
        [2]dida.core.Node,
        dida.core.NodeSpec,
        []const dida.core.NodeSpec,
        std.meta.TagType(dida.core.NodeSpec),
        dida.core.NodeSpec.MapSpec,
        *dida.core.NodeSpec.MapSpec.Mapper,
        dida.core.NodeSpec.JoinSpec,
        dida.core.NodeSpec.TimestampPushSpec,
        dida.core.NodeSpec.TimestampPopSpec,
        dida.core.NodeSpec.TimestampIncrementSpec,
        dida.core.NodeSpec.IndexSpec,
        dida.core.NodeSpec.UnionSpec,
        dida.core.NodeSpec.DistinctSpec,
        dida.core.NodeSpec.OutputSpec,
        => .Value,

        else => compileError("No SerdeStrategy for {}", .{T}),
    };
}

var gpa = std.heap.GeneralPurposeAllocator(.{
    .safety = true,
    .never_unmap = true,
}){};
pub const allocator = &gpa.allocator;
