// TODO this is just a proof of concept, api might change a lot

const std = @import("std");
const dida = @import("../dida.zig");
const u = dida.util;

fn assert_ok(result: anytype) @typeInfo(@TypeOf(result)).ErrorUnion.payload {
    return result catch |err| u.panic("{}", .{err});
}

// TODO this needs a better name
pub const Sugar = struct {
    allocator: u.Allocator,
    state: union(enum) {
        Building: dida.core.GraphBuilder,
        Running: dida.core.Shard,
    },

    pub fn init(allocator: u.Allocator) Sugar {
        return .{
            .allocator = allocator,
            .state = .{ .Building = dida.core.GraphBuilder.init(allocator) },
        };
    }

    pub fn input(self: *Sugar) Node(.Input) {
        const inner = assert_ok(self.state.Building.addNode(.{ .id = 0 }, .Input));
        return .{
            .sugar = self,
            .inner = inner,
        };
    }

    pub fn build(self: *Sugar) void {
        const graph = assert_ok(self.allocator.create(dida.core.Graph));
        graph.* = assert_ok(self.state.Building.finishAndReset());
        self.state = .{ .Running = assert_ok(dida.core.Shard.init(self.allocator, graph)) };
    }

    pub fn doAllWork(self: *Sugar) !void {
        const shard = &self.state.Running;
        while (shard.hasWork())
            try shard.doWork();
    }

    pub fn loop(self: *Sugar) Subgraph {
        const builder = &self.state.Building;
        const new_inner = assert_ok(builder.addSubgraph(.{ .id = 0 }));
        return Subgraph{
            .sugar = self,
            .inner = new_inner,
        };
    }
};

pub const Subgraph = struct {
    sugar: *Sugar,
    inner: dida.core.Subgraph,

    pub fn loop(self: Subgraph) Subgraph {
        const builder = &self.sugar.state.Building;
        const new_inner = assert_ok(builder.addSubgraph(self.inner));
        return Subgraph{
            .sugar = self.sugar,
            .inner = new_inner,
        };
    }

    pub fn loopNode(self: Subgraph) Node(.TimestampIncrement) {
        const builder = &self.sugar.state.Building;
        const node_inner = assert_ok(builder.addNode(
            self.inner,
            .{ .TimestampIncrement = .{ .input = null } },
        ));
        return Node(.TimestampIncrement){
            .sugar = self.sugar,
            .inner = node_inner,
        };
    }

    // TODO would be nice to automatically add TimestampPush/Pop in node methods as needed instead of needing import/export
    // TODO would also be nice to cache repeated calls

    pub fn importNode(self: Subgraph, node: anytype) Node(.TimestampPush) {
        const builder = &self.sugar.state.Building;
        u.assert(
            builder.node_subgraphs.items[node.inner.id].id == builder.subgraph_parents.items[self.inner.id - 1].id,
            "Can only import from parent subgraph into child subgraph",
            .{},
        );
        const node_inner = assert_ok(builder.addNode(
            self.inner,
            .{ .TimestampPush = .{ .input = node.inner } },
        ));
        return Node(.TimestampPush){
            .sugar = self.sugar,
            .inner = node_inner,
        };
    }

    pub fn exportNode(self: Subgraph, node: anytype) Node(.TimestampPop) {
        const builder = &self.sugar.state.Building;
        u.assert(
            builder.node_subgraphs.items[node.inner.id].id == self.inner.id,
            "Can only export from child subgraph into parent subgraph",
            .{},
        );
        const node_inner = assert_ok(builder.addNode(
            builder.subgraph_parents.items[self.inner.id - 1],
            .{ .TimestampPop = .{ .input = node.inner } },
        ));
        return Node(.TimestampPop){
            .sugar = self.sugar,
            .inner = node_inner,
        };
    }
};

pub fn Node(comptime tag_: std.meta.Tag(dida.core.NodeSpec)) type {
    return struct {
        sugar: *Sugar,
        inner: dida.core.Node,
        pub const tag = tag_;

        const Self = @This();

        pub fn index(self: Self) Node(.Index) {
            const builder = &self.sugar.state.Building;
            const subgraph = builder.node_subgraphs.items[self.inner.id];
            const new_inner = assert_ok(self.sugar.state.Building.addNode(
                subgraph,
                .{ .Index = .{ .input = self.inner } },
            ));
            return .{
                .sugar = self.sugar,
                .inner = new_inner,
            };
        }

        pub usingnamespace if (tag.hasIndex()) struct {
            pub fn distinct(self: Self) Node(.Distinct) {
                const builder = &self.sugar.state.Building;
                const subgraph = builder.node_subgraphs.items[self.inner.id];
                const new_inner = assert_ok(self.sugar.state.Building.addNode(
                    subgraph,
                    .{ .Distinct = .{ .input = self.inner } },
                ));
                return .{
                    .sugar = self.sugar,
                    .inner = new_inner,
                };
            }

            pub fn join(self: Self, other: anytype, key_columns: usize) Node(.Join) {
                u.comptimeAssert(
                    comptime @TypeOf(other).tag.hasIndex(),
                    "Can only call join on nodes which have indexes (Index, Distinct), not {}",
                    .{@TypeOf(other).tag},
                );

                const builder = &self.sugar.state.Building;
                const subgraph = builder.node_subgraphs.items[self.inner.id];
                const new_inner = assert_ok(self.sugar.state.Building.addNode(
                    subgraph,
                    .{ .Join = .{
                        .inputs = .{ self.inner, other.inner },
                        .key_columns = key_columns,
                    } },
                ));
                return .{
                    .sugar = self.sugar,
                    .inner = new_inner,
                };
            }
        } else struct {};

        pub fn map(self: Self, mapper: *dida.core.NodeSpec.MapSpec.Mapper) Node(.Map) {
            const builder = &self.sugar.state.Building;
            const subgraph = builder.node_subgraphs.items[self.inner.id];
            const new_inner = assert_ok(self.sugar.state.Building.addNode(
                subgraph,
                .{ .Map = .{
                    .input = self.inner,
                    .mapper = mapper,
                } },
            ));
            return .{
                .sugar = self.sugar,
                .inner = new_inner,
            };
        }

        pub fn reduce(self: Self, key_columns: usize, init_value: anytype, reducer: *dida.core.NodeSpec.ReduceSpec.Reducer) Node(.Reduce) {
            const builder = &self.sugar.state.Building;
            const subgraph = builder.node_subgraphs.items[self.inner.id];
            const new_inner = assert_ok(self.sugar.state.Building.addNode(
                subgraph,
                .{ .Reduce = .{
                    .input = self.inner,
                    .key_columns = key_columns,
                    .init_value = coerceAnonTo(self.sugar.allocator, dida.core.Value, init_value),
                    .reducer = reducer,
                } },
            ));
            return .{
                .sugar = self.sugar,
                .inner = new_inner,
            };
        }

        pub fn project(self: Self, columns: anytype) Node(.Map) {
            return self.projectInner(coerceAnonTo(self.sugar.allocator, []usize, columns));
        }

        fn projectInner(self: Self, columns: []usize) Node(.Map) {
            const project_mapper = assert_ok(self.sugar.allocator.create(ProjectMapper));
            project_mapper.* = ProjectMapper{
                .allocator = self.sugar.allocator,
                .columns = columns,
                .mapper = .{ .map_fn = ProjectMapper.map },
            };
            return self.map(&project_mapper.mapper);
        }

        // TODO shame this is a reserved name, need to think of a different name
        pub fn union_(self: Self, other: anytype) Node(.Union) {
            const builder = &self.sugar.state.Building;
            const subgraph = builder.node_subgraphs.items[self.inner.id];
            const new_inner = assert_ok(builder.addNode(
                subgraph,
                .{ .Union = .{ .inputs = .{ self.inner, other.inner } } },
            ));
            return .{
                .sugar = self.sugar,
                .inner = new_inner,
            };
        }

        pub usingnamespace if (tag == .TimestampIncrement) struct {
            pub fn fixpoint(self: Self, future: anytype) void {
                const builder = &self.sugar.state.Building;
                builder.connectLoop(future.inner, self.inner);
            }
        } else struct {};

        pub fn output(self: Self) Node(.Output) {
            const builder = &self.sugar.state.Building;
            const subgraph = builder.node_subgraphs.items[self.inner.id];
            const new_inner = assert_ok(self.sugar.state.Building.addNode(
                subgraph,
                .{ .Output = .{ .input = self.inner } },
            ));
            return .{
                .sugar = self.sugar,
                .inner = new_inner,
            };
        }

        pub usingnamespace if (tag == .Input) struct {
            pub fn push(self: Self, change: anytype) !void {
                // TODO self.pushInner won't compile here - circular reference?
                try pushInner(self, coerceAnonTo(self.sugar.allocator, dida.core.Change, change));
            }

            fn pushInner(self: Self, change: dida.core.Change) !void {
                const shard = &self.sugar.state.Running;
                try shard.pushInput(self.inner, change);
            }

            pub fn flush(self: Self) !void {
                const shard = &self.sugar.state.Running;
                try shard.flushInput(self.inner);
            }

            pub fn advance(self: Self, timestamp: anytype) !void {
                try self.advanceInner(coerceAnonTo(self.sugar.allocator, dida.core.Timestamp, timestamp));
            }

            pub fn advanceInner(self: Self, timestamp: dida.core.Timestamp) !void {
                const shard = &self.sugar.state.Running;
                try shard.advanceInput(self.inner, timestamp);
            }
        } else struct {};

        pub usingnamespace if (tag == .Output) struct {
            pub fn pop(self: Self) ?dida.core.ChangeBatch {
                const shard = &self.sugar.state.Running;
                return shard.popOutput(self.inner);
            }
        } else struct {};
    };
}

pub fn coerceAnonTo(allocator: u.Allocator, comptime T: type, anon: anytype) T {
    const ti = @typeInfo(T);
    if (ti == .Pointer and ti.Pointer.size == .Slice) {
        const slice = assert_ok(allocator.alloc(ti.Pointer.child, anon.len));
        comptime var i: usize = 0;
        inline while (i < anon.len) : (i += 1) {
            slice[i] = coerceAnonTo(allocator, ti.Pointer.child, anon[i]);
        }
        return slice;
    } else {
        switch (T) {
            u8 => return anon,
            dida.core.Timestamp => {
                return .{ .coords = coerceAnonTo(allocator, []usize, anon) };
            },
            dida.core.Change => {
                return .{
                    .row = coerceAnonTo(allocator, dida.core.Row, anon[0]),
                    .timestamp = coerceAnonTo(allocator, dida.core.Timestamp, anon[1]),
                    .diff = anon[2],
                };
            },
            ?dida.core.ChangeBatch => {
                const changes = coerceAnonTo(allocator, []dida.core.Change, anon);
                defer allocator.free(changes);
                var builder = dida.core.ChangeBatchBuilder.init(allocator);
                assert_ok(builder.changes.appendSlice(changes));
                return assert_ok(builder.finishAndReset());
            },
            dida.core.ChangeBatch => {
                return coerceAnonTo(allocator, ?dida.core.ChangeBatch, anon).?;
            },
            dida.core.Row => {
                return .{ .values = coerceAnonTo(allocator, []dida.core.Value, anon) };
            },
            dida.core.Value => {
                switch (@typeInfo(@TypeOf(anon))) {
                    .Int, .ComptimeInt => return .{ .Number = @intCast(u64, anon) },
                    .Pointer => return .{ .String = coerceAnonTo(allocator, []const u8, anon) },
                    else => u.compileError("Don't know how to coerce {} to Value", .{@TypeOf(anon)}),
                }
            },
            dida.core.FrontierChange => {
                return .{
                    .timestamp = coerceAnonTo(allocator, dida.core.Timestamp, anon[0]),
                    .diff = anon[1],
                };
            },
            dida.core.Frontier => {
                const timestamps = coerceAnonTo(allocator, []dida.core.Timestamp, anon);
                defer {
                    for (timestamps) |*timestamp| timestamp.deinit(allocator);
                    allocator.free(timestamps);
                }
                var frontier = dida.core.Frontier.init(allocator);
                var changes_into = std.ArrayList(dida.core.FrontierChange).init(allocator);
                defer changes_into.deinit();
                for (timestamps) |timestamp| {
                    assert_ok(frontier.move(.Later, timestamp, &changes_into));
                    for (changes_into.items) |*change| change.deinit(allocator);
                    assert_ok(changes_into.resize(0));
                }
                return frontier;
            },
            usize => return anon,
            else => u.compileError("Don't know how to coerce anon to {}", .{T}),
        }
    }
}

const ProjectMapper = struct {
    allocator: u.Allocator,
    columns: []usize,
    mapper: dida.core.NodeSpec.MapSpec.Mapper,

    fn map(self: *dida.core.NodeSpec.MapSpec.Mapper, input: dida.core.Row) error{OutOfMemory}!dida.core.Row {
        const project_mapper = @fieldParentPtr(ProjectMapper, "mapper", self);
        var output_values = assert_ok(project_mapper.allocator.alloc(dida.core.Value, project_mapper.columns.len));
        for (output_values) |*output_value, i| {
            output_value.* = input.values[project_mapper.columns[i]];
        }
        return dida.core.Row{ .values = output_values };
    }
};
