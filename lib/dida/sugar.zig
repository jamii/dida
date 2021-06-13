// TODO this whole file is just speculative atm

usingnamespace @import("./common.zig");

fn assert_ok(result: anytype) @typeInfo(@TypeOf(result)).ErrorUnion.payload {
    return result catch |err| panic("{}", .{err});
}

// TODO this needs a better name
pub const Sugar = struct {
    allocator: *Allocator,
    state: union(enum) {
        Building: dida.core.GraphBuilder,
        Running: dida.core.Shard,
    },

    pub fn init(allocator: *Allocator) Sugar {
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
        assert(
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
        assert(
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

pub fn Node(comptime tag_: std.meta.TagType(dida.core.NodeSpec)) type {
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

        pub usingnamespace if (comptime dida.core.NodeSpec.tagHasIndex(tag)) struct {
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
                if (!comptime (dida.core.NodeSpec.tagHasIndex(@TypeOf(other).tag)))
                    @compileError("Can only call join on nodes which have indexes (Index, Distinct), not " ++ @TypeOf(other).tag);

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

        pub fn project(self: Self, columns: anytype) Node(.Map) {
            return self.projectInner(coerceAnonToSlice(self.sugar.allocator, usize, columns));
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

        pub usingnamespace if (comptime tag == .Input) struct {
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

fn coerceAnonToSlice(allocator: *Allocator, comptime Elem: type, anon: anytype) []Elem {
    const slice = assert_ok(allocator.alloc(Elem, anon.len));
    comptime var i: usize = 0;
    inline while (i < anon.len) : (i += 1) {
        slice[i] = coerceAnonTo(allocator, Elem, anon[i]);
    }
    return slice;
}

fn coerceAnonTo(allocator: *Allocator, comptime T: type, anon: anytype) T {
    switch (T) {
        dida.core.Timestamp => {
            return .{ .coords = coerceAnonToSlice(allocator, usize, anon) };
        },
        dida.core.Change => {
            return .{
                .row = coerceAnonTo(allocator, dida.core.Row, anon[0]),
                .timestamp = coerceAnonTo(allocator, dida.core.Timestamp, anon[1]),
                .diff = anon[2],
            };
        },
        dida.core.Row => {
            return .{ .values = coerceAnonToSlice(allocator, dida.core.Value, anon) };
        },
        dida.core.Value => {
            switch (@typeInfo(@TypeOf(anon))) {
                .Int => return .{ .Number = anon },
                .Pointer => return .{ .String = anon },
                else => @compileError("Don't know how to coerce " ++ @typeName(anon) ++ " to value"),
            }
        },
        usize => return anon,
        else => @compileError("Don't know how to coerce anon to " ++ @typeName(T)),
    }
}

const ProjectMapper = struct {
    allocator: *Allocator,
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
