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
            .state = .{ .Building = assert_ok(dida.core.GraphBuilder.init(allocator)) },
        };
    }

    pub fn input(self: *Sugar) Node(.Input) {
        const inner = assert_ok(self.state.Building.addNode(.{ .id = 0 }, .Input));
        return .{
            .sugar = self,
            .inner = inner,
        };
    }

    pub fn main(self: *Sugar) Subgraph {
        const builder = &self.state.Building;
        const inner = assert_ok(builder.addSubgraph(self.inner));
        return Subgraph{
            .sugar = self,
            .inner = inner,
        };
    }

    pub fn build(self: *Sugar) void {
        const graph = assert_ok(self.allocator.create(Graph));
        graph.* = assert_ok(self.state.Building.finishAndReset());
        self.state = .{ .Running = dida.core.Shard.init(self.allocator, &self.graph) };
    }

    pub fn doAllWork(self: *Sugar) void {
        const shard = &self.state.Running;
        while (shard.hasWork())
            try shard.doWork();
    }
};

pub const Subgraph = struct {
    sugar: *Sugar,
    inner: dida.core.Subgraph,

    pub fn loop(self: Subgraph) Subgraph {
        const builder = &self.sugar.state.Building;
        const new_inner = builder.addSubgraph(self.inner);
        return Subgraph{
            .sugar = self.sugar,
            .inner = new_inner,
        };
    }

    pub fn loopNode(self: Subgraph) Node(.TimestampIncrement) {
        const builder = &self.sugar.state.Building;
        const node_inner = assert_ok(builder.addNode(self.inner, .TimestampIncrement{ .input = null }));
        return Node(.TimestampIncrement){
            .sugar = self.sugar,
            .inner = node_inner,
        };
    }
};

pub fn Node(comptime tag: std.meta.TagType(dida.core.NodeSpec)) type {
    return struct {
        sugar: *Sugar,
        inner: dida.core.Node,
        pub const tag = tag;

        const Self = @This();

        // TODO would be nicer to generate the decls for these methods so they don't appear in autocomplete (https://github.com/ziglang/zig/issues/6709)

        // TODO automatically add TimestampPush/Pop as needed

        pub fn index(self: Self) Node(.Index) {
            const builder = &self.sugar.state.Building;
            const subgraph = builder.node_subgraphs[self.inner.id];
            const new_inner = assert_ok(self.sugar.state.Building.addNode(
                subgraph,
                .{ .Index = .{ .input = self.inner } },
            ));
            return .{
                .sugar = self.sugar,
                .inner = new_inner,
            };
        }

        pub fn distinct(self: Self) Node(.Distinct) {
            if (!comptime dida.core.NodeSpec.tagHasIndex(tag))
                @compileError("Can only call distinct on nodes which have indexes (Index, Distinct), not " ++ tag);

            const builder = &self.sugar.state.Building;
            const subgraph = builder.node_subgraphs[self.inner.id];
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
            if (!comptime (dida.core.NodeSpec.tagHasIndex(tag)))
                @compileError("Can only call join on nodes which have indexes (Index, Distinct), not " ++ tag);
            if (!comptime (dida.core.NodeSpec.tagHasIndex(other.tag)))
                @compileError("Can only call join on nodes which have indexes (Index, Distinct), not " ++ other.tag);

            const builder = &self.sugar.state.Building;
            const subgraph = builder.node_subgraphs[self.inner.id];
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

        pub fn map(self: Self, mapper: dida.core.MapSpec.Mapper) Node(.Map) {
            const builder = &self.sugar.state.Building;
            const subgraph = builder.node_subgraphs[self.inner.id];
            const new_inner = assert_ok(self.sugar.state.Building.addNode(
                subgraph,
                .{ .Map = .{
                    .inputs = .{ self.inner, other.inner },
                    .mapper = mapper,
                } },
            ));
            return .{
                .sugar = self.sugar,
                .inner = new_inner,
            };
        }

        pub fn project(self: Self, columns: anytype) Node(.Map) {
            return self.projectInner(coerceAnonToSlice(self.allocator, usize, columns));
        }

        fn projectInner(self: Self, columns: []usize) Node(.Map) {
            return self.map(ProjectMapper{ .allocator = self.sugar.allocator, .columns = columns, .mapper = .{ .mapFn = ProjectMapper.map } });
        }

        pub fn fixpoint(self: Self, future: anytype) void {
            if (!tag == .TimestampIncrement)
                @compileError("Can only call fixpoint on TimestampIncrement nodes, not {}", .{tag});
            const builder = &self.sugar.state.Building;
            builder.connectLoop(future.inner, self.inner);
        }

        pub fn output(self: Self) Node(.Output) {
            const builder = &self.sugar.state.Building;
            const subgraph = builder.node_subgraphs[self.inner.id];
            const new_inner = assert_ok(self.sugar.state.Building.addNode(
                subgraph,
                .{ .Output = .{ .input = self.inner } },
            ));
            return .{
                .sugar = self.sugar,
                .inner = new_inner,
            };
        }

        pub fn push(self: Self, change: anytype) !void {
            try self.pushInner(coerceAnonToChange(change));
        }

        fn pushInner(self: Self, change: dida.core.Change) !void {
            if (!tag == .Input)
                @compileError("Can only call push on Input nodes, not {}", .{tag});
            const shard = &self.sugar.state.Running;
            try shard.pushInput(self.inner, change);
        }

        pub fn flush(self: Self) !void {
            if (!tag == .Input)
                @compileError("Can only call push on Input nodes, not {}", .{tag});
            const shard = &self.sugar.state.Running;
            try shard.flushInput(self.inner);
        }

        pub fn advance(self: Self, timestamp: anytype) !void {
            try self.advanceInner(coerceAnonToTimestamp(timestamp));
        }

        pub fn advanceInner(self: Self, timestamp: dida.core.Timestamp) !void {
            if (!tag == .Input)
                @compileError("Can only call push on Input nodes, not {}", .{tag});
            const shard = &self.sugar.state.Running;
            try shard.advanceInput(self.inner, timestamp);
        }

        pub fn pop(self: Self) !dida.core.ChangeBatch {
            if (!tag == .Output)
                @compileError("Can only call push on Input nodes, not {}", .{tag});
            const shard = &self.sugar.state.Running;
            return shard.popOutput(self.inner);
        }
    };
}

fn coerceAnonToSlice(allocator: *Allocator, comptime Elem: type, columns: anytype) []Elem {
    const slice = assert_ok(allocator.alloc(Elem, columns.len));
    for (slice) |*elem, i| elem.* = columns[i];
    return slice;
}

const ProjectMapper = struct {
    allocator: *Allocator,
    columns: []usize,
    mapper: dida.core.NodeSpec.Map.Mapper,

    fn map(self: *dida.core.NodeSpec.Map.Mapper, input: dida.core.Row) error{OutOfMemory}!dida.core.Row {
        const project_mapper = @fieldParentPtr(ProjectMapper, "mapper", self);
        var output_values = assert_ok(project_mapper.allocator.alloc(dida.core.Value, project_mapper.columns.len));
        for (output_values) |*output_value, i| {
            output_value.* = input.values[project_mapper.columns[i]];
        }
        return dida.core.Row{ .values = output_values };
    }
};
