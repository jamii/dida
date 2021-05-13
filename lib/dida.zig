pub const meta = @import("dida/meta.zig");
pub const common = @import("dida/common.zig");
usingnamespace common;

pub const Node = struct {
    id: usize,
};

pub const Port = union(enum) {
    Input: usize,
    Output: usize,
};

pub const Location = struct {
    node: Node,
    port: Port,
};

pub const InputLocation = struct {
    node: Node,
    input_port: usize,
};

pub const OutputLocation = struct {
    node: Node,
    output_port: usize,
};

// Field names are weird to be consistent with std.math.Order
pub const PartialOrder = enum {
    lt,
    eq,
    gt,
    none,

    pub fn isLessThanOrEqual(self: PartialOrder) bool {
        return switch (self) {
            .lt, .eq => true,
            .gt, .none => false,
        };
    }
};

pub const Timestamp = struct {
    coords: []const usize,

    pub fn leastUpperBound(allocator: *Allocator, self: Timestamp, other: Timestamp) !Timestamp {
        release_assert(self.coords.len == other.coords.len, "Tried to take leastUpperBound of timestamps with different lengths: {} vs {}", .{ self.coords.len, other.coords.len });
        var output_coords = try allocator.alloc(usize, self.coords.len);
        for (self.coords) |self_coord, i| {
            const other_coord = other.coords[i];
            output_coords[i] = max(self_coord, other_coord);
        }
        return Timestamp{ .coords = output_coords };
    }

    pub fn causalOrder(self: Timestamp, other: Timestamp) PartialOrder {
        release_assert(self.coords.len == other.coords.len, "Tried to compute causalOrder of timestamps with different lengths: {} vs {}", .{ self.coords.len, other.coords.len });
        var lt: usize = 0;
        var gt: usize = 0;
        var eq: usize = 0;
        for (self.coords) |self_coord, i| {
            const other_coord = other.coords[i];
            switch (std.math.order(self_coord, other_coord)) {
                .lt => lt += 1,
                .eq => eq += 1,
                .gt => gt += 1,
            }
        }
        if (eq == self.coords.len) return .eq;
        if (lt + eq == self.coords.len) return .lt;
        if (gt + eq == self.coords.len) return .gt;
        return .none;
    }

    pub fn pushCoord(self: Timestamp, allocator: *Allocator) !Timestamp {
        var new_coords = try allocator.alloc(usize, self.coords.len + 1);
        std.mem.copy(usize, new_coords, self.coords);
        new_coords[new_coords.len - 1] = 0;
        return Timestamp{ .coords = new_coords };
    }

    pub fn incrementCoord(self: Timestamp, allocator: *Allocator) !Timestamp {
        var new_coords = try std.mem.dupe(allocator, usize, self.coords[0..self.coords.len]);
        new_coords[new_coords.len - 1] += 1;
        return Timestamp{ .coords = new_coords };
    }

    pub fn popCoord(self: Timestamp, allocator: *Allocator) !Timestamp {
        release_assert(self.coords.len > 0, "Tried to call popCoord on a timestamp with length 0", .{});
        const new_coords = try std.mem.dupe(allocator, usize, self.coords[0 .. self.coords.len - 1]);
        return Timestamp{ .coords = new_coords };
    }
};

pub const Pointstamp = struct {
    location: Location,
    timestamp: Timestamp,
};

pub const Value = union(enum) {
    String: []const u8,
    Number: f64,
};

pub const Row = struct {
    values: []const Value,
};

pub const Change = struct {
    row: Row,
    timestamp: Timestamp,
    diff: isize,
};

pub const ChangeBatchBuilder = struct {
    changes: ArrayList(Change),

    pub fn init(allocator: *Allocator) ChangeBatchBuilder {
        return ChangeBatchBuilder{
            .changes = ArrayList(Change).init(allocator),
        };
    }

    pub fn finishAndClear(self: *ChangeBatchBuilder) ChangeBatch {
        // TODO sort, consolidate
        release_assert(self.changes.items.len > 0, "Refusing to build an empty change batch", .{});
        return ChangeBatch{
            .changes = self.changes.toOwnedSlice(),
        };
    }
};

pub const ChangeBatch = struct {
    // TODO Invariant: non-empty, sorted by row/timestamp, no two changes with same row/timestamp
    changes: []Change,
};

pub const Index = struct {
    change_batches: ArrayList(ChangeBatch),

    pub fn init(allocator: *Allocator) Index {
        return Index{
            .change_batches = ArrayList(ChangeBatch).init(allocator),
        };
    }
};

pub const NodeSpec = union(enum) {
    Input,
    Map: MapSpec,
    Index: IndexSpec,
    Join: JoinSpec,
    Output: OutputSpec,
    TimestampPush: TimestampPushSpec,
    TimestampIncrement: TimestampIncrementSpec,
    TimestampPop: TimestampPopSpec,
    Union: UnionSpec,
    Distinct: DistinctSpec,

    pub const MapSpec = struct {
        input: OutputLocation,
        function: fn (row: Row) error{OutOfMemory}!Row,
    };

    pub const IndexSpec = struct {
        input: OutputLocation,
    };

    pub const JoinSpec = struct {
        inputs: [2]OutputLocation,
        key_columns: usize,
    };

    pub const OutputSpec = struct {
        input: OutputLocation,
    };

    pub const TimestampPushSpec = struct {
        input: OutputLocation,
    };

    pub const TimestampIncrementSpec = struct {
        input: OutputLocation,
    };

    pub const TimestampPopSpec = struct {
        input: OutputLocation,
    };

    pub const UnionSpec = struct {
        input1: OutputLocation,
        // Initially null, set when we create a backwards edge in a loop
        input2: ?OutputLocation,
    };

    pub const DistinctSpec = struct {
        input: OutputLocation,
    };

    pub fn numInputPorts(self: NodeSpec) usize {
        return switch (self) {
            .Input, .Map, .Index, .Output, .TimestampPush, .TimestampIncrement, .TimestampPop, .Distinct => 1,
            .Join, .Union => 2,
        };
    }

    pub fn numOutputPorts(self: NodeSpec) usize {
        return switch (self) {
            .Input, .Map, .Index, .Output, .Join, .TimestampPush, .TimestampIncrement, .TimestampPop, .Union, .Distinct => 1,
        };
    }
};

pub const NodeState = union(enum) {
    Input: InputState,
    Map,
    Index: Index,
    Join,
    Output: ArrayList(ChangeBatch),
    TimestampPush,
    TimestampIncrement,
    TimestampPop,
    Union,
    Distinct,

    pub const InputState = struct {
        unflushed_changes: ChangeBatchBuilder,
        frontier: Frontier,
    };

    pub fn init(allocator: *Allocator, node_spec: NodeSpec) NodeState {
        return switch (node_spec) {
            .Input => .{
                .Input = .{
                    .unflushed_changes = ChangeBatchBuilder.init(allocator),
                    .frontier = Frontier.init(allocator),
                },
            },
            .Map => .Map,
            .Index => .{ .Index = Index.init(allocator) },
            .Join => .Join,
            .Output => .{ .Output = ArrayList(ChangeBatch).init(allocator) },
            .TimestampPush => .TimestampPush,
            .TimestampIncrement => .TimestampIncrement,
            .TimestampPop => .TimestampPop,
            .Union => .Union,
            .Distinct => .Distinct,
        };
    }
};

pub const GraphBuilder = struct {
    allocator: *Allocator,
    node_specs: ArrayList(NodeSpec),

    pub fn init(allocator: *Allocator) GraphBuilder {
        return GraphBuilder{
            .allocator = allocator,
            .node_specs = ArrayList(NodeSpec).init(allocator),
        };
    }

    pub fn addNode(self: *GraphBuilder, node_spec: NodeSpec) !Node {
        // TODO check all edges are valid
        switch (node_spec) {
            .Join => |join| {
                for (join.inputs) |input_location| {
                    release_assert(self.node_specs.items[input_location.node.id] == .Index, "Inputs to Join node must be Index nodes", .{});
                }
            },
            .Distinct => |distinct| {
                release_assert(self.node_specs.items[distinct.input.node.id] == .Index, "Input to Distinct node must be an Index node", .{});
            },
            .Input, .Map, .Index, .Output, .TimestampPush, .TimestampIncrement, .TimestampPop, .Union => {},
        }
        const node = Node{ .id = self.node_specs.items.len };
        try self.node_specs.append(node_spec);
        return node;
    }

    pub fn finishAndClear(self: *GraphBuilder) !Graph {
        var downstream_locations = try self.allocator.alloc(ArrayList(InputLocation), self.node_specs.items.len);
        var upstream_locations = try self.allocator.alloc(ArrayList(OutputLocation), self.node_specs.items.len);
        for (self.node_specs.items) |_, node_id| {
            downstream_locations[node_id] = ArrayList(InputLocation).init(self.allocator);
            upstream_locations[node_id] = ArrayList(OutputLocation).init(self.allocator);
        }
        for (self.node_specs.items) |node_spec, node_id| {
            const node_inputs = switch (node_spec) {
                .Input => &[_]OutputLocation{},
                .Map => |map| &[_]OutputLocation{map.input},
                .Index => |index| &[_]OutputLocation{index.input},
                .Join => |join| &[_]OutputLocation{ join.inputs[0], join.inputs[1] },
                .Output => |output| &[_]OutputLocation{output.input},
                .TimestampPush => |timestamp_push| &[_]OutputLocation{timestamp_push.input},
                .TimestampIncrement => |timestamp_increment| &[_]OutputLocation{timestamp_increment.input},
                .TimestampPop => |timestamp_pop| &[_]OutputLocation{timestamp_pop.input},
                .Union => |union_| &[_]OutputLocation{ union_.input1, union_.input2.? },
                .Distinct => |distinct| &[_]OutputLocation{distinct.input},
            };
            for (node_inputs) |node_input, node_port| {
                try downstream_locations[node_input.node.id].append(.{ .node = .{ .id = node_id }, .input_port = node_port });
                try upstream_locations[node_id].append(.{ .node = .{ .id = node_input.node.id }, .output_port = node_input.output_port });
            }
        }
        var frozen_downstream_locations = try self.allocator.alloc([]InputLocation, self.node_specs.items.len);
        var frozen_upstream_locations = try self.allocator.alloc([]OutputLocation, self.node_specs.items.len);
        for (downstream_locations) |*locations, node_id|
            frozen_downstream_locations[node_id] = locations.toOwnedSlice();
        for (upstream_locations) |*locations, node_id|
            frozen_upstream_locations[node_id] = locations.toOwnedSlice();
        return Graph{
            .allocator = self.allocator,
            .node_specs = self.node_specs.toOwnedSlice(),
            .downstream_locations = frozen_downstream_locations,
            .upstream_locations = frozen_upstream_locations,
        };
    }
};

pub const Graph = struct {
    allocator: *Allocator,
    node_specs: []const NodeSpec,
    downstream_locations: []const []const InputLocation,
    upstream_locations: []const []const OutputLocation,
};

const Updated = enum {
    Updated,
    NotUpdated,

    fn merge(self: Updated, other: Updated) Updated {
        if (self == .Updated) return .Updated;
        if (other == .Updated) return .Updated;
        return .NotUpdated;
    }
};

pub const Frontier = struct {
    allocator: *Allocator,
    // Invariant: for any two timestamps A and B in lower_bounds `A.causalOrder(B) == .none`
    lower_bounds: DeepHashSet(Timestamp),

    pub fn init(allocator: *Allocator) Frontier {
        return Frontier{
            .allocator = allocator,
            .lower_bounds = DeepHashSet(Timestamp).init(allocator),
        };
    }

    pub fn clone(self: Frontier) !Frontier {
        return Frontier{
            .allocator = self.allocator,
            .lower_bounds = try self.lower_bounds.clone(),
        };
    }

    pub fn causalOrder(self: *Frontier, timestamp: Timestamp) PartialOrder {
        var iter = self.lower_bounds.iterator();
        while (iter.next()) |kv| {
            const other_timestamp = kv.key;
            const order = other_timestamp.causalOrder(timestamp);
            switch (order) {
                .none => {},
                else => return order,
            }
        }
        return .none;
    }

    pub fn insertTimestamp(self: *Frontier, timestamp: Timestamp) !Updated {
        var dominated = ArrayList(Timestamp).init(self.allocator);
        var iter = self.lower_bounds.iterator();
        while (iter.next()) |kv| {
            const other_timestamp = kv.key;
            switch (other_timestamp.causalOrder(timestamp)) {
                .lt => try dominated.append(other_timestamp),
                .eq => return .NotUpdated,
                .gt => panic("Frontier went backwards, from {} to {}", .{ other_timestamp, timestamp }),
                .none => {},
            }
        }
        if (dominated.items.len > 0) {
            for (dominated.items) |other_timestamp| {
                self.lower_bounds.removeAssertDiscard(other_timestamp);
            }
            return .Updated;
        }
        return .NotUpdated;
    }

    pub fn merge(self: *Frontier, other: Frontier) !Updated {
        var updated: Updated = .NotUpdated;
        var iter = other.lower_bounds.iterator();
        while (iter.next()) |kv| {
            const other_timestamp = kv.key;
            updated = updated.merge(try self.insertTimestamp(other_timestamp));
        }
        return updated;
    }
};

pub const Shard = struct {
    allocator: *Allocator,
    graph: Graph,
    node_states: []NodeState,
    unprocessed_changes: ArrayList(ChangeBatchAtLocation),

    const ChangeBatchAtLocation = struct {
        change_batch: ChangeBatch,
        location: Location,
    };

    pub fn init(allocator: *Allocator, graph: Graph) !Shard {
        var node_states = try allocator.alloc(NodeState, graph.node_specs.len);
        for (node_states) |*node_state, i| {
            node_state.* = NodeState.init(allocator, graph.node_specs[i]);
        }
        return Shard{
            .allocator = allocator,
            .graph = graph,
            .node_states = node_states,
            .unprocessed_changes = ArrayList(ChangeBatchAtLocation).init(allocator),
        };
    }

    pub fn pushInput(self: *Shard, node: Node, change: Change) !void {
        try self.node_states[node.id].Input.unflushed_changes.changes.append(change);
    }

    pub fn flushInput(self: *Shard, node: Node) !void {
        var unflushed_changes = &self.node_states[node.id].Input.unflushed_changes;
        if (unflushed_changes.changes.items.len > 0) {
            const change_batch = unflushed_changes.finishAndClear();
            try self.unprocessed_changes.append(.{
                .change_batch = change_batch,
                .location = .{ .node = node, .port = .{ .Output = 0 } },
            });
        }
    }

    pub fn advanceInput(self: *Shard, node: Node, timestamp: Timestamp) !void {
        _ = try self.node_states[node.id].Input.frontier.insertTimestamp(timestamp);
    }

    pub fn computeFrontiers(self: *Shard) ![]const Frontier {
        // frontiers[node.id] is the frontier at the *output* of node
        // Invariant: for any future change, frontiers[change.node.id].compare(change.timestamp).isLessThanOrEqual()
        var frontiers = try self.allocator.alloc(Frontier, self.graph.node_specs.len);

        var must_recompute = DeepHashSet(Node).init(self.allocator);

        // init frontiers
        for (self.node_states) |node_state, node_id| {
            switch (node_state) {
                .Input => |input_state| {
                    frontiers[node_id] = try input_state.frontier.clone();
                    for (self.graph.downstream_locations[node_id]) |location| {
                        try must_recompute.put(location.node, {});
                    }
                },
                else => {
                    frontiers[node_id] = Frontier.init(self.allocator);
                },
            }
        }

        // fixpoint frontiers
        while (must_recompute.count() > 0) {
            // const node = must_recompute.pop();
            const node = must_recompute.iterator().next().?.key;
            must_recompute.removeAssertDiscard(node);

            var input_frontier = Frontier.init(self.allocator);
            for (self.graph.upstream_locations[node.id]) |upstream_location| {
                _ = try input_frontier.merge(frontiers[upstream_location.node.id]);
            }
            var output_frontier = &frontiers[node.id];
            var updated: Updated = .NotUpdated;
            var iter = input_frontier.lower_bounds.iterator();
            while (iter.next()) |kv| {
                const input_timestamp = kv.key;
                const output_timestamp = switch (self.graph.node_specs[node.id]) {
                    .TimestampPush => try input_timestamp.pushCoord(self.allocator),
                    .TimestampIncrement => try input_timestamp.incrementCoord(self.allocator),
                    .TimestampPop => try input_timestamp.popCoord(self.allocator),
                    else => input_timestamp,
                };
                updated = updated.merge(try output_frontier.insertTimestamp(output_timestamp));
            }
            if (updated == .Updated) {
                for (self.graph.downstream_locations[node.id]) |output_location| {
                    try must_recompute.put(output_location.node, {});
                }
            }
        }

        return frontiers;
    }

    pub fn hasWork(self: Shard) bool {
        return self.unprocessed_changes.items.len > 0;
    }

    pub fn doWork(self: *Shard) !void {
        // TODO need to schedule operators when their frontier changes too
        if (self.unprocessed_changes.popOrNull()) |change_batch_at_location| {
            const frontiers = self.computeFrontiers();

            const change_batch = change_batch_at_location.change_batch;
            const location = change_batch_at_location.location;
            switch (location.port) {
                .Input => |input_port| {
                    const node_spec = self.graph.node_specs[location.node.id];
                    switch (node_spec) {
                        .Input => panic("Input nodes should not have work pending on their input port", .{}),
                        .Map => |map| {
                            var output_change_batch = ChangeBatchBuilder.init(self.allocator);
                            for (change_batch.changes) |change| {
                                var output_change = change; // copy
                                output_change.row = try map.function(change.row);
                                try output_change_batch.changes.append(output_change);
                            }
                            try self.unprocessed_changes.append(.{
                                .change_batch = output_change_batch.finishAndClear(),
                                .location = .{ .node = location.node, .port = .{ .Output = 0 } },
                            });
                        },
                        .Index => {
                            const index = &self.node_states[location.node.id].Index;
                            try index.change_batches.append(change_batch);
                            try self.unprocessed_changes.append(.{
                                .change_batch = change_batch,
                                .location = .{ .node = location.node, .port = .{ .Output = 0 } },
                            });
                        },
                        .Join => |join| {
                            const index = &self.node_states[join.inputs[1 - input_port].node.id].Index;
                            var output_change_batch = ChangeBatchBuilder.init(self.allocator);
                            for (change_batch.changes) |change| {
                                const this_key = change.row.values[0..join.key_columns];
                                for (index.change_batches.items) |other_change_batch| {
                                    for (other_change_batch.changes) |other_change| {
                                        const other_key = other_change.row.values[0..join.key_columns];
                                        if (meta.deepEqual(this_key, other_key)) {
                                            const values = switch (input_port) {
                                                0 => &[2][]const Value{ change.row.values, other_change.row.values },
                                                1 => &[2][]const Value{ other_change.row.values, change.row.values },
                                                else => panic("Bad input port for join: {}", .{input_port}),
                                            };
                                            const output_change = Change{
                                                .row = .{ .values = try std.mem.concat(self.allocator, Value, values) },
                                                .diff = change.diff * other_change.diff,
                                                .timestamp = try Timestamp.leastUpperBound(self.allocator, change.timestamp, other_change.timestamp),
                                            };
                                            try output_change_batch.changes.append(output_change);
                                        }
                                    }
                                }
                            }
                            if (output_change_batch.changes.items.len > 0) {
                                try self.unprocessed_changes.append(.{
                                    .change_batch = output_change_batch.finishAndClear(),
                                    .location = .{ .node = location.node, .port = .{ .Output = 0 } },
                                });
                            }
                        },
                        .Output => {
                            const outputs = &self.node_states[location.node.id].Output;
                            try outputs.append(change_batch);
                        },
                        .TimestampPush => {
                            var output_change_batch = ChangeBatchBuilder.init(self.allocator);
                            for (change_batch.changes) |change| {
                                var output_change = change;
                                output_change.timestamp = try change.timestamp.pushCoord(self.allocator);
                                try output_change_batch.changes.append(output_change);
                            }
                            try self.unprocessed_changes.append(.{
                                .change_batch = output_change_batch.finishAndClear(),
                                .location = .{ .node = location.node, .port = .{ .Output = 0 } },
                            });
                        },
                        .TimestampIncrement => {
                            var output_change_batch = ChangeBatchBuilder.init(self.allocator);
                            for (change_batch.changes) |change| {
                                var output_change = change;
                                output_change.timestamp = try change.timestamp.incrementCoord(self.allocator);
                                try output_change_batch.changes.append(output_change);
                            }
                            try self.unprocessed_changes.append(.{
                                .change_batch = output_change_batch.finishAndClear(),
                                .location = .{ .node = location.node, .port = .{ .Output = 0 } },
                            });
                        },
                        .TimestampPop => {
                            var output_change_batch = ChangeBatchBuilder.init(self.allocator);
                            for (change_batch.changes) |change| {
                                var output_change = change;
                                output_change.timestamp = try change.timestamp.popCoord(self.allocator);
                                try output_change_batch.changes.append(output_change);
                            }
                            try self.unprocessed_changes.append(.{
                                .change_batch = output_change_batch.finishAndClear(),
                                .location = .{ .node = location.node, .port = .{ .Output = 0 } },
                            });
                        },
                        .Union => {
                            // Pass straight through to output port
                            try self.unprocessed_changes.append(.{
                                .change_batch = change_batch,
                                .location = .{ .node = location.node, .port = .{ .Output = 0 } },
                            });
                        },
                        .Distinct => |distinct| {
                            const index = &self.node_states[distinct.input.node.id].Index;
                            // Need frontiers to implement this correctly
                            TODO();
                        },
                    }
                },
                .Output => |output_port| {
                    // Forward to all nodes that have this location as an input
                    for (self.graph.downstream_locations[location.node.id]) |downstream_location| {
                        try self.unprocessed_changes.append(.{
                            .change_batch = change_batch,
                            .location = .{
                                .node = downstream_location.node,
                                .port = .{ .Input = downstream_location.input_port },
                            },
                        });
                    }
                },
            }
        }
    }

    pub fn popOutput(self: *Shard, node: Node) ?ChangeBatch {
        return self.node_states[node.id].Output.popOrNull();
    }
};
