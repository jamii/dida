usingnamespace @import("./common.zig");

// TODO Its currently possible to remove from HashMap without invalidating interator which would simplify some of the code below. But might not be true forever.

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

    pub fn initLeast(allocator: *Allocator, num_coords: usize) !Timestamp {
        var coords = try allocator.alloc(usize, num_coords);
        for (coords) |*coord| coord.* = 0;
        return Timestamp{ .coords = coords };
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
        assert(self.coords.len > 0, "Tried to call popCoord on a timestamp with length 0", .{});
        const new_coords = try std.mem.dupe(allocator, usize, self.coords[0 .. self.coords.len - 1]);
        return Timestamp{ .coords = new_coords };
    }

    pub fn leastUpperBound(allocator: *Allocator, self: Timestamp, other: Timestamp) !Timestamp {
        assert(self.coords.len == other.coords.len, "Tried to compute leastUpperBound of timestamps with different lengths: {} vs {}", .{ self.coords.len, other.coords.len });
        var output_coords = try allocator.alloc(usize, self.coords.len);
        for (self.coords) |self_coord, i| {
            const other_coord = other.coords[i];
            output_coords[i] = max(self_coord, other_coord);
        }
        return Timestamp{ .coords = output_coords };
    }

    pub fn causalOrder(self: Timestamp, other: Timestamp) PartialOrder {
        assert(self.coords.len == other.coords.len, "Tried to compute causalOrder of timestamps with different lengths: {} vs {}", .{ self.coords.len, other.coords.len });
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

    pub fn lexicalOrder(self: Timestamp, other: Timestamp) std.math.Order {
        assert(self.coords.len == other.coords.len, "Tried to compute lexicalOrder of timestamps with different lengths: {} vs {}", .{ self.coords.len, other.coords.len });
        for (self.coords) |self_coord, i| {
            const other_coord = other.coords[i];
            switch (std.math.order(self_coord, other_coord)) {
                .lt => return .lt,
                .eq => {},
                .gt => return .gt,
            }
        }
        return .eq;
    }
};

pub const Frontier = struct {
    allocator: *Allocator,
    // Invariant: timestamps don't overlap - for any two timestamps t1 and t2 in timestamps `t1.causalOrder(t2) == .none`
    timestamps: DeepHashSet(Timestamp),

    pub fn initEmpty(allocator: *Allocator) Frontier {
        return Frontier{
            .allocator = allocator,
            .timestamps = DeepHashSet(Timestamp).init(allocator),
        };
    }

    pub fn causalOrder(self: Frontier, timestamp: Timestamp) std.math.Order {
        var iter = self.timestamps.iterator();
        while (iter.next()) |entry| {
            const order = entry.key_ptr.causalOrder(timestamp);
            switch (order) {
                .lt => return .lt,
                .eq => return .eq,
                .gt => return .gt,
                .none => {},
            }
        }
        return .gt;
    }

    pub const Direction = enum { Advance, Retreat };

    pub fn move(self: *Frontier, comptime direction: Direction, timestamp: Timestamp, changes_into: *ArrayList(FrontierChange)) !void {
        assert(changes_into.items.len == 0, "Need to start with an empty changes_into buffer so can use it to remove timestamps", .{});
        var iter = self.timestamps.iterator();
        while (iter.next()) |entry| {
            switch (timestamp.causalOrder(entry.key_ptr.*)) {
                .eq, if (direction == .Advance) .lt else .gt => {
                    assert(changes_into.items.len == 0, "Frontier timestamps invariant was broken", .{});
                    return;
                },
                if (direction == .Advance) .gt else .lt => {
                    try changes_into.append(.{ .timestamp = entry.key_ptr.*, .diff = -1 });
                },
                .none => {},
            }
        }
        // If we got this far, timestamp is being added to the frontier and might also be replacing some other timestamps that are currently on the frontier
        for (changes_into.items) |change| {
            _ = self.timestamps.remove(change.timestamp);
        }
        try changes_into.append(.{ .timestamp = timestamp, .diff = 1 });
        try self.timestamps.put(timestamp, {});
    }

    pub fn retreat(self: *Frontier, timestamp: Timestamp) !void {
        var changes_into = ArrayList(FrontierChange).init(self.allocator);
        try self.move(.Retreat, timestamp, &changes_into);
    }
};

pub const FrontierChange = struct {
    timestamp: Timestamp,
    diff: isize,
};

pub const SupportedFrontier = struct {
    allocator: *Allocator,
    support: DeepHashMap(Timestamp, usize),
    // Invariant: frontier contains exactly the least timestamps from support
    frontier: Frontier,

    pub fn initEmpty(allocator: *Allocator) !SupportedFrontier {
        return SupportedFrontier{
            .allocator = allocator,
            .support = DeepHashMap(Timestamp, usize).init(allocator),
            .frontier = Frontier.initEmpty(allocator),
        };
    }

    pub fn update(self: *SupportedFrontier, timestamp: Timestamp, diff: isize, changes_into: *ArrayList(FrontierChange)) !void {
        const support_entry = try self.support.getOrPutValue(timestamp, 0);
        support_entry.value_ptr.* = @intCast(usize, @intCast(isize, support_entry.value_ptr.*) + diff);

        if (support_entry.value_ptr.* == 0) {
            // Timestamp was just removed, might have been in frontier
            _ = self.support.remove(timestamp);
            if (self.frontier.timestamps.remove(timestamp)) {
                // Remove this timestamp
                try changes_into.append(.{ .timestamp = timestamp, .diff = -1 });

                // Find timestamps in support that might now be on the frontier
                var candidates = ArrayList(Timestamp).init(self.allocator);
                var iter = self.support.iterator();
                while (iter.next()) |entry| {
                    if (timestamp.causalOrder(entry.key_ptr.*) == .lt)
                        try candidates.append(entry.key_ptr.*);
                }

                // Add in lexical order any candidates that are not past the current frontier (or past any earlier candidates)
                std.sort.sort(Timestamp, candidates.items, {}, struct {
                    fn lessThan(_: void, a: Timestamp, b: Timestamp) bool {
                        return a.lexicalOrder(b) == .lt;
                    }
                }.lessThan);
                for (candidates.items) |candidate| {
                    if (self.frontier.causalOrder(candidate) == .gt) {
                        try self.frontier.timestamps.put(candidate, {});
                        try changes_into.append(.{ .timestamp = candidate, .diff = 1 });
                    }
                }
            }
        }

        if (support_entry.value_ptr.* == diff) {
            // Timestamp was just added, might be in frontier
            if (self.frontier.causalOrder(timestamp) != .lt) {
                // Add to frontier
                try self.frontier.timestamps.put(timestamp, {});
                try changes_into.append(.{ .timestamp = timestamp, .diff = 1 });

                // Remove any other timestamp that is greater than the new timestamp
                var to_remove = ArrayList(Timestamp).init(self.allocator);
                var iter = self.frontier.timestamps.iterator();
                while (iter.next()) |frontier_entry| {
                    if (frontier_entry.key_ptr.causalOrder(timestamp) == .gt)
                        try to_remove.append(frontier_entry.key_ptr.*);
                }
                for (to_remove.items) |other_timestamp| {
                    _ = self.frontier.timestamps.remove(other_timestamp);
                    try changes_into.append(.{ .timestamp = other_timestamp, .diff = -1 });
                }
            }
        }
    }
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
    allocator: *Allocator,
    changes: ArrayList(Change),

    pub fn init(allocator: *Allocator) ChangeBatchBuilder {
        return ChangeBatchBuilder{
            .allocator = allocator,
            .changes = ArrayList(Change).init(allocator),
        };
    }

    pub fn finishAndClear(self: *ChangeBatchBuilder) !?ChangeBatch {
        if (self.changes.items.len == 0) return null;

        std.sort.sort(Change, self.changes.items, {}, struct {
            fn lessThan(_: void, a: Change, b: Change) bool {
                return dida.meta.deepOrder(a, b) == .lt;
            }
        }.lessThan);

        // Coalesce changes with identical rows and timestamps
        var prev_i: usize = 0;
        for (self.changes.items[1..]) |change, i| {
            const prev_change = &self.changes.items[prev_i];
            if (dida.meta.deepEqual(prev_change.row, change.row) and dida.meta.deepEqual(prev_change.timestamp, change.timestamp)) {
                prev_change.diff += change.diff;
            } else {
                if (prev_change.diff != 0) {
                    prev_i += 1;
                    self.changes.items[prev_i] = change;
                }
            }
        }
        self.changes.shrinkAndFree(prev_i + 1);
        if (self.changes.items.len == 0) return null;

        var lower_bound = Frontier.initEmpty(self.allocator);
        for (self.changes.items) |change| {
            try lower_bound.retreat(change.timestamp);
        }

        return ChangeBatch{
            .lower_bound = lower_bound,
            .changes = self.changes.toOwnedSlice(),
        };
    }
};

pub const ChangeBatch = struct {
    // Invariant: for every change in changes, lower_bound.causalOrder(change).isLessThanOrEqual()
    lower_bound: Frontier,
    // Invariant: non-empty,
    // Invariant: sorted by row/timestamp
    // Invariant: no two changes with same row/timestamp
    changes: []Change,
};

pub const Index = struct {
    allocator: *Allocator,
    change_batches: ArrayList(ChangeBatch),

    pub fn init(allocator: *Allocator) Index {
        return .{
            .allocator = allocator,
            .change_batches = ArrayList(ChangeBatch).init(allocator),
        };
    }

    pub fn getBagAsOf(self: *Index, allocator: *Allocator, timestamp: Timestamp) !Bag {
        var bag = Bag.init(allocator);
        for (self.change_batches.items) |change_batch| {
            if (change_batch.lower_bound.causalOrder(timestamp) != .gt) {
                for (change_batch.changes) |change| {
                    if (change.timestamp.causalOrder(timestamp).isLessThanOrEqual()) {
                        try bag.update(change.row, change.diff);
                    }
                }
            }
        }
        return bag;
    }
};

pub const Bag = struct {
    row_counts: DeepHashMap(Row, isize),

    pub fn init(allocator: *Allocator) Bag {
        return .{
            .row_counts = DeepHashMap(Row, isize).init(allocator),
        };
    }

    pub fn update(self: *Bag, row: Row, diff: isize) !void {
        const entry = try self.row_counts.getOrPutValue(row, 0);
        entry.value_ptr.* += diff;
        _ = if (entry.value_ptr.* == 0) self.row_counts.remove(row);
    }
};

pub const Node = struct {
    id: usize,
};

pub const NodeInput = struct {
    node: Node,
    input_ix: usize,
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
        input: Node,
        mapper: *Mapper,

        pub const Mapper = struct {
            map_fn: fn (self: *Mapper, row: Row) error{OutOfMemory}!Row,
        };
    };

    pub const IndexSpec = struct {
        input: Node,
    };

    pub const JoinSpec = struct {
        inputs: [2]Node,
        key_columns: usize,
    };

    pub const OutputSpec = struct {
        input: Node,
    };

    pub const TimestampPushSpec = struct {
        input: Node,
    };

    pub const TimestampIncrementSpec = struct {
        // Initially null, will be set later to a future edge
        input: ?Node,
    };

    pub const TimestampPopSpec = struct {
        input: Node,
    };

    pub const UnionSpec = struct {
        inputs: [2]Node,
    };

    pub const DistinctSpec = struct {
        input: Node,
    };

    pub fn getInputs(self: *const NodeSpec) []const Node {
        return switch (self.*) {
            .Input => |_| &[_]Node{},
            .Map => |*spec| ptrToSlice(Node, &spec.input),
            .Index => |*spec| ptrToSlice(Node, &spec.input),
            .Output => |*spec| ptrToSlice(Node, &spec.input),
            .TimestampPush => |*spec| ptrToSlice(Node, &spec.input),
            .TimestampIncrement => |*spec| ptrToSlice(Node, &spec.input.?),
            .TimestampPop => |*spec| ptrToSlice(Node, &spec.input),
            .Distinct => |*spec| ptrToSlice(Node, &spec.input),
            .Join => |*spec| &spec.inputs,
            .Union => |*spec| &spec.inputs,
        };
    }

    pub fn hasIndex(self: NodeSpec) bool {
        return tagHasIndex(self);
    }

    // TODO this is a weird way to organize this code
    pub fn tagHasIndex(tag: std.meta.TagType(NodeSpec)) bool {
        return switch (tag) {
            .Index, .Distinct => true,
            else => false,
        };
    }
};

pub const NodeState = union(enum) {
    Input: InputState,
    Map,
    Index: IndexState,
    Join,
    Output: OutputState,
    TimestampPush,
    TimestampIncrement,
    TimestampPop,
    Union,
    Distinct: DistinctState,

    pub const InputState = struct {
        frontier: Frontier,
        unflushed_changes: ChangeBatchBuilder,
    };

    pub const IndexState = struct {
        index: Index,
        // These are waiting for the frontier to advance before they are added to the index
        pending_changes: ArrayList(Change),
    };

    pub const OutputState = struct {
        unpopped_change_batches: ArrayList(ChangeBatch),
    };

    pub const DistinctState = struct {
        index: Index,
        // These are times in the future at which the output might change even if there is no new input.
        // To calculate:
        // * Take the leastUpperBound of the timestamps of every possible subset of changes in the input node.
        // * Filter out timestamps that are before the output frontier of this node.
        pending_timestamps: DeepHashSet(Timestamp),
    };

    pub fn init(allocator: *Allocator, node_spec: NodeSpec) NodeState {
        return switch (node_spec) {
            .Input => |input_spec| .{
                .Input = .{
                    .frontier = Frontier.initEmpty(allocator),
                    .unflushed_changes = ChangeBatchBuilder.init(allocator),
                },
            },
            .Map => .Map,
            .Index => .{
                .Index = .{
                    .index = Index.init(allocator),
                    .pending_changes = ArrayList(Change).init(allocator),
                },
            },
            .Join => .Join,
            .Output => .{
                .Output = .{
                    .unpopped_change_batches = ArrayList(ChangeBatch).init(allocator),
                },
            },
            .TimestampPush => .TimestampPush,
            .TimestampIncrement => .TimestampIncrement,
            .TimestampPop => .TimestampPop,
            .Union => .Union,
            .Distinct => .{
                .Distinct = .{
                    .index = Index.init(allocator),
                    .pending_timestamps = DeepHashSet(Timestamp).init(allocator),
                },
            },
        };
    }

    pub fn getIndex(self: *NodeState) ?*Index {
        // TODO unsure if this is correct way to get payload pointer
        return switch (self.*) {
            .Index => |*state| &state.index,
            .Distinct => |*state| &state.index,
            // TODO should be able to follow TimestampPush upwards to an index and wrap it
            else => null,
        };
    }
};

pub const Subgraph = struct {
    id: usize,
};

pub const GraphBuilder = struct {
    allocator: *Allocator,
    node_specs: ArrayList(NodeSpec),
    node_subgraphs: ArrayList(Subgraph),
    subgraph_parents: ArrayList(Subgraph),

    pub fn init(allocator: *Allocator) GraphBuilder {
        return GraphBuilder{
            .allocator = allocator,
            .node_specs = ArrayList(NodeSpec).init(allocator),
            .node_subgraphs = ArrayList(Subgraph).init(allocator),
            .subgraph_parents = ArrayList(Subgraph).init(allocator),
        };
    }

    pub fn addSubgraph(self: *GraphBuilder, parent: Subgraph) !Subgraph {
        try self.subgraph_parents.append(parent);
        return Subgraph{ .id = self.subgraph_parents.items.len };
    }

    pub fn addNode(self: *GraphBuilder, subgraph: Subgraph, node_spec: NodeSpec) !Node {
        const node = Node{ .id = self.node_specs.items.len };
        try self.node_specs.append(node_spec);
        try self.node_subgraphs.append(subgraph);
        return node;
    }

    pub fn connectLoop(self: *GraphBuilder, later_node: Node, earlier_node: Node) void {
        self.node_specs.items[earlier_node.id].TimestampIncrement.input = later_node;
    }

    pub fn finishAndClear(self: *GraphBuilder) !Graph {
        return Graph.init(
            self.allocator,
            self.node_specs.toOwnedSlice(),
            self.node_subgraphs.toOwnedSlice(),
            self.subgraph_parents.toOwnedSlice(),
        );
    }
};

pub const Graph = struct {
    allocator: *Allocator,
    node_specs: []const NodeSpec,
    node_subgraphs: []const []const Subgraph,
    // Indexed by subgraph.id-1, because subgraph 0 has no parent
    subgraph_parents: []const Subgraph,
    downstream_node_inputs: []const []const NodeInput,

    pub fn init(allocator: *Allocator, node_specs: []const NodeSpec, node_immediate_subgraphs: []const Subgraph, subgraph_parents: []const Subgraph) !Graph {
        const num_nodes = node_specs.len;
        const num_subgraphs = subgraph_parents.len + 1; // +1 because subgraph 0 has no parent
        dida.common.assert(
            node_immediate_subgraphs.len == num_nodes,
            "node_specs and node_immediate_subgraphs should have same length, got {} vs {}",
            .{ node_immediate_subgraphs.len, num_nodes },
        );

        // For each node, store its subgraph, its subgraphs parent, its subgraphs parents parent etc
        var node_subgraphs = try allocator.alloc([]Subgraph, num_nodes);
        for (node_immediate_subgraphs) |immediate_subgraph, node_id| {
            var subgraphs = ArrayList(Subgraph).init(allocator);
            var subgraph = immediate_subgraph;
            while (true) {
                try subgraphs.append(subgraph);
                if (subgraph.id == 0) break;
                subgraph = subgraph_parents[subgraph.id - 1];
            }
            std.mem.reverse(Subgraph, subgraphs.items);
            node_subgraphs[node_id] = subgraphs.toOwnedSlice();
        }

        // Collect downstream nodes
        var downstream_node_inputs = try allocator.alloc(ArrayList(NodeInput), num_nodes);
        for (node_specs) |_, node_id| {
            downstream_node_inputs[node_id] = ArrayList(NodeInput).init(allocator);
        }
        for (node_specs) |node_spec, node_id| {
            for (node_spec.getInputs()) |input_node, input_ix| {
                try downstream_node_inputs[input_node.id].append(.{ .node = .{ .id = node_id }, .input_ix = input_ix });
            }
        }
        var frozen_downstream_node_inputs = try allocator.alloc([]NodeInput, node_specs.len);
        for (downstream_node_inputs) |*node_inputs, node_id|
            frozen_downstream_node_inputs[node_id] = node_inputs.toOwnedSlice();

        var self = Graph{
            .allocator = allocator,
            .node_specs = node_specs,
            .node_subgraphs = node_subgraphs,
            .subgraph_parents = subgraph_parents,
            .downstream_node_inputs = frozen_downstream_node_inputs,
        };

        self.validate();

        return self;
    }

    pub fn validate(self: Graph) void {
        const num_nodes = self.node_specs.len;
        for (self.subgraph_parents) |parent, subgraph_id_minus_one| {
            assert(
                parent.id < subgraph_id_minus_one + 1,
                "The parent of a subgraph must have a smaller id than its child",
                .{},
            );
        }
        for (self.node_specs) |node_spec, node_id| {
            for (node_spec.getInputs()) |input_node, input_ix| {
                assert(input_node.id < num_nodes, "All input nodes must exist", .{});
                if (node_spec == .TimestampIncrement) {
                    assert(
                        input_node.id > node_id,
                        "TimestampIncrement nodes must have a later node as input",
                        .{},
                    );
                } else {
                    assert(
                        input_node.id < node_id,
                        "All nodes (other than TimestampIncrement) must have an earlier node as input",
                        .{},
                    );
                }
                switch (node_spec) {
                    .Join, .Distinct => assert(
                        self.node_specs[input_node.id].hasIndex(),
                        "Inputs to {} node must contain an index",
                        .{std.meta.activeTag(node_spec)},
                    ),
                    else => {},
                }
                switch (node_spec) {
                    .TimestampPush => {
                        const input_subgraph = last(Subgraph, self.node_subgraphs[input_node.id]);
                        const output_subgraph = last(Subgraph, self.node_subgraphs[node_id]);
                        assert(
                            output_subgraph.id > 0,
                            "TimestampPush nodes cannot have an output on subgraph 0",
                            .{},
                        );
                        assert(
                            self.subgraph_parents[output_subgraph.id - 1].id == input_subgraph.id,
                            "TimestampPush nodes must cross from a parent subgraph to a child subgraph",
                            .{},
                        );
                    },
                    .TimestampPop => {
                        const input_subgraph = last(Subgraph, self.node_subgraphs[input_node.id]);
                        const output_subgraph = last(Subgraph, self.node_subgraphs[node_id]);
                        assert(
                            input_subgraph.id > 0,
                            "TimestampPop nodes cannot have an input on subgraph 0",
                            .{},
                        );
                        assert(
                            self.subgraph_parents[input_subgraph.id - 1].id == output_subgraph.id,
                            "TimestampPop nodes must cross from a child subgraph to a parent subgraph",
                            .{},
                        );
                    },
                    else => {
                        const input_subgraph = last(Subgraph, self.node_subgraphs[input_node.id]);
                        const output_subgraph = last(Subgraph, self.node_subgraphs[node_id]);
                        assert(
                            input_subgraph.id == output_subgraph.id,
                            "Nodes (other than TimestampPop and TimestampPush) must be on the same subgraph as their inputs",
                            .{},
                        );
                    },
                }
            }
        }
    }
};

pub const Shard = struct {
    allocator: *Allocator,
    graph: *const Graph,
    node_states: []NodeState,
    // Frontier for node *output*
    // Invariant: all changes emitted from a node are greater than or equal to its frontier: node_frontiers[node.id].frontier.causalOrder(change.timestamp) != .lt
    node_frontiers: []SupportedFrontier,
    unprocessed_change_batches: ArrayList(ChangeBatchAtNodeInput),
    // Updates waiting to be applied to node *input*
    unprocessed_frontier_updates: DeepHashMap(Pointstamp, isize),

    const ChangeBatchAtNodeInput = struct {
        change_batch: ChangeBatch,
        node_input: NodeInput,
    };

    const Pointstamp = struct {
        node_input: NodeInput,
        subgraphs: []const Subgraph,
        timestamp: Timestamp,
    };

    pub fn init(allocator: *Allocator, graph: *const Graph) !Shard {
        const num_nodes = graph.node_specs.len;

        var node_states = try allocator.alloc(NodeState, num_nodes);
        for (node_states) |*node_state, node_id|
            node_state.* = NodeState.init(allocator, graph.node_specs[node_id]);

        var node_frontiers = try allocator.alloc(SupportedFrontier, num_nodes);
        for (node_frontiers) |*node_frontier, node_id|
            node_frontier.* = try SupportedFrontier.initEmpty(allocator);

        var unprocessed_frontier_updates = DeepHashMap(Pointstamp, isize).init(allocator);

        var self = Shard{
            .allocator = allocator,
            .graph = graph,
            .node_states = node_states,
            .node_frontiers = node_frontiers,
            .unprocessed_change_batches = ArrayList(ChangeBatchAtNodeInput).init(allocator),
            .unprocessed_frontier_updates = unprocessed_frontier_updates,
        };

        // Init input frontiers
        for (graph.node_specs) |node_spec, node_id| {
            if (node_spec == .Input) {
                const timestamp = try Timestamp.initLeast(allocator, graph.node_subgraphs[node_id].len);
                try self.node_states[node_id].Input.frontier.timestamps.put(timestamp, {});
                _ = try self.applyFrontierUpdate(.{ .id = node_id }, timestamp, 1);
            }
        }
        while (self.hasWork()) try self.doWork();

        return self;
    }

    pub fn pushInput(self: *Shard, node: Node, change: Change) !void {
        assert(
            self.node_states[node.id].Input.frontier.causalOrder(change.timestamp) != .gt,
            "May not push inputs that are less than the Input node frontier set by Shard.advanceInput",
            .{},
        );
        try self.node_states[node.id].Input.unflushed_changes.changes.append(change);
    }

    pub fn flushInput(self: *Shard, node: Node) !void {
        var unflushed_changes = &self.node_states[node.id].Input.unflushed_changes;
        if (try unflushed_changes.finishAndClear()) |change_batch| {
            try self.emitChangeBatch(node, change_batch);
        }
    }

    pub fn advanceInput(self: *Shard, node: Node, timestamp: Timestamp) !void {
        // Have to flush input so that there aren't any pending changes with timestamps less than the new frontier
        try self.flushInput(node);

        var changes = ArrayList(FrontierChange).init(self.allocator);
        try self.node_states[node.id].Input.frontier.move(.Advance, timestamp, &changes);
        for (changes.items) |change| {
            _ = try self.applyFrontierUpdate(node, change.timestamp, change.diff);
        }
    }

    fn emitChangeBatch(self: *Shard, from_node: Node, change_batch: ChangeBatch) !void {
        // Check this is legal
        {
            const output_frontier = self.node_frontiers[from_node.id];
            var iter = change_batch.lower_bound.timestamps.iterator();
            while (iter.next()) |entry| {
                assert(
                    output_frontier.frontier.causalOrder(entry.key_ptr.*) != .gt,
                    "Emitted a change at a timestamp that is behind the output frontier. Node {}, timestamp {}.",
                    .{ from_node, entry.key_ptr.* },
                );
            }
        }

        for (self.graph.downstream_node_inputs[from_node.id]) |to_node_input| {
            try self.unprocessed_change_batches.append(.{
                .change_batch = change_batch,
                .node_input = to_node_input,
            });
            var iter = change_batch.lower_bound.timestamps.iterator();
            while (iter.next()) |entry| {
                try self.queueFrontierUpdate(to_node_input, entry.key_ptr.*, 1);
            }
        }
    }

    fn popChangeBatch(self: *Shard) !?ChangeBatchAtNodeInput {
        if (self.unprocessed_change_batches.popOrNull()) |change_batch_at_node_input| {
            var iter = change_batch_at_node_input.change_batch.lower_bound.timestamps.iterator();
            while (iter.next()) |entry| {
                try self.queueFrontierUpdate(change_batch_at_node_input.node_input, entry.key_ptr.*, -1);
            }
            return change_batch_at_node_input;
        } else {
            return null;
        }
    }

    fn processChangeBatch(self: *Shard, change_batch: ChangeBatch, node_input: NodeInput) !void {
        const node = node_input.node;
        const node_spec = self.graph.node_specs[node.id];
        const node_state = &self.node_states[node.id];
        switch (node_spec) {
            .Input => panic("Input nodes should not have work pending on their input", .{}),
            .Map => |map| {
                var output_change_batch_builder = ChangeBatchBuilder.init(self.allocator);
                for (change_batch.changes) |change| {
                    var output_change = change; // copy
                    output_change.row = try map.mapper.map_fn(map.mapper, change.row);
                    try output_change_batch_builder.changes.append(output_change);
                }
                if (try output_change_batch_builder.finishAndClear()) |output_change_batch| {
                    try self.emitChangeBatch(node_input.node, output_change_batch);
                }
            },
            .Index => {
                // These won't be emitted until the frontier passes them
                // TODO this is a lot of timestamps - is there a cheaper way to maintain the support for the index frontier?
                for (change_batch.changes) |change| {
                    assert(
                        self.node_frontiers[node.id].frontier.causalOrder(change.timestamp) != .gt,
                        "Index received a change that was behind its output frontier. Node {}, timestamp {}.",
                        .{ node, change.timestamp },
                    );
                    _ = try self.applyFrontierUpdate(node, change.timestamp, 1);
                }
                try node_state.Index.pending_changes.appendSlice(change_batch.changes);
            },
            .Join => |join| {
                const index = self.node_states[join.inputs[1 - node_input.input_ix].id].getIndex().?;
                var output_change_batch_builder = ChangeBatchBuilder.init(self.allocator);
                for (change_batch.changes) |change| {
                    const this_key = change.row.values[0..join.key_columns];
                    for (index.change_batches.items) |other_change_batch| {
                        for (other_change_batch.changes) |other_change| {
                            const other_key = other_change.row.values[0..join.key_columns];
                            if (dida.meta.deepEqual(this_key, other_key)) {
                                const values = switch (node_input.input_ix) {
                                    0 => &[2][]const Value{ change.row.values, other_change.row.values },
                                    1 => &[2][]const Value{ other_change.row.values, change.row.values },
                                    else => panic("Bad input_ix for join: {}", .{node_input.input_ix}),
                                };
                                const output_change = Change{
                                    .row = .{ .values = try std.mem.concat(self.allocator, Value, values) },
                                    .diff = change.diff * other_change.diff,
                                    .timestamp = try Timestamp.leastUpperBound(self.allocator, change.timestamp, other_change.timestamp),
                                };
                                try output_change_batch_builder.changes.append(output_change);
                            }
                        }
                    }
                }
                if (try output_change_batch_builder.finishAndClear()) |output_change_batch| {
                    try self.emitChangeBatch(node_input.node, output_change_batch);
                }
            },
            .Output => {
                try node_state.Output.unpopped_change_batches.append(change_batch);
            },
            .TimestampPush => {
                var output_change_batch_builder = ChangeBatchBuilder.init(self.allocator);
                for (change_batch.changes) |change| {
                    var output_change = change; // copy
                    output_change.timestamp = try change.timestamp.pushCoord(self.allocator);
                    try output_change_batch_builder.changes.append(output_change);
                }
                try self.emitChangeBatch(node_input.node, (try output_change_batch_builder.finishAndClear()).?);
            },
            .TimestampIncrement => {
                var output_change_batch_builder = ChangeBatchBuilder.init(self.allocator);
                for (change_batch.changes) |change| {
                    var output_change = change; // copy
                    output_change.timestamp = try change.timestamp.incrementCoord(self.allocator);
                    try output_change_batch_builder.changes.append(output_change);
                }
                try self.emitChangeBatch(node_input.node, (try output_change_batch_builder.finishAndClear()).?);
            },
            .TimestampPop => {
                var output_change_batch_builder = ChangeBatchBuilder.init(self.allocator);
                for (change_batch.changes) |change| {
                    var output_change = change; // copy
                    output_change.timestamp = try change.timestamp.popCoord(self.allocator);
                    try output_change_batch_builder.changes.append(output_change);
                }
                if (try output_change_batch_builder.finishAndClear()) |output_change_batch| {
                    try self.emitChangeBatch(node_input.node, output_change_batch);
                }
            },
            .Union => {
                // Pass straight through
                try self.emitChangeBatch(node_input.node, change_batch);
            },
            .Distinct => |distinct| {
                // Figure out which new timestamps are pending
                // TODO is there a faster way to do this?
                for (change_batch.changes) |change| {
                    // change.timestamp is pending
                    {
                        const old_entry = try node_state.Distinct.pending_timestamps.fetchPut(change.timestamp, {});
                        if (old_entry == null) {
                            _ = try self.applyFrontierUpdate(node, change.timestamp, 1);
                        }
                    }

                    // for any other_timestamp in pending_timestamps, leastUpperBound(change.timestamp, other_timestamp) is pending
                    var buffer = ArrayList(Timestamp).init(self.allocator);
                    var iter = node_state.Distinct.pending_timestamps.iterator();
                    while (iter.next()) |entry| {
                        const timestamp = try Timestamp.leastUpperBound(
                            self.allocator,
                            change.timestamp,
                            entry.key_ptr.*,
                        );
                        try buffer.append(timestamp);
                    }
                    for (buffer.items) |timestamp| {
                        const old_entry = try node_state.Distinct.pending_timestamps.fetchPut(timestamp, {});
                        if (old_entry == null) {
                            _ = try self.applyFrontierUpdate(node, timestamp, 1);
                        }
                    }
                }
            },
        }
    }

    fn queueFrontierUpdate(self: *Shard, node_input: NodeInput, timestamp: Timestamp, diff: isize) !void {
        var entry = try self.unprocessed_frontier_updates.getOrPutValue(.{
            .node_input = node_input,
            .subgraphs = self.graph.node_subgraphs[node_input.node.id],
            .timestamp = timestamp,
        }, 0);
        entry.value_ptr.* += diff;
        _ = if (entry.value_ptr.* == 0) self.unprocessed_frontier_updates.remove(entry.key_ptr.*);
    }

    fn applyFrontierUpdate(self: *Shard, node: Node, timestamp: Timestamp, diff: isize) !enum { Updated, NotUpdated } {
        var frontier_changes = ArrayList(FrontierChange).init(self.allocator);
        try self.node_frontiers[node.id].update(timestamp, diff, &frontier_changes);
        for (frontier_changes.items) |frontier_change| {
            for (self.graph.downstream_node_inputs[node.id]) |downstream_node_input| {
                try self.queueFrontierUpdate(downstream_node_input, frontier_change.timestamp, frontier_change.diff);
            }
        }
        return if (frontier_changes.items.len > 0) .Updated else .NotUpdated;
    }

    // Produces an ordering on Pointstamp that is compatible with causality.
    // IE if the existence of a change at `this` causes a change to later be produced at `that`, then we need to have `orderPointstamps(this, that) == .lt`.
    // The invariants enforced for the graph structure guarantee that this is possible.
    fn orderPointstamps(this: Pointstamp, that: Pointstamp) std.math.Order {
        const min_len = min(this.subgraphs.len, that.subgraphs.len);
        var i: usize = 0;
        while (i < min_len) : (i += 1) {
            // If `this` and `that` are in different subgraphs then there is no way for a change to travel from a later node to an earlier node without incrementing the timestamp coord at `i-1`.
            if (this.subgraphs[i].id != that.subgraphs[i].id)
                return dida.meta.deepOrder(this.node_input, that.node_input);

            // If `this` and `that` are in the same subgraph but one has a higher timestamp coord at `i` than the other then there is no way the higher timestamp could be decremented to produce the lower timestamp.
            const timestamp_order = std.math.order(this.timestamp.coords[i], that.timestamp.coords[i]);
            if (timestamp_order != .eq) return timestamp_order;
        }
        // If we get this far, either `this` and `that` are in the same subgraph or one is in a subgraph that is nested inside the other.
        // Either way there is no way for a change to travel from a later node to an earlier node without incrementing the timestamp coord at `min_len-1`.
        return dida.meta.deepOrder(this.node_input, that.node_input);
    }

    fn processFrontierUpdates(self: *Shard) !void {
        // Nodes whose input frontiers have changed
        // TODO is it worth tracking the actual change? might catch cases where the total diff is zero
        var updated_nodes = DeepHashSet(Node).init(self.allocator);

        // Propagate frontier updates
        // NOTE We have to propagate all of these before doing anything else - the intermediate states can be invalid
        while (self.unprocessed_frontier_updates.count() > 0) {

            // Find min pointstamp
            // TODO use a sorted data structure for unprocessed_frontier_updates
            var iter = self.unprocessed_frontier_updates.iterator();
            var min_entry = iter.next().?;
            while (iter.next()) |entry| {
                if (orderPointstamps(entry.key_ptr.*, min_entry.key_ptr.*) == .lt)
                    min_entry = entry;
            }
            const node = min_entry.key_ptr.node_input.node;
            const input_timestamp = min_entry.key_ptr.timestamp;
            const diff = min_entry.value_ptr.*;
            _ = self.unprocessed_frontier_updates.remove(min_entry.key_ptr.*);

            // An input frontier for this node, so we may need to take some action on it later
            try updated_nodes.put(node, {});

            // Work out how node changes the timestamp
            const output_timestamp = switch (self.graph.node_specs[node.id]) {
                .TimestampPush => try input_timestamp.pushCoord(self.allocator),
                .TimestampIncrement => try input_timestamp.incrementCoord(self.allocator),
                .TimestampPop => try input_timestamp.popCoord(self.allocator),
                else => input_timestamp,
            };

            // Apply change to frontier
            const updated = try self.applyFrontierUpdate(node, output_timestamp, diff);
        }

        // Trigger special actions at nodes whose frontier has changed
        // NOTE It's ok to buffer updates like this rather than going in pointstamp order, because these changes represent real capabilities, not just implied capabilities
        //      In fact, if we mixed this in with the processing frontier diffs then they might see an invalid intermediate state for the frontier.
        // TODO Probably should pop these one at a time though to avoid doWork being unbounded
        var updated_nodes_iter = updated_nodes.iterator();
        while (updated_nodes_iter.next()) |updated_nodes_entry| {
            const node = updated_nodes_entry.key_ptr.*;
            const node_spec = self.graph.node_specs[node.id];
            const node_state = &self.node_states[node.id];

            // Index-specific stuff
            if (node_spec == .Index) {
                // Might be able to produce an output batch now that the frontier has advanced
                var change_batch_builder = ChangeBatchBuilder.init(self.allocator);
                var pending_changes = ArrayList(Change).init(self.allocator);
                const input_frontier = self.node_frontiers[node_spec.Index.input.id];
                for (node_state.Index.pending_changes.items) |change| {
                    if (input_frontier.frontier.causalOrder(change.timestamp) == .gt) {
                        try change_batch_builder.changes.append(change);
                    } else {
                        try pending_changes.append(change);
                    }
                }
                node_state.Index.pending_changes = pending_changes;
                if (try change_batch_builder.finishAndClear()) |change_batch| {
                    try node_state.Index.index.change_batches.append(change_batch);
                    try self.emitChangeBatch(node, change_batch);
                    for (change_batch.changes) |change| {
                        _ = try self.applyFrontierUpdate(node, change.timestamp, -1);
                    }
                }
            }

            // Distinct-specific stuff
            // TODO this is very inefficient
            if (node_spec == .Distinct) {
                const input_frontier = self.node_frontiers[node_spec.Distinct.input.id];

                // Going to emit a result for any timestamp that is before the new input frontier
                var timestamps_to_emit = ArrayList(Timestamp).init(self.allocator);
                {
                    var iter = node_state.Distinct.pending_timestamps.iterator();
                    while (iter.next()) |entry| {
                        if (input_frontier.frontier.causalOrder(entry.key_ptr.*) == .gt)
                            try timestamps_to_emit.append(entry.key_ptr.*);
                    }
                }

                // Sort timestamps
                std.sort.sort(Timestamp, timestamps_to_emit.items, {}, struct {
                    fn lessThan(_: void, a: Timestamp, b: Timestamp) bool {
                        return a.lexicalOrder(b) == .lt;
                    }
                }.lessThan);

                // Compute changes at each timestamp.
                // Lexical order is a complete extension of causal order so we can be sure that for each timestamp all previous timestamps have already been handled.
                // TODO think hard about what should happen if input counts are negative
                var change_batch_builder = ChangeBatchBuilder.init(self.allocator);
                const input_index = self.node_states[node_spec.Distinct.input.id].getIndex().?;
                const output_index = self.node_states[node.id].getIndex().?;
                for (timestamps_to_emit.items) |timestamp| {
                    const old_bag = try output_index.getBagAsOf(self.allocator, timestamp);
                    var new_bag = try input_index.getBagAsOf(self.allocator, timestamp);
                    // Count things that are in new_bag
                    {
                        var iter = new_bag.row_counts.iterator();
                        while (iter.next()) |new_entry| {
                            const diff = 1 - (old_bag.row_counts.get(new_entry.key_ptr.*) orelse 0);
                            if (diff != 0)
                                try change_batch_builder.changes.append(.{
                                    .row = new_entry.key_ptr.*,
                                    .diff = diff,
                                    .timestamp = timestamp,
                                });
                        }
                    }
                    // Count things that are in old_bag and not in new_bag
                    {
                        var iter = old_bag.row_counts.iterator();
                        while (iter.next()) |old_entry| {
                            if (!new_bag.row_counts.contains(old_entry.key_ptr.*)) {
                                const diff = -old_entry.value_ptr.*;
                                try change_batch_builder.changes.append(.{
                                    .row = old_entry.key_ptr.*,
                                    .diff = diff,
                                    .timestamp = timestamp,
                                });
                            }
                        }
                    }
                }

                // Emit changes
                if (try change_batch_builder.finishAndClear()) |change_batch| {
                    try output_index.change_batches.append(change_batch);
                    try self.emitChangeBatch(node, change_batch);
                }

                // Remove emitted timestamps from pending timestamps and from support
                for (timestamps_to_emit.items) |timestamp| {
                    _ = node_state.Distinct.pending_timestamps.remove(timestamp);
                    _ = try self.applyFrontierUpdate(node, timestamp, -1);
                }
            }
        }
    }

    pub fn hasWork(self: *const Shard) bool {
        return (self.unprocessed_change_batches.items.len > 0) or
            (self.unprocessed_frontier_updates.count() > 0);
    }

    pub fn doWork(self: *Shard) !void {
        if (try self.popChangeBatch()) |change_batch_at_node_input| {
            try self.processChangeBatch(change_batch_at_node_input.change_batch, change_batch_at_node_input.node_input);
        } else if (self.unprocessed_frontier_updates.count() > 0) {
            try self.processFrontierUpdates();
        }
    }

    pub fn popOutput(self: *Shard, node: Node) ?ChangeBatch {
        return self.node_states[node.id].Output.unpopped_change_batches.popOrNull();
    }
};
