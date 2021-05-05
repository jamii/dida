pub const meta = @import("dida/meta.zig");
pub const common = @import("dida/common.zig");
usingnamespace common;

pub const Node = struct {
    id: u64,
};

pub const Port = union(enum) {
    Input: u64,
    Output: u64,
};

pub const Location = struct {
    node: Node,
    port: Port,
};

pub const InputLocation = struct {
    node: Node,
    input_port: u64,
};

pub const OutputLocation = struct {
    node: Node,
    output_port: u64,
};

pub const Timestamp = struct {
    coords: []const u64,

    pub fn least_upper_bound(allocator: *Allocator, self: Timestamp, other: Timestamp) !Timestamp {
        release_assert(self.coords.len == other.coords.len, "Tried to take least_upper_bound of timestamps with different lengths: {} vs {}", .{ self, other });
        var output_coords = try allocator.alloc(u64, self.coords.len);
        for (self.coords) |self_coord, i| {
            const other_coord = other.coords[i];
            output_coords[i] = max(self_coord, other_coord);
        }
        return Timestamp{ .coords = output_coords };
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
    diff: isize,
    timestamp: Timestamp,
};

pub const Index = struct {
    changes: ArrayList(Change),

    pub fn init(allocator: *Allocator) Index {
        return Index{
            .changes = ArrayList(Change).init(allocator),
        };
    }
};

pub const NodeSpec = union(enum) {
    Input,
    Map: MapSpec,
    Index: IndexSpec,
    Join: JoinSpec,
    Output: OutputSpec,

    pub const MapSpec = struct {
        input: OutputLocation,
        function: fn (row: Row) error{OutOfMemory}!Row,
    };

    pub const IndexSpec = struct {
        input: OutputLocation,
    };

    pub const JoinSpec = struct {
        inputs: [2]OutputLocation,
        key_columns: u64,
    };

    pub const OutputSpec = struct {
        input: OutputLocation,
    };

    pub fn num_input_ports(self: NodeSpec) usize {
        return switch (self) {
            .Input, .Map, .Index, .Output => 1,
            .Join => 2,
        };
    }

    pub fn num_output_ports(self: NodeSpec) usize {
        return switch (self) {
            .Input, .Map, .Index, .Output, .Join => 1,
        };
    }
};

pub const NodeState = union(enum) {
    Input,
    Map,
    Index: Index,
    Join,
    Output: ArrayList(Change),

    pub fn init(allocator: *Allocator, node_spec: NodeSpec) NodeState {
        switch (node_spec) {
            .Input => return .Input,
            .Map => return .Map,
            .Index => return .{ .Index = Index.init(allocator) },
            .Join => return .Join,
            .Output => return .{ .Output = ArrayList(Change).init(allocator) },
        }
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

    pub fn add_node(self: *GraphBuilder, node_spec: NodeSpec) !Node {
        // TODO check all edges are valid
        switch (node_spec) {
            .Join => |join| {
                for (join.inputs) |input_location| {
                    release_assert(self.node_specs.items[input_location.node.id] == .Index, "Inputs to Join node must be Index nodes", .{});
                }
            },
            .Input, .Map, .Index, .Output => {},
        }
        const node = Node{ .id = @intCast(u64, self.node_specs.items.len) };
        try self.node_specs.append(node_spec);
        return node;
    }

    pub fn finish_and_clear(self: *GraphBuilder) Graph {
        return Graph{
            .allocator = self.allocator,
            .nodes = self.node_specs.toOwnedSlice(),
        };
    }
};

pub const Graph = struct {
    allocator: *Allocator,
    nodes: []const NodeSpec,
};

pub const Worker = struct {
    allocator: *Allocator,
    graph: Graph,
    node_states: []NodeState,
    unprocessed_changes: ArrayList(ChangeAtLocation),

    const ChangeAtLocation = struct {
        change: Change,
        location: Location,
    };

    pub fn init(allocator: *Allocator, graph: Graph) !Worker {
        var node_states = try allocator.alloc(NodeState, graph.nodes.len);
        for (node_states) |*node_state, i| {
            node_state.* = NodeState.init(allocator, graph.nodes[i]);
        }
        return Worker{
            .allocator = allocator,
            .graph = graph,
            .node_states = node_states,
            .unprocessed_changes = ArrayList(ChangeAtLocation).init(allocator),
        };
    }

    pub fn push_input(self: *Worker, node: Node, change: Change) !void {
        try self.unprocessed_changes.append(.{
            .change = change,
            .location = .{ .node = node, .port = .{ .Input = 0 } },
        });
    }

    pub fn has_work(self: Worker) bool {
        return self.unprocessed_changes.items.len > 0;
    }

    pub fn do_work(self: *Worker) !void {
        if (self.unprocessed_changes.popOrNull()) |change_at_location| {
            const change = change_at_location.change;
            const location = change_at_location.location;
            switch (location.port) {
                .Input => |input_port| {
                    const node_spec = self.graph.nodes[location.node.id];
                    switch (node_spec) {
                        .Input => {
                            // pass straight through to output port
                            try self.unprocessed_changes.append(.{
                                .change = change,
                                .location = .{ .node = location.node, .port = .{ .Output = 0 } },
                            });
                        },
                        .Map => |map| {
                            var output_change = change;
                            output_change.row = try map.function(change.row);
                            try self.unprocessed_changes.append(.{
                                .change = output_change,
                                .location = .{ .node = location.node, .port = .{ .Output = 0 } },
                            });
                        },
                        .Index => {
                            const index = &self.node_states[location.node.id].Index;
                            try index.changes.append(change);
                            try self.unprocessed_changes.append(.{
                                .change = change,
                                .location = .{ .node = location.node, .port = .{ .Output = 0 } },
                            });
                        },
                        .Join => |join| {
                            const index = &self.node_states[join.inputs[1 - input_port].node.id].Index;
                            const this_key = change.row.values[0..join.key_columns];
                            for (index.changes.items) |other_change| {
                                const other_key = other_change.row.values[0..join.key_columns];
                                if (meta.deepEqual(this_key, other_key)) {
                                    const output_change = Change{
                                        .row = .{ .values = try std.mem.concat(self.allocator, Value, &[2][]const Value{ change.row.values, other_change.row.values }) },
                                        .diff = change.diff * other_change.diff,
                                        .timestamp = try Timestamp.least_upper_bound(self.allocator, change.timestamp, other_change.timestamp),
                                    };
                                    try self.unprocessed_changes.append(.{
                                        .change = output_change,
                                        .location = .{ .node = location.node, .port = .{ .Output = 0 } },
                                    });
                                }
                            }
                        },
                        .Output => {
                            const outputs = &self.node_states[location.node.id].Output;
                            try outputs.append(change);
                        },
                    }
                },
                .Output => |output_port| {
                    // forward to all nodes that have this location as an input
                    for (self.graph.nodes) |node_spec, node_id| {
                        const node = Node{ .id = node_id };
                        switch (node_spec) {
                            .Input => {},
                            .Map => |map| {
                                if (map.input.node.id == location.node.id and map.input.output_port == output_port) {
                                    try self.unprocessed_changes.append(.{
                                        .change = change,
                                        .location = .{ .node = node, .port = .{ .Input = 0 } },
                                    });
                                }
                            },
                            .Index => |index| {
                                if (index.input.node.id == location.node.id and index.input.output_port == output_port) {
                                    try self.unprocessed_changes.append(.{
                                        .change = change,
                                        .location = .{ .node = node, .port = .{ .Input = 0 } },
                                    });
                                }
                            },
                            .Join => |join| {
                                for (join.inputs) |join_input, join_port| {
                                    if (join_input.node.id == location.node.id and join_input.output_port == output_port) {
                                        try self.unprocessed_changes.append(.{
                                            .change = change,
                                            .location = .{ .node = node, .port = .{ .Input = join_port } },
                                        });
                                    }
                                }
                            },
                            .Output => |output| {
                                if (output.input.node.id == location.node.id and output.input.output_port == output_port) {
                                    try self.unprocessed_changes.append(.{
                                        .change = change,
                                        .location = .{ .node = node, .port = .{ .Input = 0 } },
                                    });
                                }
                            },
                        }
                    }
                },
            }
        }
    }

    pub fn pop_output(self: *Worker, node: Node) ?Change {
        return self.node_states[node.id].Output.popOrNull();
    }
};
