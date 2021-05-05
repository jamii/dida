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

pub const NodeData = union(enum) {
    Input,
    Map: Map,
    Join: Join,
    Output: Output,

    pub const Map = struct {
        input: OutputLocation,
        function: fn (row: Row) error{OutOfMemory}!Row,
    };

    pub const Join = struct {
        inputs: [2]OutputLocation,
        key_functions: [2]fn (row: Row) Row,
    };

    pub const Output = struct {
        input: OutputLocation,
    };
};

pub const NodeState = union(enum) {
    Input,
    Map,
    Join: [2]Index,
    Output: ArrayList(Change),

    pub fn init(allocator: *Allocator, node_data: NodeData) NodeState {
        switch (node_data) {
            .Input => return .Input,
            .Map => return .Map,
            .Join => return .{ .Join = .{ Index.init(allocator), Index.init(allocator) } },
            .Output => return .{ .Output = ArrayList(Change).init(allocator) },
        }
    }
};

pub const GraphBuilder = struct {
    allocator: *Allocator,
    nodes: ArrayList(NodeData),

    pub fn init(allocator: *Allocator) GraphBuilder {
        const nodes = ArrayList(NodeData).init(allocator);
        return GraphBuilder{
            .allocator = allocator,
            .nodes = nodes,
        };
    }

    pub fn add_node(self: *GraphBuilder, node_data: NodeData) error{OutOfMemory}!Node {
        const node = Node{ .id = @intCast(u64, self.nodes.items.len) };
        try self.nodes.append(node_data);
        return node;
    }

    pub fn finish_and_clear(self: *GraphBuilder) Graph {
        const nodes = self.nodes.toOwnedSlice();
        return Graph{
            .allocator = self.allocator,
            .nodes = nodes,
        };
    }
};

pub const Graph = struct {
    allocator: *Allocator,
    nodes: []const NodeData,
};

pub const Pointstamps = HashMap(Pointstamp, u64);
pub const PointstampChanges = HashMap(Pointstamp, i64);

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
                    const node_data = self.graph.nodes[location.node.id];
                    switch (node_data) {
                        .Input => {
                            // pass straight through to output for now
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
                        .Join => |join| {
                            const output_location = Location{
                                .node = location.node,
                                .port = .{ .Output = 0 },
                            };
                            const this_index = &self.node_states[location.node.id].Join[input_port];
                            const other_index = &self.node_states[location.node.id].Join[1 - input_port];

                            // add to index on this side
                            try this_index.changes.append(change);

                            // lookup in index on other side
                            const this_key = (join.key_functions[input_port])(change.row);
                            const other_key_function = join.key_functions[1 - input_port];
                            for (other_index.changes.items) |other_change| {
                                const other_key = (other_key_function)(other_change.row);
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
                        .Output => |output| {
                            const outputs = &self.node_states[location.node.id].Output;
                            try outputs.append(change);
                        },
                    }
                },
                .Output => |output_port| {
                    // forward to all nodes that have this location as an input
                    for (self.graph.nodes) |node_data, node_id| {
                        const node = Node{ .id = node_id };
                        switch (node_data) {
                            .Input => {},
                            .Map => |map| {
                                if (map.input.node.id == location.node.id and map.input.output_port == output_port) {
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
