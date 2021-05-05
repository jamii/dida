usingnamespace @import("dida/common.zig");

pub const meta = @import("dida/meta.zig");

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

pub const Timestamp = struct {
    coords: []const u64,
};

pub const Pointstamp = struct {
    location: Location,
    timestamp: Timestamp,
};

pub const Datum = []const u8;

pub const Change = struct {
    datum: Datum,
    diff: isize,
    timestamp: Timestamp,
};

pub const Trace = struct {
    changes: ArrayList(Change),
};

pub const NodeData = union(enum) {
    Input,
    Map: Map,
    Join: Join,

    pub const Map = struct {
        input: Node,
        function: fn (datum: Datum) error{OutOfMemory}!Datum,
    };

    pub const Join = struct {
        left_input: Node,
        right_input: Node,
        // TODO arrangements?
        left_key_function: fn (datum: Datum) Datum,
        right_key_function: fn (datum: Datum) Datum,
    };
};

pub const Edge = struct {
    in: Node,
    out: Node,
};

pub const GraphBuilder = struct {
    allocator: *Allocator,
    nodes: ArrayList(NodeData),

    const Self = @This();

    pub fn init(allocator: *Allocator) Self {
        const nodes = ArrayList(NodeData).init(allocator);
        return Self{
            .allocator = allocator,
            .nodes = nodes,
        };
    }

    pub fn deinit(self: Self) void {
        self.nodes.deinit();
    }

    pub fn add_node(self: *Self, node_data: NodeData) error{OutOfMemory}!Node {
        const node = Node{ .id = @intCast(u64, self.nodes.items.len) };
        try self.nodes.append(node_data);
        return node;
    }

    pub fn finish_and_clear(self: *Self) Graph {
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

    const Self = @This();

    pub fn deinit(self: Self) void {
        self.allocator.free(self.nodes);
    }
};

pub const Pointstamps = HashMap(Pointstamp, u64);
pub const PointstampChanges = HashMap(Pointstamp, i64);

pub const WorkerState = struct {
    graph: Graph,
    unprocessed_changes: ArrayList(struct { location: Location, change: Change }),
};
