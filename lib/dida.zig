usingnamespace @import("dida/common.zig");

pub const meta = @import("dida/meta.zig");

const Node = struct {
    id: u64,
};

const Port = union(enum) {
    Input: u64,
    Output: u64,
};

const Location = struct {
    node: Node,
    port: Port,
};

const Timestamp = struct {
    coords: []const u64,
};

const Pointstamp = struct {
    location: Location,
    timestamp: Timestamp,
};

const Datum = struct {
    data: []const u8,
};

const Change = struct {
    datum: Datum,
    diff: isize,
    timestamp: Timestamp,
};

const Trace = struct {
    changes: ArrayList(Change),
};

const NodeKind = enum {
    Input,
    Map: Map,
    Join: Join,
};

const NodeData = struct {
    kind: NodeKind,
};

const Edge = struct {
    in: Node,
    out: Node,
};

const GraphBuilder = struct {
    allocator: *Allocator,
    nodes: ArrayList(NodeData),
};

const Graph = struct {
    allocator: *Allocator,
    nodes: []NodeData,
    in_edges: [][]Node,
    out_edges: [][]Node,
};

const Pointstamps = HashMap(Pointstamp, u64);
const PointstampChanges = HashMap(Pointstamp, i64);

const WorkerState = struct {
    graph: Graph,
    local_pointstamps: ArrayList(Pointstamp),
    approx_global_pointstamps: ArrayList(PointStamp), // maybe contains dupes
};
