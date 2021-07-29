//! Tools for debugging dida. 

usingnamespace @import("./common.zig");

/// Things that dida does internally
pub const DebugEvent = union(enum) {
    PushInput: struct {
        node: dida.core.Node,
        change: dida.core.Change,
    },
    FlushInput: struct {
        node: dida.core.Node,
    },
    AdvanceInput: struct {
        node: dida.core.Node,
        timestamp: dida.core.Timestamp,
    },
    EmitChangeBatch: struct {
        from_node: dida.core.Node,
        change_batch: dida.core.ChangeBatch,
    },
    ProcessChangeBatch: struct {
        node_input: dida.core.NodeInput,
        change_batch: dida.core.ChangeBatch,
    },
    QueueFrontierUpdate: struct {
        node_input: dida.core.NodeInput,
        timestamp: dida.core.Timestamp,
        diff: isize,
    },
    ApplyFrontierUpdate: struct {
        node: dida.core.Node,
        timestamp: dida.core.Timestamp,
        diff: isize,
    },
    ProcessFrontierUpdates,
    ProcessFrontierUpdate: struct {
        node: dida.core.Node,
        input_timestamp: dida.core.Timestamp,
        diff: isize,
    },
    ProcessFrontierUpdateReaction: struct {
        node: dida.core.Node,
    },
    PopOutput: struct {
        node: dida.core.Node,
        change_batch: ?dida.core.ChangeBatch,
    },
    DoWork,
};

pub const DebugEventHandler = struct {
    handle: fn (self: *DebugEventHandler, shard: *const dida.core.Shard, event: DebugEvent) void,
};

pub const Dumper = struct {
    debug_event_handler: DebugEventHandler = .{ .handle = handle },

    pub fn handle(self: *DebugEventHandler, shard: *const dida.core.Shard, event: DebugEvent) void {
        //dida.common.dump(shard);
        dida.common.dump(event);
    }
};

pub var dumper = Dumper{};
