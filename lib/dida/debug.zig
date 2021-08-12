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

pub fn emitDebugEvent(shard: *const dida.core.Shard, debug_event: DebugEvent) void {
    const root = @import("root");
    if (@import("builtin").is_test) {
        // Uncomment this for debugging tests
        // dumpDebugEvent(shard, debug_event);
    } else if (@hasDecl(root, "emitDebugEvent"))
        // You can add a handler to your root file eg
        // pub const emitDebugEvent = dida.debug.dumpDebugEvent;
        root.emitDebugEvent(shard, debug_event);
}

pub fn dumpDebugEvent(shard: *const dida.core.Shard, debug_event: DebugEvent) void {
    dida.common.dump(shard);
    dida.common.dump(debug_event);
}
