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

    pub fn clone(self: DebugEvent, allocator: *Allocator) !DebugEvent {
        var result: DebugEvent = undefined;
        const tag = @enumToInt(std.meta.activeTag(self));
        inline for (@typeInfo(@typeInfo(DebugEvent).Union.tag_type.?).Enum.fields) |eti, i| {
            if (tag == eti.value) {
                const self_payload = @field(self, eti.name);
                const PayloadType = @TypeOf(self_payload);
                var result_payload: PayloadType = undefined;
                if (PayloadType != void) {
                    inline for (@typeInfo(PayloadType).Struct.fields) |sti| {
                        @field(result_payload, sti.name) = switch (sti.field_type) {
                            isize, dida.core.Node, dida.core.NodeInput => @field(self_payload, sti.name),
                            ?dida.core.ChangeBatch => if (@field(self_payload, sti.name)) |change_batch| try change_batch.clone(allocator) else null,
                            else => try @field(self_payload, sti.name).clone(allocator),
                        };
                    }
                }
                @field(result, eti.name) = result_payload;
            }
        }
        return result;
    }
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
