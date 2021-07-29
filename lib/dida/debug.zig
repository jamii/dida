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

    pub fn handle(debug_event_handler: *DebugEventHandler, shard: *const dida.core.Shard, event: DebugEvent) void {
        //dida.common.dump(shard);
        dida.common.dump(event);
    }
};

pub var dumper = Dumper{};

pub const Logger = struct {
    file: std.fs.File,
    debug_event_handler: DebugEventHandler = .{ .handle = handle },

    pub fn initTempFile() Logger {
        const tmp_dir = std.testing.tmpDir(.{});
        const file = tmp_dir.dir.createFile("debug_events.json", .{}) catch |err|
            panic("Failed to open debug_events.json: {}", .{err});
        std.debug.print("Logging debug events to zig-cache/tmp/{s}/debug_events.json\n", .{tmp_dir.sub_path});
        return Logger{ .file = file };
    }

    pub fn handle(debug_event_handler: *DebugEventHandler, shard: *const dida.core.Shard, event: DebugEvent) void {
        const self = @fieldParentPtr(Logger, "debug_event_handler", debug_event_handler);
        const writer = self.file.writer();
        // TODO would prefer json for loading into debugger
        dida.meta.dumpInto(writer, 0, event) catch |err| panic("Failed to write to debug_events.json: {}", .{err});
        writer.writeAll("\n") catch |err| panic("Failed to write to debug_events.json: {}", .{err});
        std.os.fsync(self.file.handle) catch |err| panic("Error in debug.Logger: {}", .{err});
    }
};
