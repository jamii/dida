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

pub fn dumpInto(writer: anytype, indent: u32, thing: anytype) anyerror!void {
    const T = @TypeOf(thing);
    if (comptime std.mem.startsWith(u8, @typeName(T), "Allocator")) {
        try writer.writeAll("Allocator{}");
    } else if (comptime std.mem.startsWith(u8, @typeName(T), "std.array_list.ArrayList")) {
        try dumpInto(writer, indent, thing.items);
    } else if (comptime std.mem.startsWith(u8, @typeName(T), "std.hash_map.HashMap")) {
        var iter = thing.iterator();
        const is_set = @TypeOf(iter.next().?.value_ptr.*) == void;
        try writer.writeAll(if (is_set) "HashSet(\n" else "HashMap(\n");
        while (iter.next()) |entry| {
            try writer.writeByteNTimes(' ', indent + 4);
            try dumpInto(writer, indent + 4, entry.key_ptr.*);
            if (!is_set) {
                try writer.writeAll(" => ");
                try dumpInto(writer, indent + 4, entry.value_ptr.*);
            }
            try writer.writeAll(",\n");
        }
        try writer.writeByteNTimes(' ', indent);
        try writer.writeAll(")");
    } else {
        switch (T) {
            dida.core.Value => {
                switch (thing) {
                    .Number => |number| try dida.meta.dumpInto(writer, indent + 4, number),
                    .String => |string| try dida.meta.dumpInto(writer, indent + 4, string),
                }
            },
            dida.core.Row => {
                try writer.writeAll("Row[");
                for (thing.values) |value, i| {
                    try std.fmt.format(writer, "{}", .{value});
                    if (i != thing.values.len - 1)
                        try writer.writeAll(", ");
                }
                try writer.writeAll("]");
            },
            dida.core.Timestamp => {
                try writer.writeAll("T[");
                for (thing.coords) |coord, i| {
                    try std.fmt.format(writer, "{}", .{coord});
                    if (i != thing.coords.len - 1)
                        try writer.writeAll(", ");
                }
                try writer.writeAll("]");
            },
            dida.core.Frontier => {
                try dumpInto(writer, indent, thing.timestamps);
            },
            dida.core.NodeState.DistinctState => {
                try writer.writeAll("DistinctState{\n");

                try writer.writeByteNTimes(' ', indent + 4);
                try writer.writeAll("index: ");
                try dumpInto(writer, indent + 4, thing.index);
                try writer.writeAll(",\n");

                try writer.writeByteNTimes(' ', indent + 4);
                try writer.writeAll("pending_corrections: ");
                try dumpInto(writer, indent + 4, thing.pending_corrections);

                try writer.writeAll("\n");
                try writer.writeByteNTimes(' ', indent);
                try writer.writeAll("}");
            },
            dida.core.NodeState.ReduceState => {
                try writer.writeAll("ReduceState{\n");

                try writer.writeByteNTimes(' ', indent + 4);
                try writer.writeAll("index: ");
                try dumpInto(writer, indent + 4, thing.index);
                try writer.writeAll(",\n");

                try writer.writeByteNTimes(' ', indent + 4);
                try writer.writeAll("pending_corrections: ");
                try dumpInto(writer, indent + 4, thing.pending_corrections);

                try writer.writeAll("\n");
                try writer.writeByteNTimes(' ', indent);
                try writer.writeAll("}");
            },
            dida.core.Shard => {
                try writer.writeAll("Shard{\n");

                for (thing.graph.node_specs) |node_spec, node_id| {
                    try writer.writeByteNTimes(' ', indent + 4);
                    try std.fmt.format(writer, "{}: {{\n", .{node_id});

                    try writer.writeByteNTimes(' ', indent + 8);
                    try writer.writeAll("spec: ");
                    try dumpInto(writer, indent + 8, node_spec);
                    try writer.writeAll(",\n");

                    try writer.writeByteNTimes(' ', indent + 8);
                    try writer.writeAll("state: ");
                    try dumpInto(writer, indent + 8, thing.node_states[node_id]);
                    try writer.writeAll(",\n");

                    try writer.writeByteNTimes(' ', indent + 8);
                    try writer.writeAll("frontier: ");
                    try dumpInto(writer, indent + 8, thing.node_frontiers[node_id]);
                    try writer.writeAll(",\n");

                    try writer.writeByteNTimes(' ', indent + 8);
                    try writer.writeAll("unprocessed_change_batches: [\n");
                    {
                        for (thing.unprocessed_change_batches.items) |change_batch_at_node_input| {
                            if (change_batch_at_node_input.node_input.node.id == node_id) {
                                try writer.writeByteNTimes(' ', indent + 12);
                                try dumpInto(writer, indent + 12, change_batch_at_node_input.change_batch);
                                try writer.writeAll(",\n");
                            }
                        }
                    }
                    try writer.writeByteNTimes(' ', indent + 8);
                    try writer.writeAll("],\n");

                    try writer.writeByteNTimes(' ', indent + 4);
                    try writer.writeAll("},\n");
                }

                try writer.writeByteNTimes(' ', indent);
                try writer.writeAll("}\n");
            },
            else => {
                switch (@typeInfo(T)) {
                    .Pointer => |pti| {
                        switch (pti.size) {
                            .One => {
                                try writer.writeAll("&");
                                try dumpInto(writer, indent, thing.*);
                            },
                            .Many => {
                                // bail
                                try std.fmt.format(writer, "{}", .{thing});
                            },
                            .Slice => {
                                if (pti.child == u8) {
                                    try std.fmt.format(writer, "\"{s}\"", .{thing});
                                } else {
                                    try std.fmt.format(writer, "[]{s}[\n", .{pti.child});
                                    for (thing) |elem| {
                                        try writer.writeByteNTimes(' ', indent + 4);
                                        try dumpInto(writer, indent + 4, elem);
                                        try writer.writeAll(",\n");
                                    }
                                    try writer.writeByteNTimes(' ', indent);
                                    try writer.writeAll("]");
                                }
                            },
                            .C => {
                                // bail
                                try std.fmt.format(writer, "{}", .{thing});
                            },
                        }
                    },
                    .Array => |ati| {
                        if (ati.child == u8) {
                            try std.fmt.format(writer, "\"{s}\"", .{thing});
                        } else {
                            try std.fmt.format(writer, "[{}]{s}[\n", .{ ati.len, ati.child });
                            for (thing) |elem| {
                                try writer.writeByteNTimes(' ', indent + 4);
                                try dumpInto(writer, indent + 4, elem);
                                try writer.writeAll(",\n");
                            }
                            try writer.writeByteNTimes(' ', indent);
                            try writer.writeAll("]");
                        }
                    },
                    .Struct => |sti| {
                        try writer.writeAll(@typeName(@TypeOf(thing)));
                        try writer.writeAll("{\n");
                        inline for (sti.fields) |field| {
                            try writer.writeByteNTimes(' ', indent + 4);
                            try std.fmt.format(writer, ".{s} = ", .{field.name});
                            try dumpInto(writer, indent + 4, @field(thing, field.name));
                            try writer.writeAll(",\n");
                        }
                        try writer.writeByteNTimes(' ', indent);
                        try writer.writeAll("}");
                    },
                    .Union => |uti| {
                        if (uti.tag_type) |tag_type| {
                            try writer.writeAll(@typeName(@TypeOf(thing)));
                            try writer.writeAll("{\n");
                            inline for (@typeInfo(tag_type).Enum.fields) |fti| {
                                if (@enumToInt(std.meta.activeTag(thing)) == fti.value) {
                                    try writer.writeByteNTimes(' ', indent + 4);
                                    try std.fmt.format(writer, ".{s} = ", .{fti.name});
                                    try dumpInto(writer, indent + 4, @field(thing, fti.name));
                                    try writer.writeAll("\n");
                                    try writer.writeByteNTimes(' ', indent);
                                    try writer.writeAll("}");
                                }
                            }
                        } else {
                            // bail
                            try std.fmt.format(writer, "{}", .{thing});
                        }
                    },
                    .Optional => {
                        if (thing == null) {
                            try writer.writeAll("null");
                        } else {
                            try dumpInto(writer, indent, thing.?);
                        }
                    },
                    else => {
                        // bail
                        try std.fmt.format(writer, "{any}", .{thing});
                    },
                }
            },
        }
    }
}
