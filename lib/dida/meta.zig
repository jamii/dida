//! Functions that can't be in common because we want to shadow the names on some structs

usingnamespace @import("./common.zig");

pub fn deepEqual(a: anytype, b: @TypeOf(a)) bool {
    return deepOrder(a, b) == .eq;
}

pub fn deepOrder(a: anytype, b: @TypeOf(a)) std.math.Order {
    const T = @TypeOf(a);
    const ti = @typeInfo(T);
    switch (ti) {
        .Struct, .Enum, .Union => {
            if (@hasDecl(T, "deepOrder")) {
                return T.deepOrder(a, b);
            }
        },
        else => {},
    }
    switch (ti) {
        .Bool => {
            if (a == b) return .Equal;
            if (a) return .GreaterThan;
            return .lt;
        },
        .Int, .Float => {
            if (a < b) {
                return .lt;
            }
            if (a > b) {
                return .gt;
            }
            return .eq;
        },
        .Enum => {
            return deepOrder(@enumToInt(a), @enumToInt(b));
        },
        .Pointer => |pti| {
            switch (pti.size) {
                .One => {
                    return deepOrder(a.*, b.*);
                },
                .Slice => {
                    if (a.len < b.len) {
                        return .lt;
                    }
                    if (a.len > b.len) {
                        return .gt;
                    }
                    for (a) |a_elem, a_ix| {
                        const ordering = deepOrder(a_elem, b[a_ix]);
                        if (ordering != .eq) {
                            return ordering;
                        }
                    }
                    return .eq;
                },
                .Many, .C => compileError("Cannot deepOrder {}", .{T}),
            }
        },
        .Optional => {
            if (a) |a_val| {
                if (b) |b_val| {
                    return deepOrder(a_val, b_val);
                } else {
                    return .gt;
                }
            } else {
                if (b) |_| {
                    return .lt;
                } else {
                    return .eq;
                }
            }
        },
        .Array => {
            for (a) |a_elem, a_ix| {
                const ordering = deepOrder(a_elem, b[a_ix]);
                if (ordering != .eq) {
                    return ordering;
                }
            }
            return .eq;
        },
        .Struct => |sti| {
            inline for (sti.fields) |fti| {
                const ordering = deepOrder(@field(a, fti.name), @field(b, fti.name));
                if (ordering != .eq) {
                    return ordering;
                }
            }
            return .eq;
        },
        .Union => |uti| {
            if (uti.tag_type) |tag_type| {
                const a_tag = @enumToInt(@as(tag_type, a));
                const b_tag = @enumToInt(@as(tag_type, b));
                if (a_tag < b_tag) {
                    return .lt;
                }
                if (a_tag > b_tag) {
                    return .gt;
                }
                inline for (@typeInfo(tag_type).Enum.fields) |fti| {
                    if (a_tag == fti.value) {
                        return deepOrder(
                            @field(a, fti.name),
                            @field(b, fti.name),
                        );
                    }
                }
                unreachable;
            } else {
                compileError("Cannot deepOrder {}", .{T});
            }
        },
        .Void => return .eq,
        .ErrorUnion => {
            if (a) |a_ok| {
                if (b) |b_ok| {
                    return deepOrder(a_ok, b_ok);
                } else |_| {
                    return .lt;
                }
            } else |a_err| {
                if (b) |_| {
                    return .gt;
                } else |b_err| {
                    return deepOrder(a_err, b_err);
                }
            }
        },
        .ErrorSet => return deepOrder(@errorToInt(a), @errorToInt(b)),
        else => compileError("Cannot deepOrder {}", .{T}),
    }
}

pub fn deepHash(key: anytype) u64 {
    var hasher = std.hash.Wyhash.init(0);
    deepHashInto(&hasher, key);
    return hasher.final();
}

pub fn deepHashInto(hasher: anytype, key: anytype) void {
    const T = @TypeOf(key);
    const ti = @typeInfo(T);
    switch (ti) {
        .Struct, .Enum, .Union => {
            if (@hasDecl(T, "deepHashInto")) {
                return T.deepHashInto(hasher, key);
            }
        },
        else => {},
    }
    switch (ti) {
        .Int => @call(.{ .modifier = .always_inline }, hasher.update, .{std.mem.asBytes(&key)}),
        .Float => |info| deepHashInto(hasher, @bitCast(std.meta.Int(.unsigned, info.bits), key)),
        .Bool => deepHashInto(hasher, @boolToInt(key)),
        .Enum => deepHashInto(hasher, @enumToInt(key)),
        .Pointer => |pti| {
            switch (pti.size) {
                .One => deepHashInto(hasher, key.*),
                .Slice => {
                    for (key) |element| {
                        deepHashInto(hasher, element);
                    }
                },
                .Many, .C => compileError("Cannot deepHash {}", .{T}),
            }
        },
        .Optional => if (key) |k| deepHashInto(hasher, k),
        .Array => {
            for (key) |element| {
                deepHashInto(hasher, element);
            }
        },
        .Struct => |info| {
            inline for (info.fields) |field| {
                deepHashInto(hasher, @field(key, field.name));
            }
        },
        .Union => |info| {
            if (info.tag_type) |tag_type| {
                const tag = std.meta.activeTag(key);
                deepHashInto(hasher, tag);
                inline for (@typeInfo(tag_type).Enum.fields) |fti| {
                    if (@enumToInt(std.meta.activeTag(key)) == fti.value) {
                        deepHashInto(hasher, @field(key, fti.name));
                        return;
                    }
                }
                unreachable;
            } else compileError("cannot deepHash {}", .{T});
        },
        .Void => {},
        else => compileError("cannot deepHash {}", .{T}),
    }
}

pub fn DeepHashContext(comptime K: type) type {
    return struct {
        const Self = @This();
        pub fn hash(_: Self, pseudo_key: K) u64 {
            return deepHash(pseudo_key);
        }
        pub fn eql(_: Self, pseudo_key: K, key: K) bool {
            return deepEqual(pseudo_key, key);
        }
    };
}

// This is only for debugging
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
                try dida.meta.dumpInto(writer, indent, thing.timestamps);
            },
            dida.core.NodeState.DistinctState => {
                try writer.writeAll("DistinctState{\n");

                try writer.writeByteNTimes(' ', indent + 4);
                try writer.writeAll("index: ");
                try dida.meta.dumpInto(writer, indent + 4, thing.index);
                try writer.writeAll(",\n");

                try writer.writeByteNTimes(' ', indent + 4);
                try writer.writeAll("pending_corrections: ");
                try dida.meta.dumpInto(writer, indent + 4, thing.pending_corrections);

                try writer.writeAll("\n");
                try writer.writeByteNTimes(' ', indent);
                try writer.writeAll("}");
            },
            dida.core.NodeState.ReduceState => {
                try writer.writeAll("ReduceState{\n");

                try writer.writeByteNTimes(' ', indent + 4);
                try writer.writeAll("index: ");
                try dida.meta.dumpInto(writer, indent + 4, thing.index);
                try writer.writeAll(",\n");

                try writer.writeByteNTimes(' ', indent + 4);
                try writer.writeAll("pending_corrections: ");
                try dida.meta.dumpInto(writer, indent + 4, thing.pending_corrections);

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
                    try dida.meta.dumpInto(writer, indent + 8, node_spec);
                    try writer.writeAll(",\n");

                    try writer.writeByteNTimes(' ', indent + 8);
                    try writer.writeAll("state: ");
                    try dida.meta.dumpInto(writer, indent + 8, thing.node_states[node_id]);
                    try writer.writeAll(",\n");

                    try writer.writeByteNTimes(' ', indent + 8);
                    try writer.writeAll("frontier: ");
                    try dida.meta.dumpInto(writer, indent + 8, thing.node_frontiers[node_id]);
                    try writer.writeAll(",\n");

                    try writer.writeByteNTimes(' ', indent + 8);
                    try writer.writeAll("unprocessed_change_batches: [\n");
                    {
                        for (thing.unprocessed_change_batches.items) |change_batch_at_node_input| {
                            if (change_batch_at_node_input.node_input.node.id == node_id) {
                                try writer.writeByteNTimes(' ', indent + 12);
                                try dida.meta.dumpInto(writer, indent + 12, change_batch_at_node_input.change_batch);
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
