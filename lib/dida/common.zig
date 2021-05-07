//! Things we want imported in every module for convenience.

pub const dida = @import("../dida.zig");
pub const builtin = @import("builtin");
pub const std = @import("std");
pub const panic = std.debug.panic;
pub const warn = std.debug.warn;
pub const debug_assert = std.debug.assert;
pub const max = std.math.max;
pub const min = std.math.min;
pub const Allocator = std.mem.Allocator;
pub const ArenaAllocator = std.heap.ArenaAllocator;
pub const ArrayList = std.ArrayList;
pub const HashMap = std.HashMap;
pub const AutoHashMap = std.AutoHashMap;

pub fn release_assert(condition: bool, comptime message: []const u8, args: anytype) void {
    if (!condition) panic(message, args);
}

pub fn TODO() noreturn {
    panic("TODO", .{});
}

pub fn DeepHashMap(comptime K: type, comptime V: type) type {
    return std.HashMap(K, V, struct {
        fn hash(key: K) u64 {
            return dida.meta.deepHash(key);
        }
    }.hash, struct {
        fn equal(a: K, b: K) bool {
            return dida.meta.deepEqual(a, b);
        }
    }.equal, std.hash_map.DefaultMaxLoadPercentage);
}

pub fn DeepHashSet(comptime K: type) type {
    return DeepHashMap(K, void);
}

pub fn dump(thing: anytype) void {
    const held = std.debug.getStderrMutex().acquire();
    defer held.release();
    const my_stderr = std.io.getStdErr().writer();
    dumpInto(my_stderr, 0, thing) catch return;
    my_stderr.writeAll("\n") catch return;
}

pub fn dumpInto(out_stream: anytype, indent: u32, thing: anytype) anyerror!void {
    const ti = @typeInfo(@TypeOf(thing));
    switch (ti) {
        .Pointer => |pti| {
            switch (pti.size) {
                .One => {
                    try out_stream.writeAll("&");
                    try dumpInto(out_stream, indent, thing.*);
                },
                .Many => {
                    // bail
                    try std.fmt.format(out_stream, "{}", .{thing});
                },
                .Slice => {
                    if (pti.child == u8) {
                        try std.fmt.format(out_stream, "\"{s}\"", .{thing});
                    } else {
                        try std.fmt.format(out_stream, "[]{}[\n", .{@typeName(pti.child)});
                        for (thing) |elem| {
                            try out_stream.writeByteNTimes(' ', indent + 4);
                            try dumpInto(out_stream, indent + 4, elem);
                            try out_stream.writeAll(",\n");
                        }
                        try out_stream.writeByteNTimes(' ', indent);
                        try out_stream.writeAll("]");
                    }
                },
                .C => {
                    // bail
                    try std.fmt.format(out_stream, "{}", .{thing});
                },
            }
        },
        .Array => |ati| {
            if (ati.child == u8) {
                try std.fmt.format(out_stream, "\"{s}\"", .{thing});
            } else {
                try std.fmt.format(out_stream, "[{}]{}[\n", .{ ati.len, @typeName(ati.child) });
                for (thing) |elem| {
                    try out_stream.writeByteNTimes(' ', indent + 4);
                    try dumpInto(out_stream, indent + 4, elem);
                    try out_stream.writeAll(",\n");
                }
                try out_stream.writeByteNTimes(' ', indent);
                try out_stream.writeAll("]");
            }
        },
        .Struct => |sti| {
            try out_stream.writeAll(@typeName(@TypeOf(thing)));
            try out_stream.writeAll("{\n");
            inline for (sti.fields) |field| {
                try out_stream.writeByteNTimes(' ', indent + 4);
                try std.fmt.format(out_stream, ".{} = ", .{field.name});
                try dumpInto(out_stream, indent + 4, @field(thing, field.name));
                try out_stream.writeAll(",\n");
            }
            try out_stream.writeByteNTimes(' ', indent);
            try out_stream.writeAll("}");
        },
        .Union => |uti| {
            if (uti.tag_type) |tag_type| {
                try out_stream.writeAll(@typeName(@TypeOf(thing)));
                try out_stream.writeAll("{\n");
                inline for (@typeInfo(tag_type).Enum.fields) |fti| {
                    if (@enumToInt(std.meta.activeTag(thing)) == fti.value) {
                        try out_stream.writeByteNTimes(' ', indent + 4);
                        try std.fmt.format(out_stream, ".{} = ", .{fti.name});
                        try dumpInto(out_stream, indent + 4, @field(thing, fti.name));
                        try out_stream.writeAll("\n");
                        try out_stream.writeByteNTimes(' ', indent);
                        try out_stream.writeAll("}");
                    }
                }
            } else {
                // bail
                try std.fmt.format(out_stream, "{}", .{thing});
            }
        },
        else => {
            // bail
            try std.fmt.format(out_stream, "{}", .{thing});
        },
    }
}

pub fn format(allocator: *Allocator, comptime fmt: []const u8, args: anytype) ![]const u8 {
    var buf = ArrayList(u8).init(allocator);
    var out = buf.outStream();
    try std.fmt.format(out, fmt, args);
    return buf.items;
}
