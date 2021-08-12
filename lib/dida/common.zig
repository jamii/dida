//! Things we want imported in every module for convenience.

pub const dida = @import("../dida.zig");
pub const builtin = @import("builtin");
pub const std = @import("std");
pub const warn = std.debug.warn;
pub const debug_assert = std.debug.assert;
pub const max = std.math.max;
pub const min = std.math.min;
pub const Allocator = std.mem.Allocator;
pub const ArenaAllocator = std.heap.ArenaAllocator;
pub const ArrayList = std.ArrayList;
pub const HashMap = std.HashMap;
pub const AutoHashMap = std.AutoHashMap;

pub fn panic(comptime message: []const u8, args: anytype) noreturn {
    // TODO should we preallocate memory for panics?
    var buf = ArrayList(u8).init(std.heap.page_allocator);
    var writer = buf.writer();
    std.fmt.format(writer, message, args) catch
        std.mem.copy(u8, buf.items[buf.items.len - 3 .. buf.items.len], "OOM");
    @panic(buf.toOwnedSlice());
}

pub fn assert(condition: bool, comptime message: []const u8, args: anytype) void {
    if (!condition) panic(message, args);
}

pub fn comptimeAssert(comptime condition: bool, comptime message: []const u8, comptime args: anytype) void {
    if (!condition) compileError(message, args);
}

pub fn compileError(comptime message: []const u8, comptime args: anytype) void {
    @compileError(comptime std.fmt.comptimePrint(message, args));
}

pub fn DeepHashMap(comptime K: type, comptime V: type) type {
    return std.HashMap(K, V, dida.meta.DeepHashContext(K), std.hash_map.DefaultMaxLoadPercentage);
}

pub fn DeepHashSet(comptime K: type) type {
    return DeepHashMap(K, void);
}

pub fn format(allocator: *Allocator, comptime fmt: []const u8, args: anytype) ![]const u8 {
    var buf = ArrayList(u8).init(allocator);
    var writer = buf.writer();
    try std.fmt.format(writer, fmt, args);
    return buf.items;
}

// Chain casts from *T to *[1]T to []T
pub fn ptrToSlice(comptime T: type, input: *const T) []const T {
    const one_input: *const [1]T = input;
    return one_input;
}

pub fn last(comptime T: type, slice: []const T) T {
    assert(slice.len > 0, "Tried to take last item of a 0-length slice", .{});
    return slice[slice.len - 1];
}

pub fn Queue(comptime T: type) type {
    return struct {
        in: ArrayList(T),
        out: ArrayList(T),

        const Self = @This();

        pub fn init(allocator: *Allocator) Self {
            return .{
                .in = ArrayList(T).init(allocator),
                .out = ArrayList(T).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            self.in.deinit();
            self.out.deinit();
            self.* = undefined;
        }

        pub fn push(self: *Self, item: T) !void {
            try self.in.append(item);
        }

        pub fn popOrNull(self: *Self) ?T {
            if (self.out.popOrNull()) |item| return item;
            std.mem.swap(ArrayList(T), &self.in, &self.out);
            std.mem.reverse(T, self.out.items);
            return self.out.popOrNull();
        }
    };
}

pub fn TODO() noreturn {
    panic("TODO", .{});
}

// This is only for debugging
pub fn dump(thing: anytype) void {
    const held = std.debug.getStderrMutex().acquire();
    defer held.release();
    const my_stderr = std.io.getStdErr().writer();
    dida.debug.dumpInto(my_stderr, 0, thing) catch return;
    my_stderr.writeAll("\n") catch return;
}
