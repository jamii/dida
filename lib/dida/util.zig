//! Things that we use often

const std = @import("std");
const dida = @import("../dida.zig");

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
    return std.HashMap(K, V, DeepHashContext(K), std.hash_map.DefaultMaxLoadPercentage);
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

// TODO this can be error-prone - maybe should explicitly list allowed types?
pub fn deepClone(thing: anytype, allocator: *Allocator) error{OutOfMemory}!@TypeOf(thing) {
    const T = @TypeOf(thing);
    const ti = @typeInfo(T);

    if (T == *std.mem.Allocator)
        return allocator;

    if (comptime std.mem.startsWith(u8, @typeName(T), "std.array_list.ArrayList")) {
        var cloned = try ArrayList(@TypeOf(thing.items[0])).initCapacity(allocator, thing.items.len);
        cloned.appendSliceAssumeCapacity(thing.items);
        for (cloned.items) |*item| item.* = try deepClone(item.*, allocator);
        return cloned;
    }

    if (comptime std.mem.startsWith(u8, @typeName(T), "std.hash_map.HashMap")) {
        var cloned = try thing.cloneWithAllocator(allocator);
        var iter = cloned.iterator();
        while (iter.next()) |entry| {
            entry.key_ptr.* = try deepClone(entry.key_ptr.*, allocator);
            entry.value_ptr.* = try deepClone(entry.value_ptr.*, allocator);
        }
        return cloned;
    }

    switch (ti) {
        .Bool, .Int, .Float, .Enum, .Void, .Fn => return thing,
        .Pointer => |pti| {
            switch (pti.size) {
                .One => {
                    const cloned = try allocator.create(pti.child);
                    cloned.* = try deepClone(thing.*, allocator);
                    return cloned;
                },
                .Slice => {
                    const cloned = try allocator.alloc(pti.child, thing.len);
                    for (thing) |item, i| cloned[i] = try deepClone(item, allocator);
                    return cloned;
                },
                .Many, .C => compileError("Cannot deepClone {}", .{T}),
            }
        },
        .Array => {
            var cloned: T = undefined;
            for (cloned) |*item| item.* = try deepClone(item.*, allocator);
            return cloned;
        },
        .Optional => {
            return if (thing == null) null else try deepClone(thing.?, allocator);
        },
        .Struct => |sti| {
            var cloned: T = undefined;
            inline for (sti.fields) |fti| {
                @field(cloned, fti.name) = try deepClone(@field(thing, fti.name), allocator);
            }
            return cloned;
        },
        .Union => |uti| {
            if (uti.tag_type) |tag_type| {
                const tag = @enumToInt(std.meta.activeTag(thing));
                inline for (@typeInfo(tag_type).Enum.fields) |fti| {
                    if (tag == fti.value) {
                        return @unionInit(T, fti.name, try deepClone(@field(thing, fti.name), allocator));
                    }
                }
                unreachable;
            } else {
                compileError("Cannot deepClone {}", .{T});
            }
        },
        else => compileError("Cannot deepClone {}", .{T}),
    }
}
