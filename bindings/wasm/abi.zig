//! Wasm abi mimicking NAPI
//! Used by ../js_common.zig

usingnamespace @import("../js_common.zig");

// --- wasm-specific stuff ---

pub const js = struct {
    pub extern fn getU32(Value) u32;
    pub extern fn getI32(Value) i32;
    pub extern fn pushU32(u32) Value;
    pub extern fn pushI32(i32) Value;
    pub extern fn pushString(u32, u32) Value;
    pub extern fn pushObject() Value;
    pub extern fn getProperty(Value, Value) Value;
    pub extern fn setProperty(Value, Value, Value) void;
    pub extern fn consoleLog(Value) void;
    pub extern fn consoleError(Value) void;
};

fn HandleAbiForFunction(comptime num_args: usize) type {
    return switch (num_args) {
        0 => fn callback() callconv(.C) Value,
        1 => fn callback(Value) callconv(.C) Value,
        2 => fn callback(Value, Value) callconv(.C) Value,
        3 => fn callback(Value, Value, Value) callconv(.C) Value,
        else => dida.common.compileError("Need to add a boilerplate branch for exporting functions with {} args", .{num_args}),
    };
}

pub fn handleAbiForFunction(
    comptime num_args: usize,
    comptime zig_fn: fn (env: Env, []const Value) Value,
) HandleAbiForFunction(num_args) {
    // TODO this would be much nicer if @Type() allowed creating functions
    return switch (num_args) {
        0 => struct {
            fn callback() callconv(.C) Value {
                return @call(.{}, zig_fn, .{ {}, &[_]Value{} });
            }
        }.callback,
        1 => struct {
            fn callback(a1: Value) callconv(.C) Value {
                return @call(.{}, zig_fn, .{ {}, &[_]Value{a1} });
            }
        }.callback,
        2 => struct {
            fn callback(a1: Value, a2: Value) callconv(.C) Value {
                return @call(.{}, zig_fn, .{ {}, &[_]Value{ a1, a2 } });
            }
        }.callback,
        3 => struct {
            fn callback(a1: Value, a2: Value, a3: Value) callconv(.C) Value {
                return @call(.{}, zig_fn, .{ {}, &[_]Value{ a1, a2, a3 } });
            }
        }.callback,
        else => dida.common.compileError("Need to add a boilerplate branch for exporting functions with {} args", .{num_args}),
    };
}

// --- interface required by js_common ---

pub const Env = void;
pub const Value = i32;
pub const Reference = i32;

comptime {
    dida.common.comptimeAssert(@bitSizeOf(*c_void) == 32, "Expect wasm to have 32 bit addresses", .{});
}

pub fn getUint32(env: Env, value: Value) u32 {
    return js.getU32(value);
}

pub fn getInt32(env: Env, value: Value) i32 {
    return js.getI32(value);
}

// TODO rename abi number fns
pub fn createUint32(env: Env, int: u32) Value {
    return js.pushU32(int);
}

pub fn createInt32(env: Env, int: i32) Value {
    return js.pushI32(int);
}

pub fn createString(env: Env, string: []const u8) Value {
    return js.pushString(@intCast(u32, @ptrToInt(@ptrCast([*c]const u8, string))), @intCast(u32, string.len));
}

pub fn createObject(env: Env) Value {
    return js.pushObject();
}

// TODO this is totally broken - what exactly is the abi here?
//      makes more sense to call constructors?
pub fn createExternal(env: Env, pointer: *c_void) Value {
    const address = @intCast(u32, @ptrToInt(pointer));
    return createUint32(env, address);
}

pub fn getExternal(env: Env, external: Value) *c_void {
    const address = getUint32({}, external);
    return @intToPtr(*c_void, @intCast(usize, address));
}

pub fn getProperty(env: Env, object: Value, name: Value) Value {
    return js.getProperty(object, name);
}

pub fn setProperty(env: Env, object: Value, name: Value, value: Value) void {
    return js.setProperty(object, name, value);
}
