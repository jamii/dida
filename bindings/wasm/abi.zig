//! Wasm abi mimicking NAPI
//! Used by ../js_common.zig

usingnamespace @import("../js_common.zig");

// --- wasm-specific stuff ---

pub const js = struct {
    pub extern fn jsTypeOf(Value) u32;
    pub extern fn pushUndefined() Value;
    pub extern fn pushBool(bool) Value;
    pub extern fn pushU32(u32) Value;
    pub extern fn pushI32(i32) Value;
    pub extern fn pushI64(i64) Value;
    pub extern fn pushF64(f64) Value;
    pub extern fn pushString(u32, u32) Value;
    pub extern fn pushObject() Value;
    pub extern fn pushArray(u32) Value;
    pub extern fn createRefCounted(Value, u32) RefCounted;
    pub extern fn getRefCounted(RefCounted) Value;
    pub extern fn getU32(Value) u32;
    pub extern fn getI32(Value) i32;
    pub extern fn getI64(Value) i64;
    pub extern fn getF64(Value) f64;
    pub extern fn getStringLength(Value) u32;
    pub extern fn getStringInto(Value, u32, u32) u32;
    pub extern fn getArrayLength(Value) u32;
    pub extern fn getElement(Value, u32) Value;
    pub extern fn setElement(Value, u32, Value) void;
    pub extern fn getProperty(Value, Value) Value;
    pub extern fn setProperty(Value, Value, Value) void;
    pub extern fn callFunction(Value, Value) Value;
    pub extern fn consoleLog(Value) void;
    pub extern fn consoleError(Value) void;
    pub extern fn throwException(Value) noreturn;
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
// TODO should wrap these in structs to avoid type errors?
pub const Value = i32;
pub const RefCounted = i32;

comptime {
    dida.common.comptimeAssert(@bitSizeOf(*c_void) == 32, "Expect wasm to have 32 bit addresses", .{});
}

pub fn jsTypeOf(env: Env, value: Value) JsType {
    return @intToEnum(JsType, @intCast(u3, js.jsTypeOf(value)));
}

pub fn createUndefined(env: Env) Value {
    return js.pushUndefined();
}

pub fn createBoolean(env: Env, b: bool) Value {
    return js.pushBool(b);
}

// TODO rename abi number fns
pub fn createUint32(env: Env, int: u32) Value {
    return js.pushU32(int);
}

pub fn createInt32(env: Env, int: i32) Value {
    return js.pushI32(int);
}

pub fn createInt64(env: Env, int: i64) Value {
    return js.pushI64(int);
}

pub fn createFloat64(env: Env, int: f64) Value {
    return js.pushF64(int);
}

pub fn createString(env: Env, string: []const u8) Value {
    return js.pushString(@intCast(u32, @ptrToInt(@ptrCast([*c]const u8, string))), @intCast(u32, string.len));
}

pub fn createObject(env: Env) Value {
    return js.pushObject();
}

pub fn createArray(env: Env, len: usize) Value {
    return js.pushArray(len);
}

pub fn createRefCounted(env: Env, value: Value, refcount: u32) RefCounted {
    return js.createRefCounted(value, refcount);
}

// TODO this is totally broken - what exactly is the abi here?
//      makes more sense to call constructors?
pub fn createExternal(env: Env, pointer: *c_void) Value {
    const address = @intCast(u32, @ptrToInt(pointer));
    return createUint32(env, address);
}

pub fn getUint32(env: Env, value: Value) u32 {
    return js.getU32(value);
}

pub fn getInt32(env: Env, value: Value) i32 {
    return js.getI32(value);
}

pub fn getInt64(env: Env, value: Value) i64 {
    return js.getI64(value);
}

pub fn getFloat64(env: Env, value: Value) f64 {
    return js.getF64(value);
}

pub fn getString(env: Env, value: Value) ![]const u8 {
    const len = js.getStringLength(value);
    var buffer = try allocator.alloc(u8, len);
    return getStringInto(env, value, buffer);
}

pub fn getStringInto(env: Env, value: Value, buffer: []u8) []const u8 {
    const len = js.getStringInto(value, @intCast(u32, @ptrToInt(@ptrCast([*c]u8, buffer))), buffer.len);
    return buffer[0..len];
}

pub fn getExternal(env: Env, external: Value) *c_void {
    const address = getUint32({}, external);
    return @intToPtr(*c_void, @intCast(usize, address));
}

pub fn getRefCounted(env: Env, ref: RefCounted) Value {
    return js.getRefCounted(ref);
}

pub fn getArrayLength(env: Env, array: Value) u32 {
    return js.getArrayLength(array);
}

pub fn getElement(env: Env, array: Value, index: u32) Value {
    return js.getElement(array, index);
}

pub fn setElement(env: Env, array: Value, index: u32, value: Value) void {
    js.setElement(array, index, value);
}

pub fn getProperty(env: Env, object: Value, name: Value) Value {
    return js.getProperty(object, name);
}

pub fn setProperty(env: Env, object: Value, name: Value, value: Value) void {
    return js.setProperty(object, name, value);
}

pub fn callFunction(env: Env, function: Value, args: []const Value) Value {
    const args_array = createArray(env, args.len);
    for (args) |arg, i|
        setElement(env, args_array, @intCast(u32, i), arg);
    return js.callFunction(function, args_array);
}
