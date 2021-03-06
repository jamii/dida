//! Wasm abi mimicking NAPI
//! Used by ../js_common.zig
//! TODO much of this could be automatically generated with https://github.com/ziglang/zig/issues/6709

const dida = @import("../../lib/dida.zig");
const js_common = @import("../js_common.zig");

// --- wasm-specific stuff ---

pub const js = struct {
    pub extern fn jsTypeOf(Value) u32;
    pub extern fn createUndefined() Value;
    pub extern fn createBool(bool) Value;
    pub extern fn createU32(u32) Value;
    pub extern fn createI32(i32) Value;
    pub extern fn createI64(i64) Value;
    pub extern fn createF64(f64) Value;
    pub extern fn createString(u32, u32) Value;
    pub extern fn createObject() Value;
    pub extern fn createArray(u32) Value;
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
        else => dida.util.compileError("Need to add a boilerplate branch for exporting functions with {} args", .{num_args}),
    };
}

pub fn handleAbiForFunction(
    comptime num_args: usize,
    comptime zig_fn: fn (env: Env, []const Value) Value,
) HandleAbiForFunction(num_args) {
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
        else => dida.util.compileError("Need to add a boilerplate branch for exporting functions with {} args", .{num_args}),
    };
}

// --- interface required by js_common ---

pub const Env = void;
pub const Value = i32;
pub const RefCounted = i32;

comptime {
    dida.util.comptimeAssert(@bitSizeOf(*anyopaque) == 32, "Expect wasm to have 32 bit addresses", .{});
}

pub fn jsTypeOf(_: Env, value: Value) js_common.JsType {
    return @intToEnum(js_common.JsType, @intCast(u3, js.jsTypeOf(value)));
}

pub fn createUndefined(_: Env) Value {
    return js.createUndefined();
}

pub fn createBoolean(_: Env, b: bool) Value {
    return js.createBool(b);
}

pub fn createU32(_: Env, int: u32) Value {
    return js.createU32(int);
}

pub fn createI32(_: Env, int: i32) Value {
    return js.createI32(int);
}

pub fn createI64(_: Env, int: i64) Value {
    return js.createI64(int);
}

pub fn createF64(_: Env, int: f64) Value {
    return js.createF64(int);
}

pub fn createString(_: Env, string: []const u8) Value {
    return js.createString(@as(u32, @ptrToInt(@ptrCast([*c]const u8, string))), @as(u32, string.len));
}

pub fn createObject(_: Env) Value {
    return js.createObject();
}

pub fn createArray(_: Env, len: usize) Value {
    return js.createArray(len);
}

pub fn createRefCounted(_: Env, value: Value, refcount: u32) RefCounted {
    return js.createRefCounted(value, refcount);
}

pub fn createExternal(env: Env, pointer: *anyopaque) Value {
    const address = @as(u32, @ptrToInt(pointer));
    return createU32(env, address);
}

pub fn getU32(_: Env, value: Value) u32 {
    return js.getU32(value);
}

pub fn getI32(_: Env, value: Value) i32 {
    return js.getI32(value);
}

pub fn getI64(_: Env, value: Value) i64 {
    return js.getI64(value);
}

pub fn getF64(_: Env, value: Value) f64 {
    return js.getF64(value);
}

pub fn getString(env: Env, value: Value) ![]const u8 {
    const len = js.getStringLength(value);
    var buffer = try js_common.allocator.alloc(u8, len);
    return getStringInto(env, value, buffer);
}

pub fn getStringInto(_: Env, value: Value, buffer: []u8) []const u8 {
    const len = js.getStringInto(value, @as(u32, @ptrToInt(@ptrCast([*c]u8, buffer))), buffer.len);
    return buffer[0..len];
}

pub fn getExternal(_: Env, external: Value) *anyopaque {
    const address = getU32({}, external);
    return @intToPtr(*anyopaque, @as(usize, address));
}

pub fn getRefCounted(_: Env, ref: RefCounted) Value {
    return js.getRefCounted(ref);
}

pub fn getArrayLength(_: Env, array: Value) u32 {
    return js.getArrayLength(array);
}

pub fn getElement(_: Env, array: Value, index: u32) Value {
    return js.getElement(array, index);
}

pub fn setElement(_: Env, array: Value, index: u32, value: Value) void {
    js.setElement(array, index, value);
}

pub fn getProperty(_: Env, object: Value, name: Value) Value {
    return js.getProperty(object, name);
}

pub fn setProperty(_: Env, object: Value, name: Value, value: Value) void {
    return js.setProperty(object, name, value);
}

pub fn callFunction(env: Env, function: Value, args: []const Value) Value {
    const args_array = createArray(env, args.len);
    for (args) |arg, i|
        setElement(env, args_array, @as(u32, i), arg);
    return js.callFunction(function, args_array);
}
