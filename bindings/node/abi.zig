//! Wrapper aound [NAPI](https://nodejs.org/api/n-api.html).
//! Used by ../js_common.zig

usingnamespace @import("../js_common.zig");

// --- node-specific stuff ---

const c = @cImport({
    @cInclude("node_api.h");
});

fn napiCall(comptime napi_fn: anytype, args: anytype, comptime ReturnType: type) ReturnType {
    if (ReturnType != void) {
        var result: ReturnType = undefined;
        const status: c.napi_status = @call(.{}, napi_fn, args ++ .{&result});
        dida.common.assert(status == .napi_ok, "Call returned status {}", .{status});
        return result;
    } else {
        const status: c.napi_status = @call(.{}, napi_fn, args);
        dida.common.assert(status == .napi_ok, "Call returned status {}", .{status});
    }
}

pub fn createFunction(
    env: Env,
    name: [:0]const u8,
    comptime num_args: usize,
    comptime zig_fn: fn (env: Env, []const Value) Value,
) Value {
    const callback = struct {
        fn callback(callback_env: Env, info: c.napi_callback_info) callconv(.C) Value {
            var actual_num_args: usize = num_args;
            var args: [num_args]Value = undefined;
            var this: Value = undefined;
            napiCall(c.napi_get_cb_info, .{
                callback_env,
                info,
                &actual_num_args,
                // TODO try removing this
                if (num_args == 0) null else @ptrCast([*]Value, &args),
                &this,
                null,
            }, void);
            dida.common.assert(
                actual_num_args == num_args,
                "Expected {} args, got {} args",
                .{
                    num_args,
                    actual_num_args,
                },
            );
            return @call(.{}, zig_fn, .{ callback_env, &args });
        }
    }.callback;
    return napiCall(c.napi_create_function, .{ env, name, name.len, callback, null }, Value);
}

// --- interface required by js_common ---

pub const Env = c.napi_env;
pub const Value = c.napi_value;
pub const RefCounted = c.napi_ref;

pub fn jsTypeOf(env: Env, value: Value) JsType {
    const napi_type = napiCall(c.napi_typeof, .{ env, value }, c.napi_valuetype);
    return switch (napi_type) {
        .napi_undefined => .Undefined,
        .napi_null => .Null,
        .napi_boolean => .Boolean,
        .napi_number => .Number,
        .napi_string => .String,
        .napi_object => .Object,
        .napi_function => .Function,
        else => dida.common.panic("Don't know how to handle this napi_valuetype: {}", .{napi_type}),
    };
}

pub fn createUndefined(env: Env) Value {
    return napiCall(c.napi_get_undefined, .{env}, Value);
}

pub fn createBoolean(env: Env, value: bool) Value {
    // Not a typo - napi_get_boolean retrieves a global singleton
    return napiCall(c.napi_get_boolean, .{ env, value }, Value);
}

pub fn createInt32(env: Env, int: i32) Value {
    return napiCall(c.napi_create_int32, .{ env, int }, Value);
}

pub fn createInt64(env: Env, int: i64) Value {
    return napiCall(c.napi_create_int64, .{ env, int }, Value);
}

pub fn createFloat64(env: Env, float: f64) Value {
    return napiCall(c.napi_create_double, .{ env, float }, Value);
}

pub fn createString(env: Env, utf8_string: []const u8) Value {
    return napiCall(c.napi_create_string_utf8, .{ env, @ptrCast([*c]const u8, utf8_string), utf8_string.len }, Value);
}

pub fn createObject(env: Env) Value {
    return napiCall(c.napi_create_object, .{env}, Value);
}

pub fn createArray(env: Env, len: usize) Value {
    return napiCall(c.napi_create_array_with_length, .{ env, @intCast(u32, len) }, Value);
}

pub fn createExternal(env: Env, pointer: *c_void) Value {
    return napiCall(c.napi_create_external, .{ env, pointer, null, null }, Value);
}

pub fn createRefCounted(env: Env, value: Value, refcount: u32) RefCounted {
    return napiCall(c.napi_create_reference, .{ env, value, refcount }, RefCounted);
}

pub fn getInt32(env: Env, value: Value) i32 {
    return napiCall(c.napi_get_value_int32, .{ env, value }, i32);
}

pub fn getInt64(env: Env, value: Value) i64 {
    return napiCall(c.napi_get_value_int64, .{ env, value }, i64);
}

pub fn getFloat64(env: Env, value: Value) f64 {
    return napiCall(c.napi_get_value_double, .{ env, value }, f64);
}

pub fn getString(env: Env, value: Value) ![]const u8 {
    const len = napiCall(c.napi_get_value_string_utf8, .{ env, value, null, 0 }, usize);
    // len+1 for null byte
    var buffer = try allocator.alloc(u8, len + 1);
    return getStringInto(env, value, buffer);
}

pub fn getStringInto(env: Env, value: Value, buffer: []u8) []const u8 {
    const len = napiCall(c.napi_get_value_string_utf8, .{ env, value, @ptrCast([*c]u8, buffer), buffer.len }, usize);
    return buffer[0..len];
}

pub fn getExternal(env: Env, value: Value) *c_void {
    return napiCall(c.napi_get_value_external, .{ env, value }, ?*c_void).?;
}

pub fn getRefCounted(env: Env, ref: RefCounted) Value {
    return napiCall(c.napi_get_reference_value, .{ env, ref }, Value);
}

pub fn getArrayLength(env: Env, array: Value) u32 {
    return napiCall(c.napi_get_array_length, .{ env, array }, u32);
}

pub fn getElement(env: Env, array: Value, index: u32) Value {
    return napiCall(c.napi_get_element, .{ env, array, index }, Value);
}

pub fn setElement(env: Env, array: Value, index: u32, value: Value) void {
    napiCall(c.napi_set_element, .{ env, array, index, value }, void);
}

pub fn setProperty(env: Env, object: Value, name: Value, value: Value) void {
    napiCall(c.napi_set_property, .{ env, object, name, value }, void);
}

pub fn getProperty(env: Env, object: Value, name: Value) Value {
    return napiCall(c.napi_get_property, .{ env, object, name }, Value);
}

pub fn callFunction(env: Env, function: Value, args: []const Value) Value {
    dida.common.assert(!napiCall(c.napi_is_exception_pending, .{env}, bool), "Shouldn't be any exceptions before function call", .{});
    const napi_undefined = getUndefined(env);
    const output = napiCall(c.napi_call_function, .{ env, napi_undefined, function, args.len, @ptrCast([*c]const Value, args) }, Value);
    dida.common.assert(!napiCall(c.napi_is_exception_pending, .{env}, bool), "Shouldn't be any exceptions after function call", .{});
    return output;
}
