const std = @import("std");
const dida = @import("../../core/dida.zig");

const c = @cImport({
    @cInclude("node_api.h");
});

var gpa = std.heap.GeneralPurposeAllocator(.{
    .safety = true,
    .never_unmap = true,
}){};
var arena = std.heap.ArenaAllocator.init(&gpa.allocator);
const allocator = &arena.allocator;

export fn napi_register_module_v1(env: c.napi_env, exports: c.napi_value) c.napi_value {
    {
        const GraphBuilder_init_fn = napiCall(c.napi_create_function, .{ env, "GraphBuilder_init", "GraphBuilder_init".len, GraphBuilder_init, null }, c.napi_value);
        napiCall(c.napi_set_named_property, .{ env, exports, "GraphBuilder_init", GraphBuilder_init_fn }, void);
    }

    {
        const GraphBuilder_addSubgraph_fn = napiCall(c.napi_create_function, .{ env, "GraphBuilder_addSubgraph", "GraphBuilder_addSubgraph".len, GraphBuilder_addSubgraph, null }, c.napi_value);
        napiCall(c.napi_set_named_property, .{ env, exports, "GraphBuilder_addSubgraph", GraphBuilder_addSubgraph_fn }, void);
    }

    return exports;
}

fn GraphBuilder_init(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    const args_and_this = napiGetCbInfo(env, info, 0);

    const graph_builder = allocator.create(dida.GraphBuilder) catch |err| dida.common.panic("{}", .{err});
    graph_builder.* = dida.GraphBuilder.init(allocator);

    return napiCreateExternal(env, graph_builder);
}

fn GraphBuilder_addSubgraph(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    const args_and_this = napiGetCbInfo(env, info, 2);
    const graph_builder = napiGetExternal(env, args_and_this.args[0], dida.GraphBuilder);
    const parent = napiGetValue(env, args_and_this.args[1], dida.Subgraph);

    const subgraph = graph_builder.addSubgraph(parent) catch |err| dida.common.panic("{}", .{err});

    return napiCreateValue(env, subgraph);
}

// --- helpers ---

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

fn napiGetCbInfo(env: c.napi_env, info: c.napi_callback_info, comptime expected_num_args: usize) struct { args: [expected_num_args]c.napi_value, this: c.napi_value } {
    var actual_num_args: usize = expected_num_args;
    var args: [expected_num_args]c.napi_value = undefined;
    var this: c.napi_value = undefined;
    napiCall(c.napi_get_cb_info, .{ env, info, &actual_num_args, if (expected_num_args == 0) null else @ptrCast([*]c.napi_value, &args), &this, null }, void);
    dida.common.assert(actual_num_args == expected_num_args, "Expected {} args, got {} args", .{ expected_num_args, actual_num_args });
    return .{
        .args = args,
        .this = this,
    };
}

const usize_bits = @bitSizeOf(usize);
comptime {
    if (usize_bits != 64 and usize_bits != 32) {
        @compileError("Can only handle 32 bit or 64 bit architectures");
    }
}

fn napiCreateExternal(env: c.napi_env, value: anytype) c.napi_value {
    const info = @typeInfo(@TypeOf(value));
    dida.common.assert(info == .Pointer and info.Pointer.size == .One, "napiCreateExternal should be called with *T, got {}", .{@typeName(@TypeOf(value))});
    return napiCall(c.napi_create_external, .{ env, @ptrCast(*c_void, value), null, null }, c.napi_value);
}

fn napiGetExternal(env: c.napi_env, value: c.napi_value, comptime ReturnType: type) *ReturnType {
    const ptr = napiCall(c.napi_get_value_external, .{ env, value }, ?*c_void);
    return @ptrCast(*ReturnType, @alignCast(@alignOf(ReturnType), ptr));
}

fn napiCreateValue(env: c.napi_env, value: anytype) c.napi_value {
    const info = @typeInfo(@TypeOf(value));
    switch (info) {
        .Int => {
            const cast_value = switch (@TypeOf(value)) {
                usize => @intCast(isize, value),
                else => value,
            };
            const napi_fn = switch (@TypeOf(cast_value)) {
                isize => if (usize_bits == 64) c.napi_create_int64 else c.napi_create_int32,
                else => dida.common.panic("Don't know how to create napi value for {}", @TypeOf(cast_value)),
            };
            return napiCall(napi_fn, .{ env, cast_value }, c.napi_value);
        },
        .Struct => |struct_info| {
            // TODO would like to use napi_new_instance to name this object, but it requires having already registered a constructor
            var result = napiCall(c.napi_create_object, .{env}, c.napi_value);
            inline for (struct_info.fields) |field_info| {
                var field_name: [field_info.name.len:0]u8 = undefined;
                std.mem.copy(u8, &field_name, field_info.name);
                field_name[field_info.name.len] = 0;
                const field_value = napiCreateValue(env, @field(value, field_info.name));
                napiCall(c.napi_set_named_property, .{ env, result, &field_name, field_value }, void);
            }
            return result;
        },
        else => dida.common.panic("Dont know how to create value of type {}", .{@TypeOf(value)}),
    }
}

fn napiGetValue(env: c.napi_env, value: c.napi_value, comptime ReturnType: type) ReturnType {
    const info = @typeInfo(ReturnType);
    switch (info) {
        .Int => {
            const napi_fn = switch (ReturnType) {
                usize, isize => if (usize_bits == 64) c.napi_get_value_int64 else c.napi.get_value_int32,
                else => dida.common.panic("Don't know how to get napi value for {}", @TypeOf(cast_value)),
            };
            const CastType = switch (ReturnType) {
                usize => isize,
                else => ReturnType,
            };
            const result = napiCall(napi_fn, .{ env, value }, CastType);
            return @intCast(ReturnType, result);
        },
        .Struct => |struct_info| {
            var result: ReturnType = undefined;
            inline for (struct_info.fields) |field_info| {
                var field_name: [field_info.name.len:0]u8 = undefined;
                std.mem.copy(u8, &field_name, field_info.name);
                field_name[field_info.name.len] = 0;
                const field_value = napiCall(c.napi_get_named_property, .{ env, value, &field_name }, c.napi_value);
                @field(result, field_info.name) = napiGetValue(env, field_value, field_info.field_type);
            }
            return result;
        },
        else => dida.common.panic("Dont know how to get value of type {}", .{ReturnType}),
    }
}
