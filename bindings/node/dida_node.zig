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
        const name = "GraphBuilder";
        const properties = [_]c.napi_property_descriptor{
            .{
                .utf8name = "addSubgraph",
                .name = null,
                .method = GraphBuilder_addSubgraph,
                .getter = null,
                .setter = null,
                .value = null,
                // TODO use napi_default_method?
                .attributes = .napi_default,
                .data = null,
            },
        };
        const class = napiCall(c.napi_define_class, .{ env, name, name.len, GraphBuilder_constructor, null, properties.len, &properties }, c.napi_value);
        napiCall(c.napi_set_named_property, .{ env, exports, "GraphBuilder", class }, void);
    }

    return exports;
}

fn GraphBuilder_constructor(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    const target = napiCall(c.napi_get_new_target, .{ env, info }, c.napi_value);
    dida.common.assert(target != null, "Wasn't called with new", .{});

    const args_and_this = napiGetCbInfo(env, info, 0);

    const graph_builder = allocator.create(dida.GraphBuilder) catch |err| dida.common.panic("{}", .{err});
    graph_builder.* = dida.GraphBuilder.init(allocator);

    // TODO this fails for some reason
    //_ = napiCall(c.napi_wrap, .{ env, args_and_this.this, @ptrCast(*c_void, graph_builder), null, null }, c.napi_ref);

    // TODO do we need to store the wrapper somewhere?
    napiCall(c.napi_wrap, .{ env, args_and_this.this, @ptrCast(*c_void, graph_builder), null, null, null }, void);

    return args_and_this.this;
}

fn GraphBuilder_addSubgraph(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    const args_and_this = napiGetCbInfo(env, info, 1);
    const graph_builder = napiUnwrap(env, args_and_this.this, dida.GraphBuilder);
    const parent_id = napiGetValue(env, args_and_this.args[0], usize);

    const subgraph = graph_builder.addSubgraph(.{ .id = parent_id }) catch |err| dida.common.panic("{}", .{err});

    return napiCreateValue(env, subgraph.id);
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

fn napiCreateValue(env: c.napi_env, value: anytype) c.napi_value {
    const cast_value = switch (@TypeOf(value)) {
        usize => @intCast(isize, value),
        else => value,
    };
    const napi_fn = switch (@TypeOf(cast_value)) {
        isize => if (usize_bits == 64) c.napi_create_int64 else c.napi_create_int32,
        else => dida.common.panic("Don't know how to create napi value for {}", @TypeOf(cast_value)),
    };
    return napiCall(napi_fn, .{ env, cast_value }, c.napi_value);
}

fn napiGetValue(env: c.napi_env, value: c.napi_value, comptime ReturnType: type) ReturnType {
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
}

fn napiUnwrap(env: c.napi_env, value: c.napi_value, comptime WrappedType: type) *WrappedType {
    const pointer = napiCall(c.napi_unwrap, .{ env, value }, ?*c_void);
    return @ptrCast(*WrappedType, @alignCast(@alignOf(WrappedType), pointer));
}
