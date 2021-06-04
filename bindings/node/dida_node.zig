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

    const graph_builder = allocator.create(dida.GraphBuilder) catch dida.common.panic("OOM", .{});
    graph_builder.* = dida.GraphBuilder.init(allocator);

    // TODO this fails for some reason
    //_ = napiCall(c.napi_wrap, .{ env, args_and_this.this, @ptrCast(*c_void, graph_builder), null, null }, c.napi_ref);
    // TODO do we need to store the wrapper somewhere?
    napiCall(c.napi_wrap, .{ env, args_and_this.this, @ptrCast(*c_void, graph_builder), null, null, null }, void);

    return args_and_this.this;
}

fn GraphBuilder_addSubgraph(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    const args_and_this = napiGetCbInfo(env, info, 1);
    const js_parent_id = args_and_this.args[0];

    const js_parent_id_type = napiCall(c.napi_typeof, .{ env, js_parent_id }, c.napi_valuetype);
    dida.common.assert(js_parent_id_type == .napi_number, "Expected napi_number, found {}", .{js_parent_id_type});
    const parent_id = @intCast(usize, napiCall(c.napi_get_value_int64, .{ env, js_parent_id }, i64));

    const graph_builder = @ptrCast(*dida.GraphBuilder, @alignCast(@alignOf(dida.GraphBuilder), napiCall(c.napi_unwrap, .{ env, args_and_this.this }, ?*c_void)));
    const subgraph = graph_builder.addSubgraph(.{ .id = parent_id }) catch dida.common.panic("OOM", .{});

    const js_id = napiCall(c.napi_create_int64, .{ env, @intCast(i64, subgraph.id) }, c.napi_value);
    return js_id;
}

fn napiCall(comptime napi_fn: anytype, args: anytype, comptime return_type: type) return_type {
    if (return_type != void) {
        var result: return_type = undefined;
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
