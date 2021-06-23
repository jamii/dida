usingnamespace @import("../js_common/js_common.zig");
pub const abi = @import("./abi.zig");

// TODO remove once abi is complete
const c = abi.c;

export fn napi_register_module_v1(env: abi.Env, exports: abi.Value) abi.Value {
    napiExportFn(env, exports, napiWrapFn(GraphBuilder_init), "GraphBuilder_init");
    napiExportFn(env, exports, napiWrapFn(dida.core.GraphBuilder.addSubgraph), "GraphBuilder_addSubgraph");
    napiExportFn(env, exports, napiWrapFn(dida.core.GraphBuilder.addNode), "GraphBuilder_addNode");
    napiExportFn(env, exports, napiWrapFn(dida.core.GraphBuilder.connectLoop), "GraphBuilder_connectLoop");
    napiExportFn(env, exports, napiWrapFn(dida.core.GraphBuilder.finishAndReset), "GraphBuilder_finishAndReset");

    napiExportFn(env, exports, napiWrapFn(Graph_init), "Graph_init");

    napiExportFn(env, exports, napiWrapFn(Shard_init), "Shard_init");
    napiExportFn(env, exports, napiWrapFn(dida.core.Shard.pushInput), "Shard_pushInput");
    napiExportFn(env, exports, napiWrapFn(dida.core.Shard.flushInput), "Shard_flushInput");
    napiExportFn(env, exports, napiWrapFn(dida.core.Shard.advanceInput), "Shard_advanceInput");
    napiExportFn(env, exports, napiWrapFn(dida.core.Shard.hasWork), "Shard_hasWork");
    napiExportFn(env, exports, napiWrapFn(dida.core.Shard.doWork), "Shard_doWork");
    napiExportFn(env, exports, napiWrapFn(dida.core.Shard.popOutput), "Shard_popOutput");

    return exports;
}

fn GraphBuilder_init() !dida.core.GraphBuilder {
    return dida.core.GraphBuilder.init(allocator);
}

fn Graph_init(node_specs: []const dida.core.NodeSpec, node_immediate_subgraphs: []const dida.core.Subgraph, subgraph_parents: []const dida.core.Subgraph) !dida.core.Graph {
    return dida.core.Graph.init(allocator, node_specs, node_immediate_subgraphs, subgraph_parents);
}

fn Shard_init(graph: *const dida.core.Graph) !dida.core.Shard {
    return dida.core.Shard.init(allocator, graph);
}

// --- helpers ---

fn napiExportFn(env: abi.Env, exports: abi.Value, export_fn: c.napi_callback, export_fn_name: [:0]const u8) void {
    const napi_fn = abi.napiCall(c.napi_create_function, .{ env, export_fn_name, export_fn_name.len, export_fn, null }, abi.Value);
    abi.setProperty(env, exports, abi.createString(env, export_fn_name), napi_fn);
}

fn napiWrapFn(comptime zig_fn: anytype) c.napi_callback {
    return struct {
        fn wrappedMethod(env: abi.Env, info: c.napi_callback_info) callconv(.C) abi.Value {
            var zig_args: std.meta.ArgsTuple(@TypeOf(zig_fn)) = undefined;
            const napi_args = napiGetCbInfo(env, info, zig_args.len).args;
            comptime var i: usize = 0;
            inline while (i < zig_args.len) : (i += 1) {
                const napi_arg = napi_args[i];
                const zig_arg_type = @TypeOf(zig_args[i]);
                zig_args[i] = switch (comptime serdeStrategy(zig_arg_type)) {
                    .External => napiGetExternal(env, napi_arg, zig_arg_type),
                    .Value => napiGetValue(env, napi_arg, zig_arg_type),
                };
            }
            const result_or_err = @call(.{}, zig_fn, zig_args);
            const result = if (@typeInfo(@TypeOf(result_or_err)) == .ErrorUnion)
                result_or_err catch |err| dida.common.panic("{}", .{err})
            else
                result_or_err;
            switch (comptime serdeStrategy(@TypeOf(result))) {
                .External => {
                    const result_ptr = allocator.create(@TypeOf(result)) catch |err| dida.common.panic("{}", .{err});
                    result_ptr.* = result;
                    return napiCreateExternal(env, result_ptr);
                },
                .Value => {
                    return napiCreateValue(env, result);
                },
            }
        }
    }.wrappedMethod;
}

fn napiGetCbInfo(env: abi.Env, info: c.napi_callback_info, comptime expected_num_args: usize) struct { args: [expected_num_args]abi.Value, this: abi.Value } {
    var actual_num_args: usize = expected_num_args;
    var args: [expected_num_args]abi.Value = undefined;
    var this: abi.Value = undefined;
    abi.napiCall(c.napi_get_cb_info, .{ env, info, &actual_num_args, if (expected_num_args == 0) null else @ptrCast([*]abi.Value, &args), &this, null }, void);
    dida.common.assert(actual_num_args == expected_num_args, "Expected {} args, got {} args", .{ expected_num_args, actual_num_args });
    return .{
        .args = args,
        .this = this,
    };
}

const usize_bits = @bitSizeOf(usize);
comptime {
    dida.common.comptimeAssert(
        usize_bits == 32 or usize_bits == 64,
        "Can only handle 32 bit or 64 bit architectures, not {}",
        .{usize_bits},
    );
}

fn napiCreateExternal(env: abi.Env, value: anytype) abi.Value {
    const info = @typeInfo(@TypeOf(value));
    dida.common.comptimeAssert(
        comptime (serdeStrategy(@TypeOf(value)) == .External),
        "Used napiCreateExternal on a type that is expected to require napiCreateValue: {}",
        .{@TypeOf(value)},
    );
    dida.common.comptimeAssert(
        info == .Pointer and info.Pointer.size == .One,
        "napiCreateExternal should be called with *T, got {}",
        .{@TypeOf(value)},
    );
    dida.common.comptimeAssert(
        comptime hasJsConstructor(info.Pointer.child),
        "Tried to create an external for a type that doesn't have a matching js constructor: {}",
        .{@TypeOf(value)},
    );
    const external = abi.createExternal(env, @ptrCast(*c_void, value));
    const result = abi.createObject(env);
    abi.setProperty(env, result, abi.createString(env, "external"), external);
    return result;
}

fn napiGetExternal(env: abi.Env, value: abi.Value, comptime ReturnType: type) ReturnType {
    dida.common.comptimeAssert(
        comptime (serdeStrategy(ReturnType) == .External),
        "Used napiGetExternal on a type that is expected to require napiGetValue: {}",
        .{ReturnType},
    );
    const info = @typeInfo(ReturnType);
    dida.common.comptimeAssert(
        info == .Pointer and info.Pointer.size == .One,
        "napiGetExternal should be called with *T, got {}",
        .{ReturnType},
    );
    const external = abi.getProperty(env, value, abi.createString(env, "external"));
    const ptr = abi.getExternal(env, external);
    return @ptrCast(ReturnType, @alignCast(@alignOf(info.Pointer.child), ptr));
}

fn napiCreateValue(env: abi.Env, value: anytype) abi.Value {
    dida.common.comptimeAssert(
        comptime (serdeStrategy(@TypeOf(value)) == .Value),
        "Used napiCreateValue on a type that is expected to require napiCreateExternal: {}",
        .{@TypeOf(value)},
    );

    if (@TypeOf(value) == []const u8) {
        return abi.createString(env, value);
    }

    if (@TypeOf(value) == dida.core.Value) {
        return switch (value) {
            .String => |string| napiCreateValue(env, string),
            .Number => |number| napiCreateValue(env, number),
        };
    }

    if (@TypeOf(value) == dida.core.Row) {
        return napiCreateValue(env, value.values);
    }

    if (@TypeOf(value) == dida.core.Timestamp) {
        return napiCreateValue(env, value.coords);
    }

    if (@TypeOf(value) == dida.core.Frontier) {
        const len = value.timestamps.count();
        const napi_array = abi.createArray(env, len);
        var iter = value.timestamps.iterator();
        var i: usize = 0;
        while (iter.next()) |entry| {
            const napi_timestamp = napiCreateValue(env, entry.key_ptr.*);
            abi.setElement(env, napi_array, @intCast(u32, i), napi_timestamp);
            i += 1;
        }
        return napi_array;
    }

    const info = @typeInfo(@TypeOf(value));
    switch (info) {
        .Bool => {
            return abi.createBoolean(env, value);
        },
        .Int => {
            const cast_value = switch (@TypeOf(value)) {
                usize => @intCast(isize, value),
                else => value,
            };
            const abi_fn = switch (@TypeOf(cast_value)) {
                isize => if (usize_bits == 64) abi.createInt64 else abi.createInt32,
                else => dida.common.compileError("Don't know how to create napi value for {}", .{@TypeOf(cast_value)}),
            };
            return @call(.{}, abi_fn, .{ env, cast_value });
        },
        .Float => {
            const abi_fn = switch (@TypeOf(value)) {
                f64 => abi.createFloat64,
                else => dida.common.compileError("Don't know how to create napi value for {}", .{@TypeOf(value)}),
            };
            return @call(.{}, abi_fn, .{ env, value });
        },
        .Struct => |struct_info| {
            dida.common.comptimeAssert(
                comptime hasJsConstructor(@TypeOf(value)),
                "Tried to create a value for a struct type that doesn't have a matching js constructor: {}",
                .{@TypeOf(value)},
            );
            const result = abi.createObject(env);
            inline for (struct_info.fields) |field_info| {
                const field_value = napiCreateValue(env, @field(value, field_info.name));
                abi.setProperty(env, result, abi.createString(env, field_info.name), field_value);
            }
            return result;
        },
        .Union => |union_info| {
            if (union_info.tag_type) |tag_type| {
                dida.common.comptimeAssert(
                    comptime hasJsConstructor(@TypeOf(value)),
                    "Tried to create a value for a union type that doesn't have a matching js constructor: {}",
                    .{@TypeOf(value)},
                );
                const result = abi.createObject(env);
                const tag = std.meta.activeTag(value);
                abi.setProperty(env, result, abi.createString(env, "tag"), abi.createString(env, @tagName(tag)));
                inline for (@typeInfo(tag_type).Enum.fields) |enum_field_info, i| {
                    if (@enumToInt(tag) == enum_field_info.value) {
                        const payload = @field(value, enum_field_info.name);
                        const napi_payload = napiCreateValue(env, payload);
                        abi.setProperty(env, result, abi.createString(env, "payload"), napi_payload);
                        return result;
                    }
                }
                unreachable;
            } else {
                dida.common.compileError("Can't create value for untagged union type {}", .{ReturnType});
            }
        },
        .Enum => |enum_info| {
            comptime var max_len: usize = 0;
            inline for (enum_info.fields) |field_info| {
                comptime {
                    max_len = dida.common.max(max_len, field_info.name.len);
                }
            }
            // max_len+1 to make space for null byte
            var buffer: [max_len + 1]u8 = undefined;
            const tag_name = abi.getStringInto(env, value, &buffer);
            inline for (enum_info.fields) |field_info| {
                if (std.mem.eql(u8, tag_name, field_info.name)) {
                    return @intToEnum(ReturnType, field_info.value);
                }
            }
            dida.common.panic("Type {s} does not contain a tag named \"{s}\"", .{ ReturnType, tag_name });
        },
        .Pointer => |pointer_info| {
            switch (pointer_info.size) {
                .Slice => {
                    const napi_array = abi.createArray(env, value.len);
                    for (value) |elem, i| {
                        const napi_elem = napiCreateValue(env, elem);
                        abi.setElement(env, napi_array, @intCast(u32, i), napi_elem);
                    }
                    return napi_array;
                },
                else => dida.common.compileError("Don't know how to create value of type {}", .{@TypeOf(value)}),
            }
        },
        .Optional => {
            return if (value) |payload|
                napiCreateValue(env, payload)
            else
                abi.getUndefined(env);
        },
        .Void => {
            return abi.getUndefined(env);
        },
        else => dida.common.compileError("Don't know how to create value of type {}", .{@TypeOf(value)}),
    }
}

const NapiMapper = struct {
    // TODO is it safe to hold on to this?
    env: abi.Env,
    napi_fn_ref: c.napi_ref,
    mapper: dida.core.NodeSpec.MapSpec.Mapper,

    fn map(self: *dida.core.NodeSpec.MapSpec.Mapper, row: dida.core.Row) error{OutOfMemory}!dida.core.Row {
        const parent = @fieldParentPtr(NapiMapper, "mapper", self);
        const napi_fn = abi.getRefCounted(parent.env, parent.napi_fn_ref);
        const napi_args = [_]abi.Value{
            napiCreateValue(parent.env, row),
        };
        dida.common.assert(!abi.napiCall(c.napi_is_exception_pending, .{parent.env}, bool), "Shouldn't be any exceptions before mapper cal", .{});
        const napi_undefined = abi.getUndefined(parent.env);
        const napi_output = abi.napiCall(c.napi_call_function, .{ parent.env, napi_undefined, napi_fn, napi_args.len, &napi_args }, abi.Value);
        dida.common.assert(!abi.napiCall(c.napi_is_exception_pending, .{parent.env}, bool), "Shouldn't be any exceptions after mapper call", .{});
        const output = napiGetValue(parent.env, napi_output, dida.core.Row);
        return output;
    }
};

const NapiReducer = struct {
    // TODO is it safe to hold on to this?
    env: abi.Env,
    napi_fn_ref: c.napi_ref,
    reducer: dida.core.NodeSpec.ReduceSpec.Reducer,

    fn reduce(self: *dida.core.NodeSpec.ReduceSpec.Reducer, reduced_value: dida.core.Value, row: dida.core.Row, count: usize) error{OutOfMemory}!dida.core.Value {
        const parent = @fieldParentPtr(NapiReducer, "reducer", self);
        const napi_fn = abi.getRefCounted(parent.env, parent.napi_fn_ref);
        const napi_args = [_]abi.Value{
            napiCreateValue(parent.env, reduced_value),
            napiCreateValue(parent.env, row),
            napiCreateValue(parent.env, count),
        };
        dida.common.assert(!abi.napiCall(c.napi_is_exception_pending, .{parent.env}, bool), "Shouldn't be any exceptions before mapper cal", .{});
        const napi_undefined = abi.getUndefined(parent.env);
        const napi_output = abi.napiCall(c.napi_call_function, .{ parent.env, napi_undefined, napi_fn, napi_args.len, &napi_args }, abi.Value);
        dida.common.assert(!abi.napiCall(c.napi_is_exception_pending, .{parent.env}, bool), "Shouldn't be any exceptions after mapper call", .{});
        const output = napiGetValue(parent.env, napi_output, dida.core.Value);
        return output;
    }
};

fn napiGetValue(env: abi.Env, value: abi.Value, comptime ReturnType: type) ReturnType {
    dida.common.comptimeAssert(
        comptime (serdeStrategy(ReturnType) == .Value),
        "Used napiGetValue on a type that is expected to require napiGetExternal: {}",
        .{ReturnType},
    );

    if (ReturnType == []const u8) {
        return abi.getString(env, value) catch |err| dida.common.panic("{}", .{err});
    }

    if (ReturnType == dida.core.Value) {
        const js_type = abi.jsTypeOf(env, value);
        return switch (js_type) {
            .String => dida.core.Value{ .String = napiGetValue(env, value, []const u8) },
            .Number => dida.core.Value{ .Number = napiGetValue(env, value, f64) },
            else => dida.common.panic("Don't know how to get a dida.core.Value from {}", .{js_type}),
        };
    }

    if (ReturnType == dida.core.Row) {
        return dida.core.Row{ .values = napiGetValue(env, value, []const dida.core.Value) };
    }

    if (ReturnType == dida.core.Timestamp) {
        return dida.core.Timestamp{ .coords = napiGetValue(env, value, []const usize) };
    }

    if (ReturnType == *dida.core.NodeSpec.MapSpec.Mapper) {
        // TODO we're just leaking this for now
        var napi_mapper = allocator.create(NapiMapper) catch |err| dida.common.panic("{}", .{err});
        napi_mapper.* = NapiMapper{
            .env = env,
            .napi_fn_ref = abi.createRefCounted(env, value, 1),
            .mapper = .{
                .map_fn = NapiMapper.map,
            },
        };
        return &napi_mapper.mapper;
    }

    if (ReturnType == *dida.core.NodeSpec.ReduceSpec.Reducer) {
        // TODO we're just leaking this for now
        var napi_reducer = allocator.create(NapiReducer) catch |err| dida.common.panic("{}", .{err});
        napi_reducer.* = NapiReducer{
            .env = env,
            .napi_fn_ref = abi.createRefCounted(env, value, 1),
            .reducer = .{
                .reduce_fn = NapiReducer.reduce,
            },
        };
        return &napi_reducer.reducer;
    }

    const info = @typeInfo(ReturnType);
    switch (info) {
        .Int => {
            const abi_fn = switch (ReturnType) {
                usize, isize => if (usize_bits == 64) abi.getInt64 else abi.getInt32,
                else => dida.common.compileError("Don't know how to create napi value for {}", .{ReturnType}),
            };
            const result = @call(.{}, abi_fn, .{ env, value });
            return @intCast(ReturnType, result);
        },
        .Float => {
            const abi_fn = switch (ReturnType) {
                f64 => abi.getFloat64,
                else => dida.common.compileError("Don't know how to create napi value for {}", .{ReturnType}),
            };
            return @call(.{}, abi_fn, .{ env, value });
        },
        .Struct => |struct_info| {
            var result: ReturnType = undefined;
            inline for (struct_info.fields) |field_info| {
                const field_value = abi.getProperty(env, value, abi.createString(env, field_info.name));
                @field(result, field_info.name) = napiGetValue(env, field_value, field_info.field_type);
            }
            return result;
        },
        .Union => |union_info| {
            if (union_info.tag_type) |tag_type| {
                const tag = napiGetValue(env, abi.getProperty(env, value, abi.createString(env, "tag")), tag_type);
                inline for (@typeInfo(tag_type).Enum.fields) |enum_field_info, i| {
                    if (@enumToInt(tag) == enum_field_info.value) {
                        const union_field_info = union_info.fields[i];
                        const payload = napiGetValue(env, abi.getProperty(env, value, abi.createString(env, "payload")), union_field_info.field_type);
                        return @unionInit(ReturnType, union_field_info.name, payload);
                    }
                }
                unreachable;
            } else {
                dida.common.compileError("Can't get value for untagged union type {}", .{ReturnType});
            }
        },
        .Enum => |enum_info| {
            comptime var max_len: usize = 0;
            inline for (enum_info.fields) |field_info| {
                comptime {
                    max_len = dida.common.max(max_len, field_info.name.len);
                }
            }
            // max_len+1 to make space for null byte :(
            var buffer: [max_len + 1]u8 = undefined;
            const tag_name = abi.getStringInto(env, value, &buffer);
            inline for (enum_info.fields) |field_info| {
                if (std.mem.eql(u8, tag_name, field_info.name)) {
                    return @intToEnum(ReturnType, field_info.value);
                }
            }
            dida.common.panic("Type {s} does not contain a tag named \"{s}\"", .{ ReturnType, tag_name });
        },
        .Array => |array_info| {
            const napi_len = abi.getArrayLength(env, value);
            dida.common.assert(napi_len == array_info.len, "Expected array of length {}, got length {}", .{ array_info.len, napi_len });
            var result: ReturnType = undefined;
            for (result) |*elem, i| {
                const napi_elem = abi.getElement(env, value, @intCast(u32, i));
                elem.* = napiGetValue(env, napi_elem, array_info.child);
            }
            return result;
        },
        .Pointer => |pointer_info| {
            switch (pointer_info.size) {
                .Slice => {
                    const len = abi.getArrayLength(env, value);
                    const result = allocator.alloc(pointer_info.child, len) catch |err| dida.common.panic("{}", .{err});
                    for (result) |*elem, i| {
                        const napi_elem = abi.getElement(env, value, @intCast(u32, i));
                        elem.* = napiGetValue(env, napi_elem, pointer_info.child);
                    }
                    return result;
                },
                else => dida.common.compileError("Don't know how to get value for type {}", .{ReturnType}),
            }
        },
        .Optional => |optional_info| {
            const js_type = abi.jsTypeOf(env, value);
            return switch (js_type) {
                .Undefined, .Null => null,
                else => napiGetValue(env, value, optional_info.child),
            };
        },
        .Void => {
            return {};
        },
        else => dida.common.compileError("Don't know how to get value for type {}", .{ReturnType}),
    }
}
