pub const std = @import("std");
pub const dida = @import("../lib/dida.zig");

/// root.abi defines functions for interacting with javascript values
const abi = @import("root").abi;

// --- global allocator ---

var gpa = std.heap.GeneralPurposeAllocator(.{
    .safety = true,
    .never_unmap = true,
}){};
pub const allocator = &gpa.allocator;

// --- constructors for External types need to use global allocator ---

pub fn GraphBuilder_init() !dida.core.GraphBuilder {
    return dida.core.GraphBuilder.init(allocator);
}

pub fn Graph_init(node_specs: []const dida.core.NodeSpec, node_immediate_subgraphs: []const dida.core.Subgraph, subgraph_parents: []const dida.core.Subgraph) !dida.core.Graph {
    return dida.core.Graph.init(allocator, node_specs, node_immediate_subgraphs, subgraph_parents);
}

pub fn Shard_init(graph: *const dida.core.Graph) !dida.core.Shard {
    return dida.core.Shard.init(allocator, graph);
}

// --- serde ---

pub const exported_functions = .{
    .{ "GraphBuilder_init", GraphBuilder_init },
    .{ "GraphBuilder_addSubgraph", dida.core.GraphBuilder.addSubgraph },
    .{ "GraphBuilder_addNode", dida.core.GraphBuilder.addNode },
    .{ "GraphBuilder_connectLoop", dida.core.GraphBuilder.connectLoop },
    .{ "GraphBuilder_finishAndReset", dida.core.GraphBuilder.finishAndReset },

    .{ "Graph_init", Graph_init },

    .{ "Shard_init", Shard_init },
    .{ "Shard_pushInput", dida.core.Shard.pushInput },
    .{ "Shard_flushInput", dida.core.Shard.flushInput },
    .{ "Shard_advanceInput", dida.core.Shard.advanceInput },
    .{ "Shard_hasWork", dida.core.Shard.hasWork },
    .{ "Shard_doWork", dida.core.Shard.doWork },
    .{ "Shard_popOutput", dida.core.Shard.popOutput },
};

pub const types_with_js_constructors = .{
    dida.core.GraphBuilder,
    dida.core.Graph,
    dida.core.Shard,

    dida.core.Change,
    dida.core.ChangeBatch,
    dida.core.Subgraph,
    dida.core.Node,
    dida.core.NodeSpec,
    dida.core.NodeSpec.MapSpec,
    dida.core.NodeSpec.JoinSpec,
    dida.core.NodeSpec.TimestampPushSpec,
    dida.core.NodeSpec.TimestampPopSpec,
    dida.core.NodeSpec.TimestampIncrementSpec,
    dida.core.NodeSpec.IndexSpec,
    dida.core.NodeSpec.UnionSpec,
    dida.core.NodeSpec.DistinctSpec,
    dida.core.NodeSpec.ReduceSpec,
    dida.core.NodeSpec.OutputSpec,
};

pub fn hasJsConstructor(comptime T: type) bool {
    for (types_with_js_constructors) |T2| {
        if (T == T2) return true;
    }
    return false;
}

/// How a given type will be (de)serialized across the abi boundary
pub const SerdeStrategy = enum {
    /// Heap-allocate on zig side and give a pointer to js side
    External,
    /// Convert between zig and js values at the callsite
    Value,
};

pub fn serdeStrategy(comptime T: type) SerdeStrategy {
    return switch (T) {
        dida.core.GraphBuilder,
        *dida.core.GraphBuilder,
        dida.core.Graph,
        *dida.core.Graph,
        *const dida.core.Graph,
        dida.core.Shard,
        *dida.core.Shard,
        *const dida.core.Shard,
        => .External,

        void,
        bool,
        usize,
        isize,
        []const usize,
        dida.core.Timestamp,
        dida.core.Frontier,
        f64,
        []const u8,
        dida.core.Value,
        std.meta.TagType(dida.core.Value),
        []const dida.core.Value,
        dida.core.Row,
        dida.core.Change,
        []dida.core.Change,
        dida.core.ChangeBatch,
        ?dida.core.ChangeBatch,
        dida.core.Subgraph,
        []const dida.core.Subgraph,
        dida.core.Node,
        ?dida.core.Node,
        [2]dida.core.Node,
        dida.core.NodeSpec,
        []const dida.core.NodeSpec,
        std.meta.TagType(dida.core.NodeSpec),
        dida.core.NodeSpec.MapSpec,
        *dida.core.NodeSpec.MapSpec.Mapper,
        dida.core.NodeSpec.JoinSpec,
        dida.core.NodeSpec.TimestampPushSpec,
        dida.core.NodeSpec.TimestampPopSpec,
        dida.core.NodeSpec.TimestampIncrementSpec,
        dida.core.NodeSpec.IndexSpec,
        dida.core.NodeSpec.UnionSpec,
        dida.core.NodeSpec.DistinctSpec,
        dida.core.NodeSpec.ReduceSpec,
        *dida.core.NodeSpec.ReduceSpec.Reducer,
        dida.core.NodeSpec.OutputSpec,
        => .Value,

        else => dida.common.compileError("No SerdeStrategy for {}", .{T}),
    };
}

/// Take a zig function and return a wrapped version that handles serde
pub fn handleSerdeForFunction(comptime zig_fn: anytype) fn (abi.Env, []const abi.Value) abi.Value {
    return struct {
        fn wrappedMethod(env: abi.Env, js_args: []const abi.Value) abi.Value {
            var zig_args: std.meta.ArgsTuple(@TypeOf(zig_fn)) = undefined;
            comptime var i: usize = 0;
            inline while (i < zig_args.len) : (i += 1) {
                const js_arg = js_args[i];
                const zig_arg_type = @TypeOf(zig_args[i]);
                zig_args[i] = switch (comptime serdeStrategy(zig_arg_type)) {
                    .External => deserializeExternal(env, js_arg, zig_arg_type),
                    .Value => deserializeValue(env, js_arg, zig_arg_type),
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
                    return serializeExternal(env, result_ptr);
                },
                .Value => {
                    return serializeValue(env, result);
                },
            }
        }
    }.wrappedMethod;
}

// --- private serde stuff ---

const usize_bits = @bitSizeOf(usize);
comptime {
    dida.common.comptimeAssert(
        usize_bits == 32 or usize_bits == 64,
        "Can only handle 32 bit or 64 bit architectures, not {}",
        .{usize_bits},
    );
}

fn serializeExternal(env: abi.Env, value: anytype) abi.Value {
    const info = @typeInfo(@TypeOf(value));
    dida.common.comptimeAssert(
        comptime (serdeStrategy(@TypeOf(value)) == .External),
        "Used serializeExternal on a type that is expected to require serializeValue: {}",
        .{@TypeOf(value)},
    );
    dida.common.comptimeAssert(
        info == .Pointer and info.Pointer.size == .One,
        "serializeExternal should be called with *T, got {}",
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

fn deserializeExternal(env: abi.Env, value: abi.Value, comptime ReturnType: type) ReturnType {
    dida.common.comptimeAssert(
        comptime (serdeStrategy(ReturnType) == .External),
        "Used deserializeExternal on a type that is expected to require deserializeValue: {}",
        .{ReturnType},
    );
    const info = @typeInfo(ReturnType);
    dida.common.comptimeAssert(
        info == .Pointer and info.Pointer.size == .One,
        "deserializeExternal should be called with *T, got {}",
        .{ReturnType},
    );
    const external = abi.getProperty(env, value, abi.createString(env, "external"));
    const ptr = abi.getExternal(env, external);
    return @ptrCast(ReturnType, @alignCast(@alignOf(info.Pointer.child), ptr));
}

fn serializeValue(env: abi.Env, value: anytype) abi.Value {
    dida.common.comptimeAssert(
        comptime (serdeStrategy(@TypeOf(value)) == .Value),
        "Used serializeValue on a type that is expected to require serializeExternal: {}",
        .{@TypeOf(value)},
    );

    if (@TypeOf(value) == []const u8) {
        return abi.createString(env, value);
    }

    if (@TypeOf(value) == dida.core.Value) {
        return switch (value) {
            .String => |string| serializeValue(env, string),
            .Number => |number| serializeValue(env, number),
        };
    }

    if (@TypeOf(value) == dida.core.Row) {
        return serializeValue(env, value.values);
    }

    if (@TypeOf(value) == dida.core.Timestamp) {
        return serializeValue(env, value.coords);
    }

    if (@TypeOf(value) == dida.core.Frontier) {
        const len = value.timestamps.count();
        const js_array = abi.createArray(env, len);
        var iter = value.timestamps.iterator();
        var i: usize = 0;
        while (iter.next()) |entry| {
            const js_timestamp = serializeValue(env, entry.key_ptr.*);
            abi.setElement(env, js_array, @intCast(u32, i), js_timestamp);
            i += 1;
        }
        return js_array;
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
                else => dida.common.compileError("Don't know how to create js value for {}", .{@TypeOf(cast_value)}),
            };
            return @call(.{}, abi_fn, .{ env, cast_value });
        },
        .Float => {
            const abi_fn = switch (@TypeOf(value)) {
                f64 => abi.createFloat64,
                else => dida.common.compileError("Don't know how to create js value for {}", .{@TypeOf(value)}),
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
                const field_value = serializeValue(env, @field(value, field_info.name));
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
                        const js_payload = serializeValue(env, payload);
                        abi.setProperty(env, result, abi.createString(env, "payload"), js_payload);
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
                    const js_array = abi.createArray(env, value.len);
                    for (value) |elem, i| {
                        const js_elem = serializeValue(env, elem);
                        abi.setElement(env, js_array, @intCast(u32, i), js_elem);
                    }
                    return js_array;
                },
                else => dida.common.compileError("Don't know how to create value of type {}", .{@TypeOf(value)}),
            }
        },
        .Optional => {
            return if (value) |payload|
                serializeValue(env, payload)
            else
                abi.getUndefined(env);
        },
        .Void => {
            return abi.getUndefined(env);
        },
        else => dida.common.compileError("Don't know how to create value of type {}", .{@TypeOf(value)}),
    }
}

const JsMapper = struct {
    // TODO is it safe to hold on to this?
    env: abi.Env,
    js_fn_ref: abi.RefCounted,
    mapper: dida.core.NodeSpec.MapSpec.Mapper,

    fn map(self: *dida.core.NodeSpec.MapSpec.Mapper, row: dida.core.Row) error{OutOfMemory}!dida.core.Row {
        const parent = @fieldParentPtr(JsMapper, "mapper", self);
        const js_fn = abi.getRefCounted(parent.env, parent.js_fn_ref);
        const js_args = [_]abi.Value{
            serializeValue(parent.env, row),
        };
        const js_output = abi.callFunction(parent.env, js_fn, &js_args);
        const output = deserializeValue(parent.env, js_output, dida.core.Row);
        return output;
    }
};

const JsReducer = struct {
    // TODO is it safe to hold on to this?
    env: abi.Env,
    js_fn_ref: abi.RefCounted,
    reducer: dida.core.NodeSpec.ReduceSpec.Reducer,

    fn reduce(self: *dida.core.NodeSpec.ReduceSpec.Reducer, reduced_value: dida.core.Value, row: dida.core.Row, count: usize) error{OutOfMemory}!dida.core.Value {
        const parent = @fieldParentPtr(JsReducer, "reducer", self);
        const js_fn = abi.getRefCounted(parent.env, parent.js_fn_ref);
        const js_args = [_]abi.Value{
            serializeValue(parent.env, reduced_value),
            serializeValue(parent.env, row),
            serializeValue(parent.env, count),
        };
        const js_output = abi.callFunction(parent.env, js_fn, &js_args);
        const output = deserializeValue(parent.env, js_output, dida.core.Value);
        return output;
    }
};

fn deserializeValue(env: abi.Env, value: abi.Value, comptime ReturnType: type) ReturnType {
    dida.common.comptimeAssert(
        comptime (serdeStrategy(ReturnType) == .Value),
        "Used deserializeValue on a type that is expected to require deserializeExternal: {}",
        .{ReturnType},
    );

    if (ReturnType == []const u8) {
        return abi.getString(env, value) catch |err| dida.common.panic("{}", .{err});
    }

    if (ReturnType == dida.core.Value) {
        const js_type = abi.jsTypeOf(env, value);
        return switch (js_type) {
            .String => dida.core.Value{ .String = deserializeValue(env, value, []const u8) },
            .Number => dida.core.Value{ .Number = deserializeValue(env, value, f64) },
            else => dida.common.panic("Don't know how to get a dida.core.Value from {}", .{js_type}),
        };
    }

    if (ReturnType == dida.core.Row) {
        return dida.core.Row{ .values = deserializeValue(env, value, []const dida.core.Value) };
    }

    if (ReturnType == dida.core.Timestamp) {
        return dida.core.Timestamp{ .coords = deserializeValue(env, value, []const usize) };
    }

    if (ReturnType == *dida.core.NodeSpec.MapSpec.Mapper) {
        // TODO we're just leaking this for now
        var js_mapper = allocator.create(JsMapper) catch |err| dida.common.panic("{}", .{err});
        js_mapper.* = JsMapper{
            .env = env,
            .js_fn_ref = abi.createRefCounted(env, value, 1),
            .mapper = .{
                .map_fn = JsMapper.map,
            },
        };
        return &js_mapper.mapper;
    }

    if (ReturnType == *dida.core.NodeSpec.ReduceSpec.Reducer) {
        // TODO we're just leaking this for now
        var js_reducer = allocator.create(JsReducer) catch |err| dida.common.panic("{}", .{err});
        js_reducer.* = JsReducer{
            .env = env,
            .js_fn_ref = abi.createRefCounted(env, value, 1),
            .reducer = .{
                .reduce_fn = JsReducer.reduce,
            },
        };
        return &js_reducer.reducer;
    }

    const info = @typeInfo(ReturnType);
    switch (info) {
        .Int => {
            const abi_fn = switch (ReturnType) {
                usize, isize => if (usize_bits == 64) abi.getInt64 else abi.getInt32,
                else => dida.common.compileError("Don't know how to create js value for {}", .{ReturnType}),
            };
            const result = @call(.{}, abi_fn, .{ env, value });
            return @intCast(ReturnType, result);
        },
        .Float => {
            const abi_fn = switch (ReturnType) {
                f64 => abi.getFloat64,
                else => dida.common.compileError("Don't know how to create js value for {}", .{ReturnType}),
            };
            return @call(.{}, abi_fn, .{ env, value });
        },
        .Struct => |struct_info| {
            var result: ReturnType = undefined;
            inline for (struct_info.fields) |field_info| {
                const field_value = abi.getProperty(env, value, abi.createString(env, field_info.name));
                @field(result, field_info.name) = deserializeValue(env, field_value, field_info.field_type);
            }
            return result;
        },
        .Union => |union_info| {
            if (union_info.tag_type) |tag_type| {
                const tag = deserializeValue(env, abi.getProperty(env, value, abi.createString(env, "tag")), tag_type);
                inline for (@typeInfo(tag_type).Enum.fields) |enum_field_info, i| {
                    if (@enumToInt(tag) == enum_field_info.value) {
                        const union_field_info = union_info.fields[i];
                        const payload = deserializeValue(env, abi.getProperty(env, value, abi.createString(env, "payload")), union_field_info.field_type);
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
            const js_len = abi.getArrayLength(env, value);
            dida.common.assert(js_len == array_info.len, "Expected array of length {}, got length {}", .{ array_info.len, js_len });
            var result: ReturnType = undefined;
            for (result) |*elem, i| {
                const js_elem = abi.getElement(env, value, @intCast(u32, i));
                elem.* = deserializeValue(env, js_elem, array_info.child);
            }
            return result;
        },
        .Pointer => |pointer_info| {
            switch (pointer_info.size) {
                .Slice => {
                    const len = abi.getArrayLength(env, value);
                    const result = allocator.alloc(pointer_info.child, len) catch |err| dida.common.panic("{}", .{err});
                    for (result) |*elem, i| {
                        const js_elem = abi.getElement(env, value, @intCast(u32, i));
                        elem.* = deserializeValue(env, js_elem, pointer_info.child);
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
                else => deserializeValue(env, value, optional_info.child),
            };
        },
        .Void => {
            return {};
        },
        else => dida.common.compileError("Don't know how to get value for type {}", .{ReturnType}),
    }
}

// --- abi types ---

pub const JsType = enum {
    Undefined,
    Null,
    Boolean,
    Number,
    String,
    Object,
    Function,
};
