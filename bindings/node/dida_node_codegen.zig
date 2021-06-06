const std = @import("std");
const dida = @import("../../core/dida.zig");

var gpa = std.heap.GeneralPurposeAllocator(.{
    .safety = true,
    .never_unmap = true,
}){};
const allocator = &gpa.allocator;

// TODO eventually generate this from a list of exported functions
const value_types = .{
    dida.GraphBuilder,
    dida.Shard,
};

pub fn main() !void {
    var writer = std.io.getStdOut().writer();
    try writer.writeAll("const dida = require('./dida.node');\n\n");
    var already_generated = dida.common.DeepHashSet(TypeId).init(allocator);

    // Don't generate wrappers for these
    inline for (.{ dida.Row, dida.Value }) |T| {
        try already_generated.put(typeId(T), {});
    }

    inline for (value_types) |T| {
        try generate_constructor(writer, &already_generated, T);
    }
}

// Beautiful hack from https://github.com/ziglang/zig/issues/5459
const TypeId = struct {
    id: usize,
};
fn typeId(comptime T: type) TypeId {
    return .{
        .id = @ptrToInt(&struct {
            var x: u8 = 0;
        }.x),
    };
}

fn ToJsType(comptime ZigType: type) type {
    return switch (ZigType) {
        dida.Frontier => Frontier,
        else => ZigType,
    };
}

pub const Frontier = struct {
    timestamps: []dida.Timestamp,
};

fn generate_constructor(writer: anytype, already_generated: *dida.common.DeepHashSet(TypeId), comptime ZigType: type) !void {
    const JsType = ToJsType(ZigType);

    if (already_generated.contains(typeId(JsType))) return;
    try already_generated.put(typeId(JsType), {});

    const info = @typeInfo(JsType);
    switch (JsType) {
        dida.GraphBuilder, dida.Shard => {
            inline for (info.Struct.decls) |decl_info| {
                if (decl_info.is_pub and decl_info.data == .Fn) {
                    if (std.mem.eql(u8, decl_info.name, "init")) {
                        const fn_decl_info = decl_info.data.Fn;
                        const fn_info = @typeInfo(fn_decl_info.fn_type).Fn;
                        // First arg is allocator
                        const args = fn_info.args[1..];

                        // TODO fn_decl_info.arg_names.len is empty
                        //      See https://github.com/ziglang/zig/issues/8259
                        var arg_names: [args.len][]const u8 = undefined;
                        for (arg_names) |*arg_name, i| {
                            arg_name.* = try dida.common.format(allocator, "arg{}", .{i});
                        }

                        try std.fmt.format(
                            writer,
                            \\function {}({}) {{
                            \\    this.external = dida.{}_init({}).external;
                            \\}}
                            \\
                        ,
                            .{
                                @typeName(JsType),
                                try std.mem.join(allocator, ", ", &arg_names),
                                @typeName(JsType),
                                try std.mem.join(allocator, ", ", &arg_names),
                            },
                        );
                    }
                }
            }
            inline for (info.Struct.decls) |decl_info| {
                if (decl_info.is_pub and decl_info.data == .Fn) {
                    // TODO not sure why this condition needs to be separated out to satisfy compiler
                    if (!std.mem.eql(u8, decl_info.name, "init")) {
                        const fn_decl_info = decl_info.data.Fn;
                        const fn_info = @typeInfo(fn_decl_info.fn_type).Fn;
                        // First arg is self
                        const args = fn_info.args[1..];

                        // TODO fn_decl_info.arg_names.len is empty
                        //      See https://github.com/ziglang/zig/issues/8259
                        var arg_names: [args.len][]const u8 = undefined;
                        for (arg_names) |*arg_name, i| {
                            arg_name.* = try dida.common.format(allocator, "arg{}", .{i});
                        }

                        try std.fmt.format(
                            writer,
                            \\{}.prototype.{} = function {}({}) {{
                            \\    return dida.{}_{}(this, {});
                            \\}}
                            \\
                        ,
                            .{
                                @typeName(JsType),
                                decl_info.name,
                                decl_info.name,
                                try std.mem.join(allocator, ", ", &arg_names),
                                @typeName(JsType),
                                decl_info.name,
                                try std.mem.join(allocator, ", ", &arg_names),
                            },
                        );

                        inline for (args) |arg_info| {
                            try generate_constructor(writer, already_generated, arg_info.arg_type.?);
                        }
                    }
                }
            }
            try writer.writeAll("\n");
        },
        else => {
            switch (info) {
                .Int, .Float => {},
                .Pointer => |pointer_info| {
                    try generate_constructor(writer, already_generated, pointer_info.child);
                },
                .Array => |array_info| {
                    try generate_constructor(writer, already_generated, array_info.child);
                },
                .Optional => |array_info| {
                    try generate_constructor(writer, already_generated, array_info.child);
                },
                .Struct => |struct_info| {
                    try std.fmt.format(writer, "function {}(", .{@typeName(JsType)});
                    inline for (struct_info.fields) |field_info| {
                        try std.fmt.format(writer, "{}, ", .{field_info.name});
                    }
                    try writer.writeAll(") {\n");
                    inline for (struct_info.fields) |field_info| {
                        try std.fmt.format(writer, "    this.{} = {};\n", .{ field_info.name, field_info.name });
                    }
                    try writer.writeAll("}\n\n");
                    inline for (struct_info.fields) |field_info| {
                        try generate_constructor(writer, already_generated, field_info.field_type);
                    }
                },
                .Union => |union_info| {
                    if (union_info.tag_type) |tag_type| {
                        // TODO name payload args instead of using `arguments[i]`
                        try std.fmt.format(writer, "const {} = {{\n", .{@typeName(JsType)});
                        inline for (union_info.fields) |field_info| {
                            const payload = switch (field_info.field_type) {
                                []const u8, f64 => "arguments[0]",
                                void => "undefined",
                                else => payload: {
                                    const num_args = @typeInfo(field_info.field_type).Struct.fields.len;
                                    var args: [num_args][]const u8 = undefined;
                                    for (args) |*arg, arg_ix| arg.* = try dida.common.format(allocator, "arguments[{}]", .{arg_ix});
                                    break :payload try dida.common.format(allocator, "new {}({})", .{ @typeName(field_info.field_type), try std.mem.join(allocator, ", ", &args) });
                                },
                            };
                            try std.fmt.format(
                                writer,
                                \\    {}: function () {{
                                \\        this.tag = "{}";
                                \\        this.payload = {};
                                \\    }},
                                \\
                            ,
                                .{ field_info.name, field_info.name, payload },
                            );
                        }
                        try writer.writeAll("};\n\n");
                        inline for (union_info.fields) |field_info| {
                            try generate_constructor(writer, already_generated, field_info.field_type);
                        }
                    } else {
                        @compileError("Can't handle non-tagged union type " ++ @typeName(JsType));
                    }
                },
                .Void, .Fn => {
                    // TODO maybe should look at arg/return types for Fn?
                },
                else => @compileError("Can't handle type " ++ @typeName(JsType)),
            }
        },
    }
}
