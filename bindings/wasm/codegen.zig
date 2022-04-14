const std = @import("std");
const dida = @import("../../lib/dida.zig");
const js_common = @import("../js_common.zig");

pub fn main() !void {
    const file = try std.fs.cwd().createFile("zig-out/lib/dida.js", .{ .read = false, .truncate = true });
    defer file.close();
    var writer = file.writer();
    try writer.writeAll("function Dida(abi) {\n\n");
    inline for (js_common.types_with_js_constructors) |T| {
        try generateConstructor(writer, T);
    }
    try writer.writeAll("\n\n");
    inline for (js_common.types_with_js_constructors) |T| {
        try std.fmt.format(writer, "this.{s} = {s};\n", .{ T, T });
    }
    try writer.writeAll("}");
    // try writer.writeAll("export default Dida");

    const file2 = try std.fs.cwd().createFile("zig-out/lib/dida.mjs", .{ .read = false, .truncate = true });
    defer file2.close();
    writer = file2.writer();
    try writer.writeAll("function Dida(abi) {\n\n");
    inline for (js_common.types_with_js_constructors) |T| {
        try generateConstructor(writer, T);
    }
    try writer.writeAll("\n\n");
    inline for (js_common.types_with_js_constructors) |T| {
        try std.fmt.format(writer, "this.{s} = {s};\n", .{ T, T });
    }
    try writer.writeAll("}\n\n");
    try writer.writeAll("export default Dida");
}

fn generateConstructor(writer: anytype, comptime Type: type) !void {
    const info = @typeInfo(Type);
    switch (comptime js_common.serdeStrategy(Type)) {
        .External => {
            inline for (info.Struct.decls) |decl_info| {
                if (decl_info.is_pub and decl_info.data == .Fn) {
                    const fn_decl_info = decl_info.data.Fn;
                    const fn_info = @typeInfo(fn_decl_info.fn_type).Fn;
                    // First arg is allocator or self
                    const args = fn_info.args[1..];

                    // TODO fn_decl_info.arg_names.len is empty
                    //      See https://github.com/ziglang/zig/issues/8259
                    var arg_names: [args.len][]const u8 = undefined;
                    for (arg_names) |*arg_name, i| {
                        arg_name.* = try dida.util.format(js_common.allocator, "arg{}", .{i});
                    }

                    var arg_pushes: [args.len][]const u8 = undefined;
                    for (arg_pushes) |*arg_push, i| {
                        arg_push.* = try dida.util.format(js_common.allocator, "abi.stackPush(arg{})", .{i});
                    }

                    // NOTE this relies on `init` being the first decl
                    if (comptime std.mem.eql(u8, decl_info.name, "init")) {
                        try std.fmt.format(
                            writer,
                            \\function {s}({s}) {{
                            \\    const init_stack_len = abi.stackGetLength();
                            \\    const result_ix = abi.wasm.instance.exports.{s}_init({s});
                            \\    const result = abi.stackRead(result_ix);
                            \\    abi.stackReset(init_stack_len);
                            \\    this.external = result.external;
                            \\}}
                            \\
                        ,
                            .{
                                Type,
                                std.mem.join(js_common.allocator, ", ", &arg_names),
                                Type,
                                std.mem.join(js_common.allocator, ", ", &arg_pushes),
                            },
                        );
                    } else {
                        try std.fmt.format(
                            writer,
                            \\{s}.prototype.{s} = function {s}({s}) {{
                            \\    const init_stack_len = abi.stackGetLength();
                            \\    const result_ix = abi.wasm.instance.exports.{s}_{s}(abi.stackPush(this), {s});
                            \\    const result = abi.stackRead(result_ix);
                            \\    abi.stackReset(init_stack_len);
                            \\    return result;
                            \\}}
                            \\
                        ,
                            .{
                                Type,
                                decl_info.name,
                                decl_info.name,
                                std.mem.join(js_common.allocator, ", ", &arg_names),
                                Type,
                                decl_info.name,
                                std.mem.join(js_common.allocator, ", ", &arg_pushes),
                            },
                        );
                    }
                }
            }
            try writer.writeAll("\n");
        },
        else => {
            switch (info) {
                .Struct => |struct_info| {
                    try std.fmt.format(writer, "function {s}(", .{Type});
                    inline for (struct_info.fields) |field_info| {
                        try std.fmt.format(writer, "{s}, ", .{field_info.name});
                    }
                    try writer.writeAll(") {\n");
                    inline for (struct_info.fields) |field_info| {
                        try std.fmt.format(writer, "    this.{s} = {s};\n", .{ field_info.name, field_info.name });
                    }
                    try writer.writeAll("}\n\n");
                },
                .Union => |union_info| {
                    if (union_info.tag_type) |_| {
                        // TODO name payload args instead of using `arguments[i]`
                        try std.fmt.format(writer, "const {s} = {{\n", .{Type});
                        inline for (union_info.fields) |field_info| {
                            const payload = switch (field_info.field_type) {
                                []const u8, f64 => "arguments[0]",
                                void => "undefined",
                                else => payload: {
                                    const num_args = @typeInfo(field_info.field_type).Struct.fields.len;
                                    var args: [num_args][]const u8 = undefined;
                                    for (args) |*arg, arg_ix| arg.* = try dida.util.format(js_common.allocator, "arguments[{}]", .{arg_ix});
                                    break :payload try dida.util.format(js_common.allocator, "new {s}({s})", .{
                                        field_info.field_type,
                                        std.mem.join(js_common.allocator, ", ", &args),
                                    });
                                },
                            };
                            try std.fmt.format(
                                writer,
                                \\    {s}: function () {{
                                \\        this.tag = "{s}";
                                \\        this.payload = {s};
                                \\    }},
                                \\
                            ,
                                .{ field_info.name, field_info.name, payload },
                            );
                        }
                        try writer.writeAll("};\n\n");
                    } else {
                        dida.util.compileError("Don't know how to make constructor for non-tagged union type {}", .{Type});
                    }
                },
                else => dida.util.compileError("Don't know how to make constructor for type {}", .{Type}),
            }
        },
    }
}
