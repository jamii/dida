const std = @import("std");
const dida = @import("../../core/dida.zig");

var gpa = std.heap.GeneralPurposeAllocator(.{
    .safety = true,
    .never_unmap = true,
}){};
const allocator = &gpa.allocator;

// TODO eventually generate this from a list of exported functions
const value_types = .{
    dida.Timestamp,
    dida.Value,
    dida.Row,
    dida.Change,
    dida.ChangeBatch,
    dida.Node,
    dida.NodeSpec,
    dida.Subgraph,
};

pub fn main() !void {
    var writer = std.io.getStdOut().writer();
    var already_generated = dida.common.DeepHashSet(TypeId).init(allocator);
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
    switch (info) {
        .Int, .Float => {},
        .Pointer => |pointer_info| {
            try generate_constructor(writer, already_generated, pointer_info.child);
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
        else => {
            try std.fmt.format(writer, "{} = undefined;\n\n", .{@typeName(JsType)});
        },
    }
}
