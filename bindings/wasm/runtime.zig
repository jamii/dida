usingnamespace @import("../js_common.zig");
pub const abi = @import("./abi.zig");

// Requires fixing https://github.com/ziglang/zig/issues/8027
//comptime {
//inline for (exported_functions) |exported_function| {
//const num_args = @typeInfo(@TypeOf(exported_function[1])).Fn.args.len;
//@export(
//abi.handleAbiForFunction(num_args, handleSerdeForFunction(exported_function[1])),
//.{
//.name = exported_function[0],
//.linkage = .Strong,
//},
//);
//}
//}

comptime {
    {
        const name = "GraphBuilder_init";
        const function = GraphBuilder_init;
        const num_args = @typeInfo(@TypeOf(function)).Fn.args.len;
        @export(
            abi.handleAbiForFunction(num_args, handleSerdeForFunction(function)),
            .{
                .name = name,
                .linkage = .Strong,
            },
        );
    }
    {
        const name = "GraphBuilder_addSubgraph";
        const function = dida.core.GraphBuilder.addSubgraph;
        const num_args = @typeInfo(@TypeOf(function)).Fn.args.len;
        @export(
            abi.handleAbiForFunction(num_args, handleSerdeForFunction(function)),
            .{
                .name = name,
                .linkage = .Strong,
            },
        );
    }
}

pub fn panic(message: []const u8, stack_trace: ?*std.builtin.StackTrace) noreturn {
    //TODO Something in the callees of StackTrace.format tries to open stderr, which doesn't compile on wasm
    //var buf = std.ArrayList(u8).init(allocator);
    //var writer = buf.writer();
    //std.fmt.format(writer, "{s}\n\n{s}", .{ message, stack_trace }) catch |_|
    //std.mem.copy(u8, buf.items[buf.items.len - 3 .. buf.items.len], "OOM");
    //abi.js.consoleError(abi.createString({}, buf.items));
    abi.js.consoleError(abi.createString({}, message));
    unreachable;
}
