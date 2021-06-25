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
    {
        const name = "GraphBuilder_addNode";
        const function = dida.core.GraphBuilder.addNode;
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
        const name = "GraphBuilder_connectLoop";
        const function = dida.core.GraphBuilder.connectLoop;
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
        const name = "GraphBuilder_finishAndReset";
        const function = dida.core.GraphBuilder.finishAndReset;
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
        const name = "Graph_init";
        const function = Graph_init;
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
        const name = "Shard_init";
        const function = Shard_init;
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
        const name = "Shard_pushInput";
        const function = dida.core.Shard.pushInput;
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
        const name = "Shard_flushInput";
        const function = dida.core.Shard.flushInput;
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
        const name = "Shard_advanceInput";
        const function = dida.core.Shard.advanceInput;
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
        const name = "Shard_hasWork";
        const function = dida.core.Shard.hasWork;
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
        const name = "Shard_doWork";
        const function = dida.core.Shard.doWork;
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
        const name = "Shard_popOutput";
        const function = dida.core.Shard.popOutput;
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
    //TODO Something in the call tree of StackTrace.format tries to open stderr, which doesn't compile on wasm
    //var buf = std.ArrayList(u8).init(allocator);
    //var writer = buf.writer();
    //std.fmt.format(writer, "{s}\n\n{s}", .{ message, stack_trace }) catch |_|
    //std.mem.copy(u8, buf.items[buf.items.len - 3 .. buf.items.len], "OOM");
    //const js_message = abi.createString({}, buf.items);

    const js_message = abi.createString({}, message);
    // Use consoleError to get a js stack trace
    abi.js.consoleError(js_message);
    // Throw an exception to stop execution (and to get a breakpoint in the debugger)
    abi.js.throwException(js_message);
}
