usingnamespace @import("../js_common.zig");
pub const abi = @import("./abi.zig");

comptime {
    for (exported_functions) |exported_function| {
        const num_args = @typeInfo(@TypeOf(exported_function[1])).Fn.args.len;
        if (std.mem.eql(u8, exported_function[0], "GraphBuilder_init"))
            @export(
                abi.handleAbiForFunction(num_args, handleSerdeForFunction(exported_function[1])),
                .{
                    .name = exported_function[0],
                    .linkage = .Strong,
                },
            );
    }
}
