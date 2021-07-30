usingnamespace @import("../bindings/js_common.zig");
pub const abi = @import("../bindings/wasm/abi.zig");

fn frame() []const u8 {
    return "foo";
}

comptime {
    {
        const name = "frame";
        const function = frame;
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
