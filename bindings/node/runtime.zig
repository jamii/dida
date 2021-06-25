usingnamespace @import("../js_common.zig");
pub const abi = @import("./abi.zig");

export fn napi_register_module_v1(env: abi.Env, exports: abi.Value) abi.Value {
    inline for (exported_functions) |exported_function| {
        const num_args = @typeInfo(@TypeOf(exported_function[1])).Fn.args.len;
        abi.setProperty(
            env,
            exports,
            abi.createString(env, exported_function[0]),
            abi.createFunction(env, exported_function[0], num_args, comptime handleSerdeForFunction(exported_function[1])),
        );
    }
    return exports;
}
