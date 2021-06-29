const builtin = @import("builtin");
const std = @import("std");

pub fn build(b: *std.build.Builder) !void {
    const mode = b.standardReleaseOptions();

    const runtime = b.addSharedLibrary("dida", "./runtime.zig", .unversioned);
    runtime.setBuildMode(mode);
    runtime.setTarget(.{
        .cpu_arch = .wasm32,
        .os_tag = .freestanding,
    });
    runtime.setMainPkgPath("../../");
    runtime.install();

    const runtime_step = b.step("runtime", "Build runtime (zig-out/lib/dida.wasm)");
    runtime_step.dependOn(&runtime.step);

    const codegen = b.addExecutable("codegen", "./codegen.zig");
    codegen.setMainPkgPath("../../");

    const run_codegen_step = b.step("run-codegen", "Run codegen (to generate zig-out/lib/dida.js)");
    run_codegen_step.dependOn(&codegen.run().step);
}
