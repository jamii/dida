const builtin = @import("builtin");
const std = @import("std");

pub fn build(b: *std.build.Builder) !void {
    const mode = b.standardReleaseOptions();

    const runtime = b.addSharedLibrary("debugger", "./debugger.zig", .unversioned);
    runtime.setBuildMode(mode);
    runtime.setTarget(.{
        .cpu_arch = .wasm32,
        .os_tag = .freestanding,
    });
    runtime.setMainPkgPath("../");
    runtime.install();

    const runtime_step = b.step("runtime", "Build runtime (zig-out/lib/debugger.wasm)");
    runtime_step.dependOn(&runtime.step);
}
