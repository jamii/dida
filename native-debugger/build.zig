const std = @import("std");
const zt = @import("ZT/build.zig");

pub fn build(b: *std.build.Builder) void {
    const target = b.standardTargetOptions(.{});
    const mode = b.standardReleaseOptions();

    const exe = b.addExecutable("build", "./main.zig");
    zt.link(b, exe, target);
    exe.setTarget(target);
    exe.setBuildMode(mode);
    exe.setMainPkgPath("../");
    exe.install();

    // Run cmd
    const run_cmd = exe.run();
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);
}
