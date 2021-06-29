const builtin = @import("builtin");
const std = @import("std");

const allocator = std.heap.page_allocator;

pub fn build(b: *std.build.Builder) !void {
    const mode = b.standardReleaseOptions();

    const runtime = b.addSharedLibrary("dida", "./runtime.zig", .unversioned);
    runtime.setBuildMode(mode);
    runtime.setMainPkgPath("../../");
    runtime.linkLibC();
    var buffer = std.ArrayList(u8).init(allocator);
    try std.fmt.format(buffer.writer(), "{s}/include/node", .{std.os.getenv("NIX_NODEJS").?});
    runtime.addIncludeDir(buffer.items);
    runtime.install();
    b.installLibFile("zig-out/lib/libdida.so", "dida.node");

    const runtime_step = b.step("runtime", "Build runtime (zig-out/lib/dida.o TODO)");
    runtime_step.dependOn(&runtime.step);

    const codegen = b.addExecutable("codegen", "./codegen.zig");
    codegen.setMainPkgPath("../../");

    const run_codegen_step = b.step("run-codegen", "Run codegen (to generate zig-out/lib/dida.js)");
    run_codegen_step.dependOn(&codegen.run().step);
}
