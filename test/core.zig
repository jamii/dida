const std = @import("std");
const dida = @import("../lib/dida.zig");

fn expectDeepEqual(actual: anytype, expected: anytype) !void {
    if (!dida.meta.deepEqual(expected, actual)) {
        dida.common.dump(.{ .expected = expected, .actual = actual });
        return error.TestExpectedEqual;
    }
}

fn testTimestampOrder(allocator: *std.mem.Allocator, anon_a: anytype, anon_b: anytype, order: dida.core.PartialOrder) !void {
    const a = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_a);
    const b = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_b);
    try std.testing.expectEqual(a.causalOrder(b), order);
    const reverse_order: dida.core.PartialOrder = switch (order) {
        .none => .none,
        .lt => .gt,
        .eq => .eq,
        .gt => .lt,
    };
    try std.testing.expectEqual(b.causalOrder(a), reverse_order);
    const total_order: std.math.Order = switch (order) {
        .none => return,
        .lt => .lt,
        .eq => .eq,
        .gt => .gt,
    };
    if (order != .none) {
        try std.testing.expectEqual(a.lexicalOrder(b), total_order);
    }
    const lub = try dida.core.Timestamp.leastUpperBound(allocator, a, b);
    try std.testing.expect(a.causalOrder(lub).isLessThanOrEqual());
    try std.testing.expect(b.causalOrder(lub).isLessThanOrEqual());
}

test "timestamp order" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = &arena.allocator;
    try testTimestampOrder(a, .{}, .{}, .eq);
    try testTimestampOrder(a, .{0}, .{0}, .eq);
    try testTimestampOrder(a, .{0}, .{1}, .lt);
    try testTimestampOrder(a, .{1}, .{0}, .gt);
    try testTimestampOrder(a, .{ 0, 0 }, .{ 0, 0 }, .eq);
    try testTimestampOrder(a, .{ 0, 0 }, .{ 1, 0 }, .lt);
    try testTimestampOrder(a, .{ 0, 0 }, .{ 0, 1 }, .lt);
    try testTimestampOrder(a, .{ 0, 0 }, .{ 1, 1 }, .lt);
    try testTimestampOrder(a, .{ 1, 0 }, .{ 0, 1 }, .none);
}

fn testTimestampLub(allocator: *std.mem.Allocator, anon_a: anytype, anon_b: anytype, anon_lub: anytype) !void {
    const a = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_a);
    const b = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_b);
    const expected_lub = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_lub);
    const actual_lub = try dida.core.Timestamp.leastUpperBound(allocator, a, b);
    try std.testing.expectEqualSlices(usize, expected_lub.coords, actual_lub.coords);
}

test "timestamp lub" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = &arena.allocator;
    try testTimestampLub(a, .{}, .{}, .{});
    try testTimestampLub(a, .{0}, .{0}, .{0});
    try testTimestampLub(a, .{0}, .{1}, .{1});
    try testTimestampLub(a, .{1}, .{0}, .{1});
    try testTimestampLub(a, .{ 0, 0 }, .{ 0, 0 }, .{ 0, 0 });
    try testTimestampLub(a, .{ 0, 0 }, .{ 1, 0 }, .{ 1, 0 });
    try testTimestampLub(a, .{ 0, 0 }, .{ 0, 1 }, .{ 0, 1 });
    try testTimestampLub(a, .{ 0, 0 }, .{ 1, 1 }, .{ 1, 1 });
    try testTimestampLub(a, .{ 1, 0 }, .{ 0, 1 }, .{ 1, 1 });
}

fn testChangeBatchBuilder(allocator: *std.mem.Allocator, anon_input_changes: anytype, anon_expected_changes: anytype) !void {
    const input_changes = dida.sugar.coerceAnonToSlice(allocator, dida.core.Change, anon_input_changes);
    const expected_changes = dida.sugar.coerceAnonToSlice(allocator, dida.core.Change, anon_expected_changes);
    var builder = dida.core.ChangeBatchBuilder.init(allocator);
    for (input_changes) |change| {
        try builder.changes.append(change);
    }
    if (try builder.finishAndReset()) |batch| {
        try expectDeepEqual(batch.changes, expected_changes);
        // TODO test batch.lower_bound
    } else {
        const actual_changes: []dida.core.Change = &[0]dida.core.Change{};
        try expectDeepEqual(actual_changes, expected_changes);
    }
}

test "change batch builder" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = &arena.allocator;
    try testChangeBatchBuilder(
        a,
        .{},
        .{},
    );
    try testChangeBatchBuilder(
        a,
        .{
            .{ .{}, .{}, 0 },
        },
        .{},
    );
    try testChangeBatchBuilder(
        a,
        .{
            .{ .{}, .{}, 1 },
        },
        .{
            .{ .{}, .{}, 1 },
        },
    );

    try testChangeBatchBuilder(
        a,
        .{
            .{ .{"a"}, .{}, 1 },
            .{ .{"b"}, .{}, 1 },
            .{ .{"a"}, .{}, -1 },
        },
        .{
            .{ .{"b"}, .{}, 1 },
        },
    );
}
