const std = @import("std");
const dida = @import("../lib/dida.zig");

fn expectDeepEqual(actual: anytype, expected: anytype) !void {
    if (!dida.meta.deepEqual(expected, actual)) {
        dida.common.dump(.{ .expected = expected, .actual = actual });
        return error.TestExpectedEqual;
    }
}

fn testTimestampOrder(
    allocator: *std.mem.Allocator,
    anon_a: anytype,
    anon_b: anytype,
    order: dida.core.PartialOrder,
) !void {
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

fn testTimestampLub(
    allocator: *std.mem.Allocator,
    anon_a: anytype,
    anon_b: anytype,
    anon_lub: anytype,
) !void {
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

fn testChangeBatchBuilder(
    allocator: *std.mem.Allocator,
    anon_input_changes: anytype,
    anon_expected_changes: anytype,
) !void {
    const input_changes = dida.sugar.coerceAnonToSlice(allocator, dida.core.Change, anon_input_changes);
    const expected_changes = dida.sugar.coerceAnonToSlice(allocator, dida.core.Change, anon_expected_changes);
    var builder = dida.core.ChangeBatchBuilder.init(allocator);
    for (input_changes) |change| {
        try builder.changes.append(change);
    }
    if (try builder.finishAndReset()) |batch| {
        try expectDeepEqual(batch.changes, expected_changes);
        for (batch.changes) |change| {
            try std.testing.expect(batch.lower_bound.causalOrder(change.timestamp).isLessThanOrEqual());
        }
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
    try testChangeBatchBuilder(
        a,
        .{
            .{ .{"a"}, .{}, 1 },
            .{ .{"b"}, .{}, 1 },
            .{ .{"a"}, .{}, -1 },
            .{ .{"b"}, .{}, -1 },
        },
        .{},
    );
    try testChangeBatchBuilder(
        a,
        .{
            .{ .{"a"}, .{}, 1 },
            .{ .{"b"}, .{}, 1 },
            .{ .{"a"}, .{}, 1 },
            .{ .{"b"}, .{}, -1 },
        },
        .{
            .{ .{"a"}, .{}, 2 },
        },
    );
    try testChangeBatchBuilder(
        a,
        .{
            .{ .{"a"}, .{}, 1 },
            .{ .{"a"}, .{}, -1 },
            .{ .{"a"}, .{}, 1 },
        },
        .{
            .{ .{"a"}, .{}, 1 },
        },
    );
    try testChangeBatchBuilder(
        a,
        .{
            .{ .{"a"}, .{}, 0 },
            .{ .{"a"}, .{}, 0 },
            .{ .{"a"}, .{}, 0 },
        },
        .{},
    );
    try testChangeBatchBuilder(
        a,
        .{
            .{ .{"a"}, .{}, 0 },
            .{ .{"a"}, .{}, 0 },
            .{ .{"a"}, .{}, 1 },
        },
        .{
            .{ .{"a"}, .{}, 1 },
        },
    );
}

fn testFrontierMove(
    allocator: *std.mem.Allocator,
    anon_frontier: anytype,
    comptime direction: dida.core.Frontier.Direction,
    anon_timestamp: anytype,
    anon_expected_frontier: anytype,
    anon_expected_changes: anytype,
) !void {
    const frontier_timestamps = dida.sugar.coerceAnonToSlice(allocator, dida.core.Timestamp, anon_frontier);
    var frontier = dida.core.Frontier.init(allocator);
    for (frontier_timestamps) |frontier_timestamp| {
        var changes_into = std.ArrayList(dida.core.FrontierChange).init(allocator);
        try frontier.move(.Later, frontier_timestamp, &changes_into);
    }
    const timestamp = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_timestamp);
    const expected_frontier_timestamps = dida.sugar.coerceAnonToSlice(allocator, dida.core.Timestamp, anon_expected_frontier);
    const expected_changes = dida.sugar.coerceAnonToSlice(allocator, dida.core.FrontierChange, anon_expected_changes);
    var actual_changes_into = std.ArrayList(dida.core.FrontierChange).init(allocator);
    try frontier.move(direction, timestamp, &actual_changes_into);
    var actual_frontier_timestamps = std.ArrayList(dida.core.Timestamp).init(allocator);
    var iter = frontier.timestamps.iterator();
    while (iter.next()) |entry| try actual_frontier_timestamps.append(entry.key_ptr.*);
    std.sort.sort(dida.core.Timestamp, actual_frontier_timestamps.items, {}, struct {
        fn lessThan(_: void, a: dida.core.Timestamp, b: dida.core.Timestamp) bool {
            return a.lexicalOrder(b) == .lt;
        }
    }.lessThan);
    std.sort.sort(dida.core.FrontierChange, actual_changes_into.items, {}, struct {
        fn lessThan(_: void, a: dida.core.FrontierChange, b: dida.core.FrontierChange) bool {
            return dida.meta.deepOrder(a, b) == .lt;
        }
    }.lessThan);
    try expectDeepEqual(actual_frontier_timestamps.items, expected_frontier_timestamps);
    try expectDeepEqual(actual_changes_into.items, expected_changes);
}

test "test frontier move" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = &arena.allocator;
    try testFrontierMove(
        a,
        .{},
        .Later,
        .{ 0, 0 },
        .{
            .{ 0, 0 },
        },
        .{
            .{ .{ 0, 0 }, 1 },
        },
    );
    try testFrontierMove(
        a,
        .{
            .{ 0, 0 },
        },
        .Later,
        .{ 0, 1 },
        .{
            .{ 0, 1 },
        },
        .{
            .{ .{ 0, 0 }, -1 },
            .{ .{ 0, 1 }, 1 },
        },
    );
    try testFrontierMove(
        a,
        .{
            .{ 0, 1 },
        },
        .Earlier,
        .{ 0, 0 },
        .{
            .{ 0, 0 },
        },
        .{
            .{ .{ 0, 0 }, 1 },
            .{ .{ 0, 1 }, -1 },
        },
    );
    try testFrontierMove(
        a,
        .{
            .{ 0, 1 },
            .{ 1, 0 },
        },
        .Later,
        .{ 1, 1 },
        .{
            .{ 1, 1 },
        },
        .{
            .{ .{ 0, 1 }, -1 },
            .{ .{ 1, 0 }, -1 },
            .{ .{ 1, 1 }, 1 },
        },
    );
    try testFrontierMove(
        a,
        .{
            .{ 0, 0, 1 },
            .{ 0, 1, 0 },
        },
        .Later,
        .{ 1, 0, 0 },
        .{
            .{ 0, 0, 1 },
            .{ 0, 1, 0 },
            .{ 1, 0, 0 },
        },
        .{
            .{ .{ 1, 0, 0 }, 1 },
        },
    );
}

fn testFrontierOrder(
    allocator: *std.mem.Allocator,
    anon_frontier: anytype,
    anon_timestamp: anytype,
    expected_order: dida.core.PartialOrder,
) !void {
    const frontier_timestamps = dida.sugar.coerceAnonToSlice(allocator, dida.core.Timestamp, anon_frontier);
    var frontier = dida.core.Frontier.init(allocator);
    for (frontier_timestamps) |frontier_timestamp| {
        var changes_into = std.ArrayList(dida.core.FrontierChange).init(allocator);
        try frontier.move(.Later, frontier_timestamp, &changes_into);
    }
    const timestamp = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_timestamp);
    try std.testing.expectEqual(frontier.causalOrder(timestamp), expected_order);
}

test "test frontier order" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = &arena.allocator;
    try testFrontierOrder(
        a,
        .{},
        .{},
        .none,
    );
    try testFrontierOrder(
        a,
        .{
            .{ 0, 0 },
        },
        .{ 0, 0 },
        .eq,
    );
    try testFrontierOrder(
        a,
        .{
            .{ 0, 0 },
        },
        .{ 0, 1 },
        .lt,
    );
    try testFrontierOrder(
        a,
        .{
            .{ 0, 1 },
        },
        .{ 0, 0 },
        .gt,
    );
    try testFrontierOrder(
        a,
        .{
            .{ 0, 1 },
        },
        .{ 1, 0 },
        .none,
    );
    try testFrontierOrder(
        a,
        .{
            .{ 1, 0 },
            .{ 0, 1 },
        },
        .{ 0, 2 },
        .lt,
    );
    try testFrontierOrder(
        a,
        .{
            .{ 1, 0 },
            .{ 0, 1 },
        },
        .{ 2, 0 },
        .lt,
    );
    try testFrontierOrder(
        a,
        .{
            .{ 2, 0 },
            .{ 0, 2 },
        },
        .{ 1, 1 },
        .none,
    );
}

fn testSupportFrontierUpdate(
    allocator: *std.mem.Allocator,
    anon_support: anytype,
    anon_update: anytype,
    anon_expected_frontier: anytype,
    anon_expected_changes: anytype,
) !void {
    const support = dida.sugar.coerceAnonToSlice(allocator, dida.core.FrontierChange, anon_support);
    var frontier = try dida.core.SupportedFrontier.init(allocator);
    for (support) |change| {
        var changes_into = std.ArrayList(dida.core.FrontierChange).init(allocator);
        try frontier.update(change.timestamp, change.diff, &changes_into);
    }
    const update = dida.sugar.coerceAnonTo(allocator, dida.core.FrontierChange, anon_update);
    const expected_frontier_timestamps = dida.sugar.coerceAnonToSlice(allocator, dida.core.Timestamp, anon_expected_frontier);
    const expected_changes = dida.sugar.coerceAnonToSlice(allocator, dida.core.FrontierChange, anon_expected_changes);
    var actual_changes_into = std.ArrayList(dida.core.FrontierChange).init(allocator);
    try frontier.update(update.timestamp, update.diff, &actual_changes_into);
    var actual_frontier_timestamps = std.ArrayList(dida.core.Timestamp).init(allocator);
    var iter = frontier.frontier.timestamps.iterator();
    while (iter.next()) |entry| try actual_frontier_timestamps.append(entry.key_ptr.*);
    std.sort.sort(dida.core.Timestamp, actual_frontier_timestamps.items, {}, struct {
        fn lessThan(_: void, a: dida.core.Timestamp, b: dida.core.Timestamp) bool {
            return a.lexicalOrder(b) == .lt;
        }
    }.lessThan);
    std.sort.sort(dida.core.FrontierChange, actual_changes_into.items, {}, struct {
        fn lessThan(_: void, a: dida.core.FrontierChange, b: dida.core.FrontierChange) bool {
            return dida.meta.deepOrder(a, b) == .lt;
        }
    }.lessThan);
    try expectDeepEqual(actual_frontier_timestamps.items, expected_frontier_timestamps);
    try expectDeepEqual(actual_changes_into.items, expected_changes);
}

test "test support frontier update" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = &arena.allocator;
    try testSupportFrontierUpdate(
        a,
        .{},
        .{ .{ 0, 0 }, 1 },
        .{
            .{ 0, 0 },
        },
        .{
            .{ .{ 0, 0 }, 1 },
        },
    );
    try testSupportFrontierUpdate(
        a,
        .{
            .{ .{ 0, 0 }, 1 },
        },
        .{ .{ 0, 0 }, 1 },
        .{
            .{ 0, 0 },
        },
        .{},
    );
    try testSupportFrontierUpdate(
        a,
        .{
            .{ .{ 0, 0 }, 1 },
        },
        .{ .{ 0, 0 }, -1 },
        .{},
        .{
            .{ .{ 0, 0 }, -1 },
        },
    );
    try testSupportFrontierUpdate(
        a,
        .{
            .{ .{ 0, 0 }, 2 },
        },
        .{ .{ 0, 0 }, -1 },
        .{
            .{ 0, 0 },
        },
        .{},
    );
    try testSupportFrontierUpdate(
        a,
        .{
            .{ .{ 0, 0 }, 1 },
            .{ .{ 1, 1 }, 1 },
        },
        .{ .{ 0, 0 }, -1 },
        .{
            .{ 1, 1 },
        },
        .{
            .{ .{ 0, 0 }, -1 },
            .{ .{ 1, 1 }, 1 },
        },
    );
    try testSupportFrontierUpdate(
        a,
        .{
            .{ .{ 1, 1 }, 1 },
        },
        .{ .{ 0, 0 }, 1 },
        .{
            .{ 0, 0 },
        },
        .{
            .{ .{ 0, 0 }, 1 },
            .{ .{ 1, 1 }, -1 },
        },
    );
    try testSupportFrontierUpdate(
        a,
        .{
            .{ .{ 0, 0 }, 1 },
        },
        .{ .{ 1, 1 }, 1 },
        .{
            .{ 0, 0 },
        },
        .{},
    );
    try testSupportFrontierUpdate(
        a,
        .{
            .{ .{ 2, 0 }, 1 },
            .{ .{ 0, 1 }, 1 },
        },
        .{ .{ 1, 0 }, 1 },
        .{
            .{ 0, 1 },
            .{ 1, 0 },
        },
        .{
            .{ .{ 1, 0 }, 1 },
            .{ .{ 2, 0 }, -1 },
        },
    );
}
