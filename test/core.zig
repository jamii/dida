const std = @import("std");
const dida = @import("../lib/dida.zig");

pub const allocator = std.testing.allocator;

fn expectDeepEqual(expected: anytype, actual: anytype) !void {
    if (!dida.util.deepEqual(expected, actual)) {
        dida.util.dump(.{ .expected = expected, .actual = actual });
        return error.TestExpectedEqual;
    }
}

fn testTimestampOrder(
    anon_a: anytype,
    anon_b: anytype,
    order: dida.core.PartialOrder,
) !void {
    var a = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_a);
    defer a.deinit(allocator);
    var b = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_b);
    defer b.deinit(allocator);
    try std.testing.expectEqual(order, a.causalOrder(b));
    const reverse_order: dida.core.PartialOrder = switch (order) {
        .none => .none,
        .lt => .gt,
        .eq => .eq,
        .gt => .lt,
    };
    try std.testing.expectEqual(reverse_order, b.causalOrder(a));
    const total_order: std.math.Order = switch (order) {
        .none => return,
        .lt => .lt,
        .eq => .eq,
        .gt => .gt,
    };
    if (order != .none) {
        try std.testing.expectEqual(total_order, a.lexicalOrder(b));
    }
    var lub = try dida.core.Timestamp.leastUpperBound(allocator, a, b);
    defer lub.deinit(allocator);
    try std.testing.expect(a.causalOrder(lub).isLessThanOrEqual());
    try std.testing.expect(b.causalOrder(lub).isLessThanOrEqual());
}

test "timestamp order" {
    try testTimestampOrder(.{}, .{}, .eq);
    try testTimestampOrder(.{0}, .{0}, .eq);
    try testTimestampOrder(.{0}, .{1}, .lt);
    try testTimestampOrder(.{1}, .{0}, .gt);
    try testTimestampOrder(.{ 0, 0 }, .{ 0, 0 }, .eq);
    try testTimestampOrder(.{ 0, 0 }, .{ 1, 0 }, .lt);
    try testTimestampOrder(.{ 0, 0 }, .{ 0, 1 }, .lt);
    try testTimestampOrder(.{ 0, 0 }, .{ 1, 1 }, .lt);
    try testTimestampOrder(.{ 1, 0 }, .{ 0, 1 }, .none);
}

fn testTimestampLub(
    anon_a: anytype,
    anon_b: anytype,
    anon_lub: anytype,
) !void {
    var a = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_a);
    defer a.deinit(allocator);
    var b = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_b);
    defer b.deinit(allocator);
    var expected_lub = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_lub);
    defer expected_lub.deinit(allocator);
    var actual_lub = try dida.core.Timestamp.leastUpperBound(allocator, a, b);
    defer actual_lub.deinit(allocator);
    try std.testing.expectEqualSlices(usize, expected_lub.coords, actual_lub.coords);
}

test "timestamp lub" {
    try testTimestampLub(.{}, .{}, .{});
    try testTimestampLub(.{0}, .{0}, .{0});
    try testTimestampLub(.{0}, .{1}, .{1});
    try testTimestampLub(.{1}, .{0}, .{1});
    try testTimestampLub(.{ 0, 0 }, .{ 0, 0 }, .{ 0, 0 });
    try testTimestampLub(.{ 0, 0 }, .{ 1, 0 }, .{ 1, 0 });
    try testTimestampLub(.{ 0, 0 }, .{ 0, 1 }, .{ 0, 1 });
    try testTimestampLub(.{ 0, 0 }, .{ 1, 1 }, .{ 1, 1 });
    try testTimestampLub(.{ 1, 0 }, .{ 0, 1 }, .{ 1, 1 });
}

fn testChangeBatchBuilder(
    anon_input_changes: anytype,
    anon_expected_changes: anytype,
) !void {
    const input_changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_input_changes);
    defer allocator.free(input_changes);
    const expected_changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_expected_changes);
    defer {
        for (expected_changes) |*expected_change| expected_change.deinit(allocator);
        allocator.free(expected_changes);
    }
    var builder = dida.core.ChangeBatchBuilder.init(allocator);
    defer builder.deinit();
    for (input_changes) |change| {
        try builder.changes.append(change);
    }
    if (try builder.finishAndReset()) |_batch| {
        var batch = _batch;
        defer batch.deinit(allocator);
        try expectDeepEqual(expected_changes, batch.changes);
        for (batch.changes) |change| {
            try std.testing.expect(batch.lower_bound.causalOrder(change.timestamp).isLessThanOrEqual());
        }
    } else {
        const actual_changes: []dida.core.Change = &[0]dida.core.Change{};
        try expectDeepEqual(expected_changes, actual_changes);
    }
}

test "change batch builder" {
    try testChangeBatchBuilder(
        .{},
        .{},
    );
    try testChangeBatchBuilder(
        .{
            .{ .{}, .{}, 0 },
        },
        .{},
    );
    try testChangeBatchBuilder(
        .{
            .{ .{}, .{}, 1 },
        },
        .{
            .{ .{}, .{}, 1 },
        },
    );
    try testChangeBatchBuilder(
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
        .{
            .{ .{"a"}, .{}, 1 },
            .{ .{"b"}, .{}, 1 },
            .{ .{"a"}, .{}, -1 },
            .{ .{"b"}, .{}, -1 },
        },
        .{},
    );
    try testChangeBatchBuilder(
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
        .{
            .{ .{"a"}, .{}, 0 },
            .{ .{"a"}, .{}, 0 },
            .{ .{"a"}, .{}, 0 },
        },
        .{},
    );
    try testChangeBatchBuilder(
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
    anon_frontier: anytype,
    comptime direction: dida.core.Frontier.Direction,
    anon_timestamp: anytype,
    anon_expected_frontier: anytype,
    anon_expected_changes: anytype,
) !void {
    const frontier_timestamps = dida.sugar.coerceAnonTo(allocator, []dida.core.Timestamp, anon_frontier);
    defer {
        for (frontier_timestamps) |*frontier_timestamp| frontier_timestamp.deinit(allocator);
        allocator.free(frontier_timestamps);
    }
    var frontier = dida.core.Frontier.init(allocator);
    defer frontier.deinit();
    var changes_into = std.ArrayList(dida.core.FrontierChange).init(allocator);
    defer changes_into.deinit();
    for (frontier_timestamps) |frontier_timestamp| {
        try frontier.move(.Later, frontier_timestamp, &changes_into);
        for (changes_into.items) |*change| change.deinit(allocator);
        try changes_into.resize(0);
    }
    var timestamp = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_timestamp);
    defer timestamp.deinit(allocator);
    const expected_frontier_timestamps = dida.sugar.coerceAnonTo(allocator, []dida.core.Timestamp, anon_expected_frontier);
    defer {
        for (expected_frontier_timestamps) |*expected_frontier_timestamp| expected_frontier_timestamp.deinit(allocator);
        allocator.free(expected_frontier_timestamps);
    }
    const expected_changes = dida.sugar.coerceAnonTo(allocator, []dida.core.FrontierChange, anon_expected_changes);
    defer {
        for (expected_changes) |*expected_change| expected_change.deinit(allocator);
        allocator.free(expected_changes);
    }
    var actual_changes_into = std.ArrayList(dida.core.FrontierChange).init(allocator);
    defer {
        for (actual_changes_into.items) |*change| change.deinit(allocator);
        actual_changes_into.deinit();
    }
    try frontier.move(direction, timestamp, &actual_changes_into);
    var actual_frontier_timestamps = std.ArrayList(dida.core.Timestamp).init(allocator);
    defer actual_frontier_timestamps.deinit();
    var iter = frontier.timestamps.iterator();
    while (iter.next()) |entry| try actual_frontier_timestamps.append(entry.key_ptr.*);
    std.sort.sort(dida.core.Timestamp, actual_frontier_timestamps.items, {}, struct {
        fn lessThan(_: void, a: dida.core.Timestamp, b: dida.core.Timestamp) bool {
            return a.lexicalOrder(b) == .lt;
        }
    }.lessThan);
    std.sort.sort(dida.core.FrontierChange, actual_changes_into.items, {}, struct {
        fn lessThan(_: void, a: dida.core.FrontierChange, b: dida.core.FrontierChange) bool {
            return dida.util.deepOrder(a, b) == .lt;
        }
    }.lessThan);
    try expectDeepEqual(expected_frontier_timestamps, actual_frontier_timestamps.items);
    try expectDeepEqual(expected_changes, actual_changes_into.items);
}

test "test frontier move" {
    try testFrontierMove(
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
    anon_frontier: anytype,
    anon_timestamp: anytype,
    expected_order: dida.core.PartialOrder,
) !void {
    const frontier_timestamps = dida.sugar.coerceAnonTo(allocator, []dida.core.Timestamp, anon_frontier);
    defer {
        for (frontier_timestamps) |*frontier_timestamp| frontier_timestamp.deinit(allocator);
        allocator.free(frontier_timestamps);
    }
    var frontier = dida.core.Frontier.init(allocator);
    defer frontier.deinit();
    var changes_into = std.ArrayList(dida.core.FrontierChange).init(allocator);
    defer changes_into.deinit();
    for (frontier_timestamps) |frontier_timestamp| {
        try frontier.move(.Later, frontier_timestamp, &changes_into);
        for (changes_into.items) |*change| change.deinit(allocator);
        try changes_into.resize(0);
    }
    var timestamp = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_timestamp);
    defer timestamp.deinit(allocator);
    try std.testing.expectEqual(expected_order, frontier.causalOrder(timestamp));
}

test "test frontier order" {
    try testFrontierOrder(
        .{},
        .{},
        .none,
    );
    try testFrontierOrder(
        .{
            .{ 0, 0 },
        },
        .{ 0, 0 },
        .eq,
    );
    try testFrontierOrder(
        .{
            .{ 0, 0 },
        },
        .{ 0, 1 },
        .lt,
    );
    try testFrontierOrder(
        .{
            .{ 0, 1 },
        },
        .{ 0, 0 },
        .gt,
    );
    try testFrontierOrder(
        .{
            .{ 0, 1 },
        },
        .{ 1, 0 },
        .none,
    );
    try testFrontierOrder(
        .{
            .{ 1, 0 },
            .{ 0, 1 },
        },
        .{ 0, 2 },
        .lt,
    );
    try testFrontierOrder(
        .{
            .{ 1, 0 },
            .{ 0, 1 },
        },
        .{ 2, 0 },
        .lt,
    );
    try testFrontierOrder(
        .{
            .{ 2, 0 },
            .{ 0, 2 },
        },
        .{ 1, 1 },
        .none,
    );
}

fn testSupportFrontierUpdate(
    anon_support: anytype,
    anon_update: anytype,
    anon_expected_frontier: anytype,
    anon_expected_changes: anytype,
) !void {
    const support = dida.sugar.coerceAnonTo(allocator, []dida.core.FrontierChange, anon_support);
    defer {
        for (support) |*frontier_change| frontier_change.deinit(allocator);
        allocator.free(support);
    }
    var frontier = try dida.core.SupportedFrontier.init(allocator);
    defer frontier.deinit();
    var changes_into = std.ArrayList(dida.core.FrontierChange).init(allocator);
    defer changes_into.deinit();
    for (support) |change| {
        try frontier.update(change.timestamp, change.diff, &changes_into);
        for (changes_into.items) |*frontier_change| frontier_change.deinit(allocator);
        try changes_into.resize(0);
    }
    var update = dida.sugar.coerceAnonTo(allocator, dida.core.FrontierChange, anon_update);
    defer update.deinit(allocator);
    const expected_frontier_timestamps = dida.sugar.coerceAnonTo(allocator, []dida.core.Timestamp, anon_expected_frontier);
    defer {
        for (expected_frontier_timestamps) |*expected_frontier_timestamp| expected_frontier_timestamp.deinit(allocator);
        allocator.free(expected_frontier_timestamps);
    }
    const expected_changes = dida.sugar.coerceAnonTo(allocator, []dida.core.FrontierChange, anon_expected_changes);
    defer {
        for (expected_changes) |*expected_change| expected_change.deinit(allocator);
        allocator.free(expected_changes);
    }
    var actual_changes_into = std.ArrayList(dida.core.FrontierChange).init(allocator);
    defer {
        for (actual_changes_into.items) |*change| change.deinit(allocator);
        actual_changes_into.deinit();
    }
    try frontier.update(update.timestamp, update.diff, &actual_changes_into);
    var actual_frontier_timestamps = std.ArrayList(dida.core.Timestamp).init(allocator);
    defer actual_frontier_timestamps.deinit();
    var iter = frontier.frontier.timestamps.iterator();
    while (iter.next()) |entry| try actual_frontier_timestamps.append(entry.key_ptr.*);
    std.sort.sort(dida.core.Timestamp, actual_frontier_timestamps.items, {}, struct {
        fn lessThan(_: void, a: dida.core.Timestamp, b: dida.core.Timestamp) bool {
            return a.lexicalOrder(b) == .lt;
        }
    }.lessThan);
    std.sort.sort(dida.core.FrontierChange, actual_changes_into.items, {}, struct {
        fn lessThan(_: void, a: dida.core.FrontierChange, b: dida.core.FrontierChange) bool {
            return dida.util.deepOrder(a, b) == .lt;
        }
    }.lessThan);
    try expectDeepEqual(expected_frontier_timestamps, actual_frontier_timestamps.items);
    try expectDeepEqual(expected_changes, actual_changes_into.items);
}

test "test supported frontier update" {
    try testSupportFrontierUpdate(
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

pub fn testIndexAdd(
    anon_change_batches: anytype,
    anon_expected_changess: anytype,
) !void {
    var index = dida.core.Index.init(allocator);
    defer index.deinit();
    const change_batches = dida.sugar.coerceAnonTo(allocator, []dida.core.ChangeBatch, anon_change_batches);
    defer allocator.free(change_batches);
    const expected_changess = dida.sugar.coerceAnonTo(allocator, [][]dida.core.Change, anon_expected_changess);
    defer {
        for (expected_changess) |expected_changes| {
            for (expected_changes) |*expected_change| {
                expected_change.deinit(allocator);
            }
            allocator.free(expected_changes);
        }
        allocator.free(expected_changess);
    }
    for (change_batches) |change_batch| {
        try index.addChangeBatch(change_batch);
    }
    const actual_changess = try allocator.alloc([]dida.core.Change, index.change_batches.items.len);
    defer allocator.free(actual_changess);
    for (actual_changess) |*changes, i| changes.* = index.change_batches.items[i].changes;
    try expectDeepEqual(expected_changess, actual_changess);
}

test "test index add" {
    try testIndexAdd(
        .{},
        .{},
    );
    try testIndexAdd(
        .{
            .{
                .{ .{"a"}, .{0}, 1 },
            },
        },
        .{
            .{
                .{ .{"a"}, .{0}, 1 },
            },
        },
    );
    try testIndexAdd(
        .{
            .{
                .{ .{"a"}, .{0}, 1 },
            },
            .{
                .{ .{"a"}, .{0}, 1 },
            },
        },
        .{
            .{
                .{ .{"a"}, .{0}, 2 },
            },
        },
    );
    try testIndexAdd(
        .{
            .{
                .{ .{"a"}, .{0}, 1 },
                .{ .{"b"}, .{0}, 1 },
            },
            .{
                .{ .{"a"}, .{0}, 1 },
            },
        },
        .{
            .{
                .{ .{"a"}, .{0}, 1 },
                .{ .{"b"}, .{0}, 1 },
            },
            .{
                .{ .{"a"}, .{0}, 1 },
            },
        },
    );
    try testIndexAdd(
        .{
            .{
                .{ .{"a"}, .{0}, 1 },
                .{ .{"b"}, .{0}, 1 },
            },
            .{
                .{ .{"a"}, .{0}, 1 },
                .{ .{"b"}, .{0}, -1 },
            },
        },
        .{
            .{
                .{ .{"a"}, .{0}, 2 },
            },
        },
    );
    try testIndexAdd(
        .{
            .{
                .{ .{"a"}, .{0}, 1 },
            },
            .{
                .{ .{"b"}, .{0}, 1 },
            },
            .{
                .{ .{"c"}, .{0}, 1 },
            },
            .{
                .{ .{"d"}, .{0}, 1 },
            },
            .{
                .{ .{"e"}, .{0}, 1 },
            },
            .{
                .{ .{"f"}, .{0}, 1 },
            },
            .{
                .{ .{"g"}, .{0}, 1 },
            },
        },
        .{
            .{
                .{ .{"a"}, .{0}, 1 },
                .{ .{"b"}, .{0}, 1 },
                .{ .{"c"}, .{0}, 1 },
                .{ .{"d"}, .{0}, 1 },
            },
            .{
                .{ .{"e"}, .{0}, 1 },
                .{ .{"f"}, .{0}, 1 },
            },
            .{
                .{ .{"g"}, .{0}, 1 },
            },
        },
    );
    // TODO bulk adds break the leveling
    try testIndexAdd(
        .{
            .{
                .{ .{"a"}, .{0}, 1 },
            },
            .{
                .{ .{"b"}, .{0}, 1 },
            },
            .{
                .{ .{"c"}, .{0}, 1 },
            },
            .{
                .{ .{"d"}, .{0}, 1 },
            },
            .{
                .{ .{"e"}, .{0}, 1 },
            },
            .{
                .{ .{"f"}, .{0}, 1 },
            },
            .{
                .{ .{"g"}, .{0}, 1 },
            },
            .{
                .{ .{"h"}, .{0}, 1 },
                .{ .{"i"}, .{0}, 1 },
                .{ .{"j"}, .{0}, 1 },
                .{ .{"k"}, .{0}, 1 },
                .{ .{"l"}, .{0}, 1 },
                .{ .{"m"}, .{0}, 1 },
                .{ .{"n"}, .{0}, 1 },
                .{ .{"o"}, .{0}, 1 },
            },
        },
        .{
            .{
                .{ .{"a"}, .{0}, 1 },
                .{ .{"b"}, .{0}, 1 },
                .{ .{"c"}, .{0}, 1 },
                .{ .{"d"}, .{0}, 1 },
                .{ .{"e"}, .{0}, 1 },
                .{ .{"f"}, .{0}, 1 },
                .{ .{"g"}, .{0}, 1 },
                .{ .{"h"}, .{0}, 1 },
                .{ .{"i"}, .{0}, 1 },
                .{ .{"j"}, .{0}, 1 },
                .{ .{"k"}, .{0}, 1 },
                .{ .{"l"}, .{0}, 1 },
                .{ .{"m"}, .{0}, 1 },
                .{ .{"n"}, .{0}, 1 },
                .{ .{"o"}, .{0}, 1 },
            },
        },
    );
}

fn testChangeBatchSeekRowStart(
    anon_changes: anytype,
    ix: usize,
    anon_row: anytype,
    key_columns: usize,
    expected_ix: usize,
) !void {
    const changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_changes);
    defer allocator.free(changes);
    var row = dida.sugar.coerceAnonTo(allocator, dida.core.Row, anon_row);
    defer row.deinit(allocator);

    var builder = dida.core.ChangeBatchBuilder.init(allocator);
    defer builder.deinit();
    for (changes) |change| {
        try builder.changes.append(change);
    }
    var change_batch = (try builder.finishAndReset()).?;
    defer change_batch.deinit(allocator);

    const actual_ix = change_batch.seekRowStart(ix, row, key_columns);
    try std.testing.expectEqual(expected_ix, actual_ix);
}

test "test change batch seek row start" {
    const changes = .{
        .{ .{ "a", "x" }, .{0}, 1 },
        .{ .{ "a", "y" }, .{1}, 1 },
        .{ .{ "a", "z" }, .{2}, 1 },
        .{ .{ "c", "c" }, .{0}, 1 },
        .{ .{ "e", "e" }, .{0}, 1 },
    };

    try testChangeBatchSeekRowStart(changes, 0, .{"a"}, 1, 0);
    try testChangeBatchSeekRowStart(changes, 1, .{"a"}, 1, 1);
    try testChangeBatchSeekRowStart(changes, 2, .{"a"}, 1, 2);
    try testChangeBatchSeekRowStart(changes, 3, .{"a"}, 1, 3);
    try testChangeBatchSeekRowStart(changes, 4, .{"a"}, 1, 4);
    try testChangeBatchSeekRowStart(changes, 5, .{"a"}, 1, 5);

    try testChangeBatchSeekRowStart(changes, 0, .{"b"}, 1, 3);
    try testChangeBatchSeekRowStart(changes, 1, .{"b"}, 1, 3);
    try testChangeBatchSeekRowStart(changes, 2, .{"b"}, 1, 3);
    try testChangeBatchSeekRowStart(changes, 3, .{"b"}, 1, 3);
    try testChangeBatchSeekRowStart(changes, 4, .{"b"}, 1, 4);
    try testChangeBatchSeekRowStart(changes, 5, .{"b"}, 1, 5);

    try testChangeBatchSeekRowStart(changes, 0, .{"c"}, 1, 3);
    try testChangeBatchSeekRowStart(changes, 1, .{"c"}, 1, 3);
    try testChangeBatchSeekRowStart(changes, 2, .{"c"}, 1, 3);
    try testChangeBatchSeekRowStart(changes, 3, .{"c"}, 1, 3);
    try testChangeBatchSeekRowStart(changes, 4, .{"c"}, 1, 4);
    try testChangeBatchSeekRowStart(changes, 5, .{"c"}, 1, 5);

    try testChangeBatchSeekRowStart(changes, 0, .{"d"}, 1, 4);
    try testChangeBatchSeekRowStart(changes, 1, .{"d"}, 1, 4);
    try testChangeBatchSeekRowStart(changes, 2, .{"d"}, 1, 4);
    try testChangeBatchSeekRowStart(changes, 3, .{"d"}, 1, 4);
    try testChangeBatchSeekRowStart(changes, 4, .{"d"}, 1, 4);
    try testChangeBatchSeekRowStart(changes, 5, .{"d"}, 1, 5);

    try testChangeBatchSeekRowStart(changes, 0, .{"e"}, 1, 4);
    try testChangeBatchSeekRowStart(changes, 1, .{"e"}, 1, 4);
    try testChangeBatchSeekRowStart(changes, 2, .{"e"}, 1, 4);
    try testChangeBatchSeekRowStart(changes, 3, .{"e"}, 1, 4);
    try testChangeBatchSeekRowStart(changes, 4, .{"e"}, 1, 4);
    try testChangeBatchSeekRowStart(changes, 5, .{"e"}, 1, 5);

    try testChangeBatchSeekRowStart(changes, 0, .{"f"}, 1, 5);
    try testChangeBatchSeekRowStart(changes, 1, .{"f"}, 1, 5);
    try testChangeBatchSeekRowStart(changes, 2, .{"f"}, 1, 5);
    try testChangeBatchSeekRowStart(changes, 3, .{"f"}, 1, 5);
    try testChangeBatchSeekRowStart(changes, 4, .{"f"}, 1, 5);
    try testChangeBatchSeekRowStart(changes, 5, .{"f"}, 1, 5);
}

fn testChangeBatchSeekRowEnd(
    anon_changes: anytype,
    ix: usize,
    anon_row: anytype,
    key_columns: usize,
    expected_ix: usize,
) !void {
    const changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_changes);
    defer allocator.free(changes);
    var row = dida.sugar.coerceAnonTo(allocator, dida.core.Row, anon_row);
    defer row.deinit(allocator);

    var builder = dida.core.ChangeBatchBuilder.init(allocator);
    defer builder.deinit();
    for (changes) |change| {
        try builder.changes.append(change);
    }
    var change_batch = (try builder.finishAndReset()).?;
    defer change_batch.deinit(allocator);

    const actual_ix = change_batch.seekRowEnd(ix, row, key_columns);
    try std.testing.expectEqual(expected_ix, actual_ix);
}

test "test change batch seek current row end" {
    const changes = .{
        .{ .{ "a", "x" }, .{0}, 1 },
        .{ .{ "a", "y" }, .{1}, 1 },
        .{ .{ "a", "z" }, .{2}, 1 },
        .{ .{ "c", "c" }, .{0}, 1 },
        .{ .{ "e", "e" }, .{0}, 1 },
    };

    try testChangeBatchSeekRowEnd(changes, 0, .{"a"}, 1, 3);
    try testChangeBatchSeekRowEnd(changes, 1, .{"a"}, 1, 3);
    try testChangeBatchSeekRowEnd(changes, 2, .{"a"}, 1, 3);
    try testChangeBatchSeekRowEnd(changes, 3, .{"a"}, 1, 3);
    try testChangeBatchSeekRowEnd(changes, 4, .{"a"}, 1, 4);
    try testChangeBatchSeekRowEnd(changes, 5, .{"a"}, 1, 5);

    try testChangeBatchSeekRowEnd(changes, 0, .{"b"}, 1, 3);
    try testChangeBatchSeekRowEnd(changes, 1, .{"b"}, 1, 3);
    try testChangeBatchSeekRowEnd(changes, 2, .{"b"}, 1, 3);
    try testChangeBatchSeekRowEnd(changes, 3, .{"b"}, 1, 3);
    try testChangeBatchSeekRowEnd(changes, 4, .{"b"}, 1, 4);
    try testChangeBatchSeekRowEnd(changes, 5, .{"b"}, 1, 5);

    try testChangeBatchSeekRowEnd(changes, 0, .{"c"}, 1, 4);
    try testChangeBatchSeekRowEnd(changes, 1, .{"c"}, 1, 4);
    try testChangeBatchSeekRowEnd(changes, 2, .{"c"}, 1, 4);
    try testChangeBatchSeekRowEnd(changes, 3, .{"c"}, 1, 4);
    try testChangeBatchSeekRowEnd(changes, 4, .{"c"}, 1, 4);
    try testChangeBatchSeekRowEnd(changes, 5, .{"c"}, 1, 5);

    try testChangeBatchSeekRowEnd(changes, 0, .{"d"}, 1, 4);
    try testChangeBatchSeekRowEnd(changes, 1, .{"d"}, 1, 4);
    try testChangeBatchSeekRowEnd(changes, 2, .{"d"}, 1, 4);
    try testChangeBatchSeekRowEnd(changes, 3, .{"d"}, 1, 4);
    try testChangeBatchSeekRowEnd(changes, 4, .{"d"}, 1, 4);
    try testChangeBatchSeekRowEnd(changes, 5, .{"d"}, 1, 5);

    try testChangeBatchSeekRowEnd(changes, 0, .{"e"}, 1, 5);
    try testChangeBatchSeekRowEnd(changes, 1, .{"e"}, 1, 5);
    try testChangeBatchSeekRowEnd(changes, 2, .{"e"}, 1, 5);
    try testChangeBatchSeekRowEnd(changes, 3, .{"e"}, 1, 5);
    try testChangeBatchSeekRowEnd(changes, 4, .{"e"}, 1, 5);
    try testChangeBatchSeekRowEnd(changes, 5, .{"e"}, 1, 5);

    try testChangeBatchSeekRowEnd(changes, 0, .{"f"}, 1, 5);
    try testChangeBatchSeekRowEnd(changes, 1, .{"f"}, 1, 5);
    try testChangeBatchSeekRowEnd(changes, 2, .{"f"}, 1, 5);
    try testChangeBatchSeekRowEnd(changes, 3, .{"f"}, 1, 5);
    try testChangeBatchSeekRowEnd(changes, 4, .{"f"}, 1, 5);
    try testChangeBatchSeekRowEnd(changes, 5, .{"f"}, 1, 5);
}

fn testChangeBatchSeekCurrentRowEnd(
    anon_changes: anytype,
    ix: usize,
    key_columns: usize,
    expected_ix: usize,
) !void {
    const changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_changes);
    defer allocator.free(changes);

    var builder = dida.core.ChangeBatchBuilder.init(allocator);
    defer builder.deinit();
    for (changes) |change| {
        try builder.changes.append(change);
    }
    var change_batch = (try builder.finishAndReset()).?;
    defer change_batch.deinit(allocator);

    const actual_ix = change_batch.seekCurrentRowEnd(ix, key_columns);
    try std.testing.expectEqual(expected_ix, actual_ix);
}

test "test change batch seek current row end" {
    const changes = .{
        .{ .{ "a", "x" }, .{0}, 1 },
        .{ .{ "a", "y" }, .{1}, 1 },
        .{ .{ "a", "z" }, .{2}, 1 },
        .{ .{ "c", "c" }, .{0}, 1 },
        .{ .{ "e", "e" }, .{0}, 1 },
    };
    try testChangeBatchSeekCurrentRowEnd(changes, 0, 1, 3);
    try testChangeBatchSeekCurrentRowEnd(changes, 1, 1, 3);
    try testChangeBatchSeekCurrentRowEnd(changes, 2, 1, 3);
    try testChangeBatchSeekCurrentRowEnd(changes, 3, 1, 4);
    try testChangeBatchSeekCurrentRowEnd(changes, 4, 1, 5);
    try testChangeBatchSeekCurrentRowEnd(changes, 5, 1, 5);
}

fn testChangeBatchJoin(
    anon_left_changes: anytype,
    anon_left_frontier: anytype,
    anon_right_changes: anytype,
    key_columns: usize,
    concat_order: dida.core.ConcatOrder,
    anon_expected_changes: anytype,
) !void {
    const left_changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_left_changes);
    defer allocator.free(left_changes);
    var left_frontier = dida.sugar.coerceAnonTo(allocator, dida.core.Frontier, anon_left_frontier);
    defer left_frontier.deinit();
    const right_changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_right_changes);
    defer allocator.free(right_changes);
    const expected_changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_expected_changes);
    defer {
        for (expected_changes) |*change| change.deinit(allocator);
        allocator.free(expected_changes);
    }

    var left_builder = dida.core.ChangeBatchBuilder.init(allocator);
    defer left_builder.deinit();
    for (left_changes) |change| {
        try left_builder.changes.append(change);
    }
    var left_change_batch = (try left_builder.finishAndReset()).?;
    defer left_change_batch.deinit(allocator);

    var right_builder = dida.core.ChangeBatchBuilder.init(allocator);
    defer right_builder.deinit();
    for (right_changes) |change| {
        try right_builder.changes.append(change);
    }
    var right_change_batch = (try right_builder.finishAndReset()).?;
    defer right_change_batch.deinit(allocator);

    var output_builder = dida.core.ChangeBatchBuilder.init(allocator);
    defer output_builder.deinit();
    try left_change_batch.mergeJoin(left_frontier, right_change_batch, key_columns, concat_order, &output_builder);
    if (try output_builder.finishAndReset()) |_output_change_batch| {
        var output_change_batch = _output_change_batch;
        defer output_change_batch.deinit(allocator);
        try expectDeepEqual(expected_changes, output_change_batch.changes);
    } else {
        const actual_changes: []dida.core.Change = &[0]dida.core.Change{};
        try expectDeepEqual(expected_changes, actual_changes);
    }
}

test "test change batch join" {
    try testChangeBatchJoin(
        .{
            .{ .{"a"}, .{ 0, 1 }, 2 },
        },
        .{
            .{ 100, 100 },
        },
        .{
            .{ .{"a"}, .{ 1, 0 }, 3 },
        },
        1,
        .LeftThenRight,
        .{
            .{ .{"a"}, .{ 1, 1 }, 6 },
        },
    );
    try testChangeBatchJoin(
        .{
            .{ .{"a"}, .{ 0, 1 }, 2 },
        },
        .{
            .{ 100, 100 },
        },
        .{
            .{ .{"b"}, .{ 1, 0 }, 3 },
        },
        1,
        .LeftThenRight,
        .{},
    );
    try testChangeBatchJoin(
        .{
            .{ .{"a"}, .{ 0, 1 }, 2 },
            .{ .{"a"}, .{ 0, 2 }, 5 },
        },
        .{
            .{ 100, 100 },
        },
        .{
            .{ .{"a"}, .{ 1, 0 }, 3 },
            .{ .{"a"}, .{ 2, 0 }, 7 },
        },
        1,
        .LeftThenRight,
        .{
            .{ .{"a"}, .{ 1, 1 }, 6 },
            .{ .{"a"}, .{ 1, 2 }, 15 },
            .{ .{"a"}, .{ 2, 1 }, 14 },
            .{ .{"a"}, .{ 2, 2 }, 35 },
        },
    );
    try testChangeBatchJoin(
        .{
            .{ .{"a"}, .{ 0, 1 }, 2 },
            .{ .{"b"}, .{ 0, 2 }, 5 },
        },
        .{
            .{ 100, 100 },
        },
        .{
            .{ .{"a"}, .{ 1, 0 }, 3 },
            .{ .{"b"}, .{ 2, 0 }, 7 },
        },
        1,
        .LeftThenRight,
        .{
            .{ .{"a"}, .{ 1, 1 }, 6 },
            .{ .{"b"}, .{ 2, 2 }, 35 },
        },
    );
    {
        const changes = .{
            .{ .{"a"}, .{0}, 1 },
            .{ .{"a"}, .{1}, 1 },
            .{ .{"a"}, .{2}, 1 },
            .{ .{"c"}, .{0}, 1 },
            .{ .{"e"}, .{0}, 1 },
        };
        try testChangeBatchJoin(
            changes,
            .{
                .{100},
            },
            changes,
            1,
            .LeftThenRight,
            .{
                .{ .{"a"}, .{0}, 1 },
                .{ .{"a"}, .{1}, 3 },
                .{ .{"a"}, .{2}, 5 },
                .{ .{"c"}, .{0}, 1 },
                .{ .{"e"}, .{0}, 1 },
            },
        );
    }
    {
        const changes = .{
            .{ .{ "a", "x" }, .{0}, 1 },
            .{ .{ "a", "y" }, .{1}, 1 },
            .{ .{ "a", "z" }, .{2}, 1 },
            .{ .{ "c", "c" }, .{0}, 1 },
            .{ .{ "e", "e" }, .{0}, 1 },
        };
        try testChangeBatchJoin(
            changes,
            .{
                .{100},
            },
            changes,
            1,
            .LeftThenRight,
            .{
                .{ .{ "a", "x", "x" }, .{0}, 1 },
                .{ .{ "a", "x", "y" }, .{1}, 1 },
                .{ .{ "a", "x", "z" }, .{2}, 1 },
                .{ .{ "a", "y", "x" }, .{1}, 1 },
                .{ .{ "a", "y", "y" }, .{1}, 1 },
                .{ .{ "a", "y", "z" }, .{2}, 1 },
                .{ .{ "a", "z", "x" }, .{2}, 1 },
                .{ .{ "a", "z", "y" }, .{2}, 1 },
                .{ .{ "a", "z", "z" }, .{2}, 1 },
                .{ .{ "c", "c", "c" }, .{0}, 1 },
                .{ .{ "e", "e", "e" }, .{0}, 1 },
            },
        );
        try testChangeBatchJoin(
            changes,
            .{
                .{100},
            },
            changes,
            1,
            .RightThenLeft,
            .{
                .{ .{ "a", "x", "x" }, .{0}, 1 },
                .{ .{ "a", "x", "y" }, .{1}, 1 },
                .{ .{ "a", "x", "z" }, .{2}, 1 },
                .{ .{ "a", "y", "x" }, .{1}, 1 },
                .{ .{ "a", "y", "y" }, .{1}, 1 },
                .{ .{ "a", "y", "z" }, .{2}, 1 },
                .{ .{ "a", "z", "x" }, .{2}, 1 },
                .{ .{ "a", "z", "y" }, .{2}, 1 },
                .{ .{ "a", "z", "z" }, .{2}, 1 },
                .{ .{ "c", "c", "c" }, .{0}, 1 },
                .{ .{ "e", "e", "e" }, .{0}, 1 },
            },
        );
        try testChangeBatchJoin(
            changes,
            .{
                .{2},
            },
            changes,
            1,
            .LeftThenRight,
            .{
                .{ .{ "a", "x", "x" }, .{0}, 1 },
                .{ .{ "a", "x", "y" }, .{1}, 1 },
                .{ .{ "a", "x", "z" }, .{2}, 1 },
                .{ .{ "a", "y", "x" }, .{1}, 1 },
                .{ .{ "a", "y", "y" }, .{1}, 1 },
                .{ .{ "a", "y", "z" }, .{2}, 1 },
                .{ .{ "c", "c", "c" }, .{0}, 1 },
                .{ .{ "e", "e", "e" }, .{0}, 1 },
            },
        );
        try testChangeBatchJoin(
            changes,
            .{
                .{100},
            },
            changes,
            2,
            .LeftThenRight,
            .{
                .{ .{ "a", "x" }, .{0}, 1 },
                .{ .{ "a", "y" }, .{1}, 1 },
                .{ .{ "a", "z" }, .{2}, 1 },
                .{ .{ "c", "c" }, .{0}, 1 },
                .{ .{ "e", "e" }, .{0}, 1 },
            },
        );
        try testChangeBatchJoin(
            changes,
            .{
                .{2},
            },
            changes,
            2,
            .LeftThenRight,
            .{
                .{ .{ "a", "x" }, .{0}, 1 },
                .{ .{ "a", "y" }, .{1}, 1 },
                .{ .{ "c", "c" }, .{0}, 1 },
                .{ .{ "e", "e" }, .{0}, 1 },
            },
        );
    }
}

pub fn testIndexGetCountForRowAsOf(
    anon_change_batches: anytype,
    anon_row: anytype,
    anon_timestamp: anytype,
    expected_count: isize,
) !void {
    var index = dida.core.Index.init(allocator);
    defer index.deinit();
    const change_batches = dida.sugar.coerceAnonTo(allocator, []dida.core.ChangeBatch, anon_change_batches);
    defer allocator.free(change_batches);
    var row = dida.sugar.coerceAnonTo(allocator, dida.core.Row, anon_row);
    defer row.deinit(allocator);
    var timestamp = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_timestamp);
    defer timestamp.deinit(allocator);
    for (change_batches) |change_batch| {
        try index.addChangeBatch(change_batch);
    }
    const actual_count = index.getCountForRowAsOf(row, timestamp);
    try std.testing.expectEqual(expected_count, actual_count);
}

test "test index get count for row as of" {
    const changes = .{
        .{
            .{ .{"a"}, .{1}, 1 },
        },
        .{
            .{ .{"b"}, .{0}, 1 },
        },
        .{
            .{ .{"c"}, .{0}, 1 },
        },
        .{
            .{ .{"a"}, .{2}, 2 },
        },
        .{
            .{ .{"e"}, .{0}, 1 },
        },
        .{
            .{ .{"f"}, .{0}, 1 },
        },
        .{
            .{ .{"a"}, .{0}, 1 },
        },
        .{
            .{ .{"h"}, .{3}, 1 },
            .{ .{"i"}, .{0}, 1 },
            .{ .{"c"}, .{0}, 2 },
            .{ .{"k"}, .{0}, 1 },
            .{ .{"l"}, .{0}, 1 },
            .{ .{"c"}, .{1}, -3 },
            .{ .{"n"}, .{0}, 1 },
            .{ .{"o"}, .{0}, 1 },
        },
    };
    try testIndexGetCountForRowAsOf(changes, .{"a"}, .{0}, 1);
    try testIndexGetCountForRowAsOf(changes, .{"a"}, .{1}, 2);
    try testIndexGetCountForRowAsOf(changes, .{"a"}, .{2}, 4);
    try testIndexGetCountForRowAsOf(changes, .{"a"}, .{3}, 4);
    try testIndexGetCountForRowAsOf(changes, .{"c"}, .{0}, 3);
    try testIndexGetCountForRowAsOf(changes, .{"c"}, .{1}, 0);
    try testIndexGetCountForRowAsOf(changes, .{"h"}, .{0}, 0);
    try testIndexGetCountForRowAsOf(changes, .{"h"}, .{3}, 1);
    try testIndexGetCountForRowAsOf(changes, .{"z"}, .{3}, 0);
}

pub fn testNodeOutput(shard: *dida.core.Shard, node: dida.core.Node, anon_expected_change_batches: anytype) !void {
    const expected_change_batches = dida.sugar.coerceAnonTo(allocator, []dida.core.ChangeBatch, anon_expected_change_batches);
    defer {
        for (expected_change_batches) |*expected_change_batch| expected_change_batch.deinit(allocator);
        allocator.free(expected_change_batches);
    }

    var actual_change_batches = std.ArrayList(dida.core.ChangeBatch).init(allocator);
    defer {
        for (actual_change_batches.items) |*actual_change_batch| actual_change_batch.deinit(allocator);
        actual_change_batches.deinit();
    }
    while (shard.popOutput(node)) |actual_change_batch|
        try actual_change_batches.append(actual_change_batch);

    const expected_len = expected_change_batches.len;
    const actual_len = actual_change_batches.items.len;
    var i: usize = 0;
    while (i < std.math.min(expected_len, actual_len) and
        dida.util.deepEqual(expected_change_batches[i].changes, actual_change_batches.items[i].changes))
        i += 1;
    if (i < std.math.max(expected_len, actual_len)) {
        dida.util.dump(.{ .expected = expected_change_batches[i..], .actual = actual_change_batches.items[i..] });
        return error.TestExpectedEqual;
    }
}

test "test shard graph reach" {
    var graph_builder = dida.core.GraphBuilder.init(allocator);
    defer graph_builder.deinit();

    const subgraph_0 = dida.core.Subgraph{ .id = 0 };
    const subgraph_1 = try graph_builder.addSubgraph(subgraph_0);

    const edges = try graph_builder.addNode(subgraph_0, .Input);
    const edges_1 = try graph_builder.addNode(subgraph_1, .{ .TimestampPush = .{ .input = edges } });
    const reach_future = try graph_builder.addNode(subgraph_1, .{ .TimestampIncrement = .{ .input = null } });
    const reach_index = try graph_builder.addNode(subgraph_1, .{ .Index = .{ .input = reach_future } });
    const distinct_reach_index = try graph_builder.addNode(subgraph_1, .{ .Distinct = .{ .input = reach_index } });
    var swapped_edges_mapper = dida.core.NodeSpec.MapSpec.Mapper{
        .map_fn = (struct {
            fn swap(_: *dida.core.NodeSpec.MapSpec.Mapper, input: dida.core.Row) error{OutOfMemory}!dida.core.Row {
                var output_values = try allocator.alloc(dida.core.Value, 2);
                output_values[0] = try dida.util.deepClone(input.values[1], allocator);
                output_values[1] = try dida.util.deepClone(input.values[0], allocator);
                return dida.core.Row{ .values = output_values };
            }
        }).swap,
    };
    const swapped_edges = try graph_builder.addNode(subgraph_1, .{
        .Map = .{
            .input = edges_1,
            .mapper = &swapped_edges_mapper,
        },
    });
    const swapped_edges_index = try graph_builder.addNode(subgraph_1, .{ .Index = .{ .input = swapped_edges } });
    const joined = try graph_builder.addNode(subgraph_1, .{
        .Join = .{
            .inputs = .{
                distinct_reach_index,
                swapped_edges_index,
            },
            .key_columns = 1,
        },
    });
    var without_middle_mapper = dida.core.NodeSpec.MapSpec.Mapper{
        .map_fn = (struct {
            fn drop_middle(_: *dida.core.NodeSpec.MapSpec.Mapper, input: dida.core.Row) error{OutOfMemory}!dida.core.Row {
                var output_values = try allocator.alloc(dida.core.Value, 2);
                output_values[0] = try dida.util.deepClone(input.values[2], allocator);
                output_values[1] = try dida.util.deepClone(input.values[1], allocator);
                return dida.core.Row{ .values = output_values };
            }
        }).drop_middle,
    };
    const without_middle = try graph_builder.addNode(subgraph_1, .{
        .Map = .{
            .input = joined,
            .mapper = &without_middle_mapper,
        },
    });
    const reach = try graph_builder.addNode(subgraph_1, .{ .Union = .{ .inputs = .{ edges_1, without_middle } } });
    graph_builder.connectLoop(reach, reach_future);
    const reach_pop = try graph_builder.addNode(subgraph_0, .{ .TimestampPop = .{ .input = distinct_reach_index } });
    const reach_out = try graph_builder.addNode(subgraph_0, .{ .Output = .{ .input = reach_pop } });

    var reducer = dida.core.NodeSpec.ReduceSpec.Reducer{
        .reduce_fn = (struct {
            fn concat(_: *dida.core.NodeSpec.ReduceSpec.Reducer, reduced_value: dida.core.Value, row: dida.core.Row, count: usize) !dida.core.Value {
                var string = std.ArrayList(u8).init(allocator);
                try string.appendSlice(reduced_value.String);
                var i: usize = 0;
                while (i < count) : (i += 1) {
                    try string.appendSlice(row.values[1].String);
                }
                return dida.core.Value{ .String = string.toOwnedSlice() };
            }
        }).concat,
    };
    const reach_summary = try graph_builder.addNode(subgraph_1, .{ .Reduce = .{
        .input = distinct_reach_index,
        .key_columns = 1,
        .init_value = .{ .String = "" },
        .reducer = &reducer,
    } });
    const reach_summary_out = try graph_builder.addNode(subgraph_1, .{ .Output = .{ .input = reach_summary } });

    var graph = try graph_builder.finishAndReset();
    defer graph.deinit();

    var shard = try dida.core.Shard.init(allocator, &graph);
    defer shard.deinit();

    const timestamp0 = dida.core.Timestamp{ .coords = &[_]u64{0} };
    const timestamp1 = dida.core.Timestamp{ .coords = &[_]u64{1} };
    const timestamp2 = dida.core.Timestamp{ .coords = &[_]u64{2} };

    const ab = dida.core.Row{ .values = &[_]dida.core.Value{ .{ .String = "a" }, .{ .String = "b" } } };
    const bc = dida.core.Row{ .values = &[_]dida.core.Value{ .{ .String = "b" }, .{ .String = "c" } } };
    const bd = dida.core.Row{ .values = &[_]dida.core.Value{ .{ .String = "b" }, .{ .String = "d" } } };
    const ca = dida.core.Row{ .values = &[_]dida.core.Value{ .{ .String = "c" }, .{ .String = "a" } } };

    try shard.pushInput(edges, .{ .row = try dida.util.deepClone(ab, allocator), .timestamp = try dida.util.deepClone(timestamp0, allocator), .diff = 1 });
    try shard.pushInput(edges, .{ .row = try dida.util.deepClone(bc, allocator), .timestamp = try dida.util.deepClone(timestamp0, allocator), .diff = 1 });
    try shard.pushInput(edges, .{ .row = try dida.util.deepClone(bd, allocator), .timestamp = try dida.util.deepClone(timestamp0, allocator), .diff = 1 });
    try shard.pushInput(edges, .{ .row = try dida.util.deepClone(ca, allocator), .timestamp = try dida.util.deepClone(timestamp0, allocator), .diff = 1 });
    try shard.pushInput(edges, .{ .row = try dida.util.deepClone(bc, allocator), .timestamp = try dida.util.deepClone(timestamp1, allocator), .diff = -1 });
    try shard.flushInput(edges);

    try shard.advanceInput(edges, timestamp1);
    while (shard.hasWork()) try shard.doWork();

    try testNodeOutput(&shard, reach_out, .{
        .{
            .{ .{ "a", "b" }, .{0}, 1 },
            .{ .{ "b", "c" }, .{0}, 1 },
            .{ .{ "b", "d" }, .{0}, 1 },
            .{ .{ "c", "a" }, .{0}, 1 },
        },
        .{
            .{ .{ "a", "c" }, .{0}, 1 },
            .{ .{ "a", "d" }, .{0}, 1 },
            .{ .{ "b", "a" }, .{0}, 1 },
            .{ .{ "c", "b" }, .{0}, 1 },
        },
        .{
            .{ .{ "a", "a" }, .{0}, 1 },
            .{ .{ "b", "b" }, .{0}, 1 },
            .{ .{ "c", "c" }, .{0}, 1 },
            .{ .{ "c", "d" }, .{0}, 1 },
        },
    });
    try testNodeOutput(&shard, reach_summary_out, .{
        .{
            .{ .{ "a", "b" }, .{ 0, 1 }, 1 },
            .{ .{ "b", "cd" }, .{ 0, 1 }, 1 },
            .{ .{ "c", "a" }, .{ 0, 1 }, 1 },
        },
        .{
            .{ .{ "a", "b" }, .{ 0, 2 }, -1 },
            .{ .{ "b", "cd" }, .{ 0, 2 }, -1 },
            .{ .{ "c", "a" }, .{ 0, 2 }, -1 },
            .{ .{ "a", "bcd" }, .{ 0, 2 }, 1 },
            .{ .{ "b", "acd" }, .{ 0, 2 }, 1 },
            .{ .{ "c", "ab" }, .{ 0, 2 }, 1 },
        },
        .{
            .{ .{ "a", "bcd" }, .{ 0, 3 }, -1 },
            .{ .{ "b", "acd" }, .{ 0, 3 }, -1 },
            .{ .{ "c", "ab" }, .{ 0, 3 }, -1 },
            .{ .{ "a", "abcd" }, .{ 0, 3 }, 1 },
            .{ .{ "b", "abcd" }, .{ 0, 3 }, 1 },
            .{ .{ "c", "abcd" }, .{ 0, 3 }, 1 },
        },
    });

    try shard.advanceInput(edges, timestamp2);
    while (shard.hasWork()) try shard.doWork();

    try testNodeOutput(&shard, reach_out, .{
        .{
            .{ .{ "b", "c" }, .{1}, -1 },
        },
        .{
            .{ .{ "a", "c" }, .{1}, -1 },
            .{ .{ "b", "a" }, .{1}, -1 },
        },
        .{
            .{ .{ "a", "a" }, .{1}, -1 },
            .{ .{ "b", "b" }, .{1}, -1 },
            .{ .{ "c", "c" }, .{1}, -1 },
        },
    });
    try testNodeOutput(&shard, reach_summary_out, .{
        .{
            .{ .{ "b", "cd" }, .{ 1, 1 }, -1 },
            .{ .{ "b", "d" }, .{ 1, 1 }, 1 },
        },
        .{
            .{ .{ "a", "bcd" }, .{ 1, 2 }, -1 },
            .{ .{ "b", "cd" }, .{ 1, 2 }, 1 },
            .{ .{ "b", "acd" }, .{ 1, 2 }, -1 },
            .{ .{ "a", "bd" }, .{ 1, 2 }, 1 },
        },
        .{
            .{ .{ "a", "bcd" }, .{ 1, 3 }, 1 },
            .{ .{ "b", "acd" }, .{ 1, 3 }, 1 },
            .{ .{ "a", "abcd" }, .{ 1, 3 }, -1 },
            .{ .{ "b", "abcd" }, .{ 1, 3 }, -1 },
            .{ .{ "c", "abcd" }, .{ 1, 3 }, -1 },
            .{ .{ "c", "abd" }, .{ 1, 3 }, 1 },
        },
    });
}

pub fn testShardTotalBalance() !void {
    var graph_builder = dida.core.GraphBuilder.init(allocator);
    defer graph_builder.deinit();

    const subgraph_0 = dida.core.Subgraph{ .id = 0 };

    // transactions look like (from, to, amount)
    const transactions = try graph_builder.addNode(subgraph_0, .Input);

    var credits_mapper = dida.core.NodeSpec.MapSpec.Mapper{
        .map_fn = (struct {
            fn map(_: *dida.core.NodeSpec.MapSpec.Mapper, input: dida.core.Row) error{OutOfMemory}!dida.core.Row {
                // (to, amount)
                var output_values = try allocator.alloc(dida.core.Value, 2);
                output_values[0] = try dida.util.deepClone(input.values[1], allocator);
                output_values[1] = try dida.util.deepClone(input.values[2], allocator);
                return dida.core.Row{ .values = output_values };
            }
        }).map,
    };
    const account_credits = try graph_builder.addNode(subgraph_0, .{ .Map = .{
        .input = transactions,
        .mapper = &credits_mapper,
    } });
    const account_credits_index = try graph_builder.addNode(subgraph_0, .{ .Index = .{ .input = account_credits } });

    var debits_mapper = dida.core.NodeSpec.MapSpec.Mapper{
        .map_fn = (struct {
            fn map(_: *dida.core.NodeSpec.MapSpec.Mapper, input: dida.core.Row) error{OutOfMemory}!dida.core.Row {
                // (from, amount)
                var output_values = try allocator.alloc(dida.core.Value, 2);
                output_values[0] = try dida.util.deepClone(input.values[0], allocator);
                output_values[1] = try dida.util.deepClone(input.values[2], allocator);
                return dida.core.Row{ .values = output_values };
            }
        }).map,
    };
    const account_debits = try graph_builder.addNode(subgraph_0, .{ .Map = .{
        .input = transactions,
        .mapper = &debits_mapper,
    } });
    const account_debits_index = try graph_builder.addNode(subgraph_0, .{ .Index = .{ .input = account_debits } });

    var summer = dida.core.NodeSpec.ReduceSpec.Reducer{
        .reduce_fn = (struct {
            fn sum(_: *dida.core.NodeSpec.ReduceSpec.Reducer, reduced_value: dida.core.Value, row: dida.core.Row, count: usize) !dida.core.Value {
                return dida.core.Value{ .Number = reduced_value.Number + (row.values[1].Number * @intToFloat(f64, count)) };
            }
        }).sum,
    };
    const account_credit = try graph_builder.addNode(subgraph_0, .{ .Reduce = .{
        .input = account_credits_index,
        .key_columns = 1,
        .init_value = .{ .Number = 0 },
        .reducer = &summer,
    } });
    const account_debit = try graph_builder.addNode(subgraph_0, .{ .Reduce = .{
        .input = account_debits_index,
        .key_columns = 1,
        .init_value = .{ .Number = 0 },
        .reducer = &summer,
    } });

    const credit_and_debit = try graph_builder.addNode(subgraph_0, .{ .Join = .{
        .inputs = .{
            account_credit,
            account_debit,
        },
        .key_columns = 1,
    } });

    var balance_mapper = dida.core.NodeSpec.MapSpec.Mapper{
        .map_fn = (struct {
            fn map(_: *dida.core.NodeSpec.MapSpec.Mapper, input: dida.core.Row) error{OutOfMemory}!dida.core.Row {
                // (account, credit - debit)
                var output_values = try allocator.alloc(dida.core.Value, 2);
                output_values[0] = try dida.util.deepClone(input.values[0], allocator);
                output_values[1] = .{ .Number = input.values[1].Number - input.values[2].Number };
                return dida.core.Row{ .values = output_values };
            }
        }).map,
    };
    const balance = try graph_builder.addNode(subgraph_0, .{ .Map = .{
        .input = credit_and_debit,
        .mapper = &balance_mapper,
    } });
    const balance_index = try graph_builder.addNode(subgraph_0, .{ .Index = .{ .input = balance } });

    const total_balance = try graph_builder.addNode(subgraph_0, .{ .Reduce = .{
        .input = balance_index,
        .key_columns = 0,
        .init_value = .{ .Number = 0 },
        .reducer = &summer,
    } });
    const total_balance_out = try graph_builder.addNode(subgraph_0, .{ .Output = .{ .input = total_balance } });

    var graph = try graph_builder.finishAndReset();
    defer graph.deinit();

    var shard = try dida.core.Shard.init(allocator, &graph);
    defer shard.deinit();

    // TODO this is a hack to get around the fact that empty reduces don't return any results, which makes the join not work out
    var account: usize = 0;
    while (account <= std.math.maxInt(u4)) : (account += 1) {
        const row = dida.core.Row{ .values = &[_]dida.core.Value{
            .{ .Number = @intToFloat(f64, account) },
            .{ .Number = @intToFloat(f64, account) },
            .{ .Number = @intToFloat(f64, 0) },
        } };
        const timestamp = dida.core.Timestamp{ .coords = &[_]u64{0} };
        try shard.pushInput(transactions, .{ .row = try dida.util.deepClone(row, allocator), .timestamp = try dida.util.deepClone(timestamp, allocator), .diff = 1 });
    }
    try shard.advanceInput(transactions, .{ .coords = &[_]u64{1} });

    while (shard.hasWork()) try shard.doWork();
    try testNodeOutput(&shard, total_balance_out, .{.{.{ .{0}, .{0}, 1 }}});

    var rng = std.rand.DefaultPrng.init(0);
    var time: usize = 1;
    while (time < 100) : (time += 1) {
        const from_account = rng.random.int(u4);
        const to_account = rng.random.int(u4);
        const amount = rng.random.int(u8);
        const skew = rng.random.int(u3);
        const row = dida.core.Row{ .values = &[_]dida.core.Value{
            .{ .Number = @intToFloat(f64, from_account) },
            .{ .Number = @intToFloat(f64, to_account) },
            .{ .Number = @intToFloat(f64, amount) },
        } };
        const timestamp = dida.core.Timestamp{ .coords = &[_]u64{time + @as(usize, skew)} };
        try shard.pushInput(transactions, .{ .row = try dida.util.deepClone(row, allocator), .timestamp = try dida.util.deepClone(timestamp, allocator), .diff = 1 });
        try shard.advanceInput(transactions, .{ .coords = &[_]u64{time + 1} });
        while (shard.hasWork()) try shard.doWork();
        try testNodeOutput(&shard, total_balance_out, .{});
    }
}

test "test shard total balance" {
    try testShardTotalBalance();
}
