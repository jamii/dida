const std = @import("std");
const dida = @import("../lib/dida.zig");

fn expectDeepEqual(expected: anytype, actual: anytype) !void {
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
    const input_changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_input_changes);
    const expected_changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_expected_changes);
    var builder = dida.core.ChangeBatchBuilder.init(allocator);
    for (input_changes) |change| {
        try builder.changes.append(change);
    }
    if (try builder.finishAndReset()) |batch| {
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
    const frontier_timestamps = dida.sugar.coerceAnonTo(allocator, []dida.core.Timestamp, anon_frontier);
    var frontier = dida.core.Frontier.init(allocator);
    for (frontier_timestamps) |frontier_timestamp| {
        var changes_into = std.ArrayList(dida.core.FrontierChange).init(allocator);
        try frontier.move(.Later, frontier_timestamp, &changes_into);
    }
    const timestamp = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_timestamp);
    const expected_frontier_timestamps = dida.sugar.coerceAnonTo(allocator, []dida.core.Timestamp, anon_expected_frontier);
    const expected_changes = dida.sugar.coerceAnonTo(allocator, []dida.core.FrontierChange, anon_expected_changes);
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
    try expectDeepEqual(expected_frontier_timestamps, actual_frontier_timestamps.items);
    try expectDeepEqual(expected_changes, actual_changes_into.items);
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
    const frontier_timestamps = dida.sugar.coerceAnonTo(allocator, []dida.core.Timestamp, anon_frontier);
    var frontier = dida.core.Frontier.init(allocator);
    for (frontier_timestamps) |frontier_timestamp| {
        var changes_into = std.ArrayList(dida.core.FrontierChange).init(allocator);
        try frontier.move(.Later, frontier_timestamp, &changes_into);
    }
    const timestamp = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_timestamp);
    try std.testing.expectEqual(expected_order, frontier.causalOrder(timestamp));
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
    const support = dida.sugar.coerceAnonTo(allocator, []dida.core.FrontierChange, anon_support);
    var frontier = try dida.core.SupportedFrontier.init(allocator);
    for (support) |change| {
        var changes_into = std.ArrayList(dida.core.FrontierChange).init(allocator);
        try frontier.update(change.timestamp, change.diff, &changes_into);
    }
    const update = dida.sugar.coerceAnonTo(allocator, dida.core.FrontierChange, anon_update);
    const expected_frontier_timestamps = dida.sugar.coerceAnonTo(allocator, []dida.core.Timestamp, anon_expected_frontier);
    const expected_changes = dida.sugar.coerceAnonTo(allocator, []dida.core.FrontierChange, anon_expected_changes);
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
    try expectDeepEqual(expected_frontier_timestamps, actual_frontier_timestamps.items);
    try expectDeepEqual(expected_changes, actual_changes_into.items);
}

test "test supported frontier update" {
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

pub fn testIndexAdd(
    allocator: *std.mem.Allocator,
    anon_change_batches: anytype,
    anon_expected_changes: anytype,
) !void {
    var index = dida.core.Index.init(allocator);
    const change_batches = dida.sugar.coerceAnonTo(allocator, []dida.core.ChangeBatch, anon_change_batches);
    const expected_changes = dida.sugar.coerceAnonTo(allocator, [][]dida.core.Change, anon_expected_changes);
    for (change_batches) |change_batch| {
        try index.addChangeBatch(change_batch);
    }
    const actual_changes = try allocator.alloc([]dida.core.Change, index.change_batches.items.len);
    for (actual_changes) |*changes, i| changes.* = index.change_batches.items[i].changes;
    try expectDeepEqual(expected_changes, actual_changes);
}

test "test index add" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = &arena.allocator;
    try testIndexAdd(
        a,
        .{},
        .{},
    );
    try testIndexAdd(
        a,
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
        a,
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
        a,
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
        a,
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
        a,
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
        a,
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
    allocator: *std.mem.Allocator,
    anon_changes: anytype,
    ix: usize,
    anon_row: anytype,
    key_columns: usize,
    expected_ix: usize,
) !void {
    const changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_changes);
    const row = dida.sugar.coerceAnonTo(allocator, dida.core.Row, anon_row);

    var builder = dida.core.ChangeBatchBuilder.init(allocator);
    for (changes) |change| {
        try builder.changes.append(change);
    }
    const change_batch = (try builder.finishAndReset()).?;

    const actual_ix = change_batch.seekRowStart(ix, row, key_columns);
    try std.testing.expectEqual(expected_ix, actual_ix);
}

test "test change batch seek row start" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = &arena.allocator;
    const changes = .{
        .{ .{ "a", "x" }, .{0}, 1 },
        .{ .{ "a", "y" }, .{1}, 1 },
        .{ .{ "a", "z" }, .{2}, 1 },
        .{ .{ "c", "c" }, .{0}, 1 },
        .{ .{ "e", "e" }, .{0}, 1 },
    };

    try testChangeBatchSeekRowStart(a, changes, 0, .{"a"}, 1, 0);
    try testChangeBatchSeekRowStart(a, changes, 1, .{"a"}, 1, 1);
    try testChangeBatchSeekRowStart(a, changes, 2, .{"a"}, 1, 2);
    try testChangeBatchSeekRowStart(a, changes, 3, .{"a"}, 1, 3);
    try testChangeBatchSeekRowStart(a, changes, 4, .{"a"}, 1, 4);
    try testChangeBatchSeekRowStart(a, changes, 5, .{"a"}, 1, 5);

    try testChangeBatchSeekRowStart(a, changes, 0, .{"b"}, 1, 3);
    try testChangeBatchSeekRowStart(a, changes, 1, .{"b"}, 1, 3);
    try testChangeBatchSeekRowStart(a, changes, 2, .{"b"}, 1, 3);
    try testChangeBatchSeekRowStart(a, changes, 3, .{"b"}, 1, 3);
    try testChangeBatchSeekRowStart(a, changes, 4, .{"b"}, 1, 4);
    try testChangeBatchSeekRowStart(a, changes, 5, .{"b"}, 1, 5);

    try testChangeBatchSeekRowStart(a, changes, 0, .{"c"}, 1, 3);
    try testChangeBatchSeekRowStart(a, changes, 1, .{"c"}, 1, 3);
    try testChangeBatchSeekRowStart(a, changes, 2, .{"c"}, 1, 3);
    try testChangeBatchSeekRowStart(a, changes, 3, .{"c"}, 1, 3);
    try testChangeBatchSeekRowStart(a, changes, 4, .{"c"}, 1, 4);
    try testChangeBatchSeekRowStart(a, changes, 5, .{"c"}, 1, 5);

    try testChangeBatchSeekRowStart(a, changes, 0, .{"d"}, 1, 4);
    try testChangeBatchSeekRowStart(a, changes, 1, .{"d"}, 1, 4);
    try testChangeBatchSeekRowStart(a, changes, 2, .{"d"}, 1, 4);
    try testChangeBatchSeekRowStart(a, changes, 3, .{"d"}, 1, 4);
    try testChangeBatchSeekRowStart(a, changes, 4, .{"d"}, 1, 4);
    try testChangeBatchSeekRowStart(a, changes, 5, .{"d"}, 1, 5);

    try testChangeBatchSeekRowStart(a, changes, 0, .{"e"}, 1, 4);
    try testChangeBatchSeekRowStart(a, changes, 1, .{"e"}, 1, 4);
    try testChangeBatchSeekRowStart(a, changes, 2, .{"e"}, 1, 4);
    try testChangeBatchSeekRowStart(a, changes, 3, .{"e"}, 1, 4);
    try testChangeBatchSeekRowStart(a, changes, 4, .{"e"}, 1, 4);
    try testChangeBatchSeekRowStart(a, changes, 5, .{"e"}, 1, 5);

    try testChangeBatchSeekRowStart(a, changes, 0, .{"f"}, 1, 5);
    try testChangeBatchSeekRowStart(a, changes, 1, .{"f"}, 1, 5);
    try testChangeBatchSeekRowStart(a, changes, 2, .{"f"}, 1, 5);
    try testChangeBatchSeekRowStart(a, changes, 3, .{"f"}, 1, 5);
    try testChangeBatchSeekRowStart(a, changes, 4, .{"f"}, 1, 5);
    try testChangeBatchSeekRowStart(a, changes, 5, .{"f"}, 1, 5);
}

fn testChangeBatchSeekRowEnd(
    allocator: *std.mem.Allocator,
    anon_changes: anytype,
    ix: usize,
    anon_row: anytype,
    key_columns: usize,
    expected_ix: usize,
) !void {
    const changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_changes);
    const row = dida.sugar.coerceAnonTo(allocator, dida.core.Row, anon_row);

    var builder = dida.core.ChangeBatchBuilder.init(allocator);
    for (changes) |change| {
        try builder.changes.append(change);
    }
    const change_batch = (try builder.finishAndReset()).?;

    const actual_ix = change_batch.seekRowEnd(ix, row, key_columns);
    try std.testing.expectEqual(expected_ix, actual_ix);
}

test "test change batch seek current row end" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = &arena.allocator;
    const changes = .{
        .{ .{ "a", "x" }, .{0}, 1 },
        .{ .{ "a", "y" }, .{1}, 1 },
        .{ .{ "a", "z" }, .{2}, 1 },
        .{ .{ "c", "c" }, .{0}, 1 },
        .{ .{ "e", "e" }, .{0}, 1 },
    };

    try testChangeBatchSeekRowEnd(a, changes, 0, .{"a"}, 1, 3);
    try testChangeBatchSeekRowEnd(a, changes, 1, .{"a"}, 1, 3);
    try testChangeBatchSeekRowEnd(a, changes, 2, .{"a"}, 1, 3);
    try testChangeBatchSeekRowEnd(a, changes, 3, .{"a"}, 1, 3);
    try testChangeBatchSeekRowEnd(a, changes, 4, .{"a"}, 1, 4);
    try testChangeBatchSeekRowEnd(a, changes, 5, .{"a"}, 1, 5);

    try testChangeBatchSeekRowEnd(a, changes, 0, .{"b"}, 1, 3);
    try testChangeBatchSeekRowEnd(a, changes, 1, .{"b"}, 1, 3);
    try testChangeBatchSeekRowEnd(a, changes, 2, .{"b"}, 1, 3);
    try testChangeBatchSeekRowEnd(a, changes, 3, .{"b"}, 1, 3);
    try testChangeBatchSeekRowEnd(a, changes, 4, .{"b"}, 1, 4);
    try testChangeBatchSeekRowEnd(a, changes, 5, .{"b"}, 1, 5);

    try testChangeBatchSeekRowEnd(a, changes, 0, .{"c"}, 1, 4);
    try testChangeBatchSeekRowEnd(a, changes, 1, .{"c"}, 1, 4);
    try testChangeBatchSeekRowEnd(a, changes, 2, .{"c"}, 1, 4);
    try testChangeBatchSeekRowEnd(a, changes, 3, .{"c"}, 1, 4);
    try testChangeBatchSeekRowEnd(a, changes, 4, .{"c"}, 1, 4);
    try testChangeBatchSeekRowEnd(a, changes, 5, .{"c"}, 1, 5);

    try testChangeBatchSeekRowEnd(a, changes, 0, .{"d"}, 1, 4);
    try testChangeBatchSeekRowEnd(a, changes, 1, .{"d"}, 1, 4);
    try testChangeBatchSeekRowEnd(a, changes, 2, .{"d"}, 1, 4);
    try testChangeBatchSeekRowEnd(a, changes, 3, .{"d"}, 1, 4);
    try testChangeBatchSeekRowEnd(a, changes, 4, .{"d"}, 1, 4);
    try testChangeBatchSeekRowEnd(a, changes, 5, .{"d"}, 1, 5);

    try testChangeBatchSeekRowEnd(a, changes, 0, .{"e"}, 1, 5);
    try testChangeBatchSeekRowEnd(a, changes, 1, .{"e"}, 1, 5);
    try testChangeBatchSeekRowEnd(a, changes, 2, .{"e"}, 1, 5);
    try testChangeBatchSeekRowEnd(a, changes, 3, .{"e"}, 1, 5);
    try testChangeBatchSeekRowEnd(a, changes, 4, .{"e"}, 1, 5);
    try testChangeBatchSeekRowEnd(a, changes, 5, .{"e"}, 1, 5);

    try testChangeBatchSeekRowEnd(a, changes, 0, .{"f"}, 1, 5);
    try testChangeBatchSeekRowEnd(a, changes, 1, .{"f"}, 1, 5);
    try testChangeBatchSeekRowEnd(a, changes, 2, .{"f"}, 1, 5);
    try testChangeBatchSeekRowEnd(a, changes, 3, .{"f"}, 1, 5);
    try testChangeBatchSeekRowEnd(a, changes, 4, .{"f"}, 1, 5);
    try testChangeBatchSeekRowEnd(a, changes, 5, .{"f"}, 1, 5);
}

fn testChangeBatchSeekCurrentRowEnd(
    allocator: *std.mem.Allocator,
    anon_changes: anytype,
    ix: usize,
    key_columns: usize,
    expected_ix: usize,
) !void {
    const changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_changes);

    var builder = dida.core.ChangeBatchBuilder.init(allocator);
    for (changes) |change| {
        try builder.changes.append(change);
    }
    const change_batch = (try builder.finishAndReset()).?;

    const actual_ix = change_batch.seekCurrentRowEnd(ix, key_columns);
    try std.testing.expectEqual(expected_ix, actual_ix);
}

test "test change batch seek current row end" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = &arena.allocator;
    const changes = .{
        .{ .{ "a", "x" }, .{0}, 1 },
        .{ .{ "a", "y" }, .{1}, 1 },
        .{ .{ "a", "z" }, .{2}, 1 },
        .{ .{ "c", "c" }, .{0}, 1 },
        .{ .{ "e", "e" }, .{0}, 1 },
    };
    try testChangeBatchSeekCurrentRowEnd(a, changes, 0, 1, 3);
    try testChangeBatchSeekCurrentRowEnd(a, changes, 1, 1, 3);
    try testChangeBatchSeekCurrentRowEnd(a, changes, 2, 1, 3);
    try testChangeBatchSeekCurrentRowEnd(a, changes, 3, 1, 4);
    try testChangeBatchSeekCurrentRowEnd(a, changes, 4, 1, 5);
    try testChangeBatchSeekCurrentRowEnd(a, changes, 5, 1, 5);
}

fn testChangeBatchJoin(
    allocator: *std.mem.Allocator,
    anon_left_changes: anytype,
    anon_right_changes: anytype,
    key_columns: usize,
    anon_expected_changes: anytype,
) !void {
    const left_changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_left_changes);
    const right_changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_right_changes);
    const expected_changes = dida.sugar.coerceAnonTo(allocator, []dida.core.Change, anon_expected_changes);

    var left_builder = dida.core.ChangeBatchBuilder.init(allocator);
    for (left_changes) |change| {
        try left_builder.changes.append(change);
    }
    const left_change_batch = (try left_builder.finishAndReset()).?;

    var right_builder = dida.core.ChangeBatchBuilder.init(allocator);
    for (right_changes) |change| {
        try right_builder.changes.append(change);
    }
    const right_change_batch = (try right_builder.finishAndReset()).?;

    var output_builder = dida.core.ChangeBatchBuilder.init(allocator);
    try left_change_batch.mergeJoin(right_change_batch, key_columns, &output_builder);
    if (try output_builder.finishAndReset()) |output_change_batch| {
        try expectDeepEqual(expected_changes, output_change_batch.changes);
    } else {
        const actual_changes: []dida.core.Change = &[0]dida.core.Change{};
        try expectDeepEqual(expected_changes, actual_changes);
    }
}

test "test change batch join" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = &arena.allocator;
    try testChangeBatchJoin(
        a,
        .{
            .{ .{"a"}, .{ 0, 1 }, 2 },
        },
        .{
            .{ .{"a"}, .{ 1, 0 }, 3 },
        },
        1,
        .{
            .{ .{"a"}, .{ 1, 1 }, 6 },
        },
    );
    try testChangeBatchJoin(
        a,
        .{
            .{ .{"a"}, .{ 0, 1 }, 2 },
        },
        .{
            .{ .{"b"}, .{ 1, 0 }, 3 },
        },
        1,
        .{},
    );
    try testChangeBatchJoin(
        a,
        .{
            .{ .{"a"}, .{ 0, 1 }, 2 },
            .{ .{"a"}, .{ 0, 2 }, 5 },
        },
        .{
            .{ .{"a"}, .{ 1, 0 }, 3 },
            .{ .{"a"}, .{ 2, 0 }, 7 },
        },
        1,
        .{
            .{ .{"a"}, .{ 1, 1 }, 6 },
            .{ .{"a"}, .{ 1, 2 }, 15 },
            .{ .{"a"}, .{ 2, 1 }, 14 },
            .{ .{"a"}, .{ 2, 2 }, 35 },
        },
    );
    try testChangeBatchJoin(
        a,
        .{
            .{ .{"a"}, .{ 0, 1 }, 2 },
            .{ .{"b"}, .{ 0, 2 }, 5 },
        },
        .{
            .{ .{"a"}, .{ 1, 0 }, 3 },
            .{ .{"b"}, .{ 2, 0 }, 7 },
        },
        1,
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
            a,
            changes,
            changes,
            1,
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
            a,
            changes,
            changes,
            1,
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
            a,
            changes,
            changes,
            2,
            .{
                .{ .{ "a", "x" }, .{0}, 1 },
                .{ .{ "a", "y" }, .{1}, 1 },
                .{ .{ "a", "z" }, .{2}, 1 },
                .{ .{ "c", "c" }, .{0}, 1 },
                .{ .{ "e", "e" }, .{0}, 1 },
            },
        );
    }
}

pub fn testIndexGetCountForRowAsOf(
    allocator: *std.mem.Allocator,
    anon_change_batches: anytype,
    anon_row: anytype,
    anon_timestamp: anytype,
    expected_count: isize,
) !void {
    var index = dida.core.Index.init(allocator);
    const change_batches = dida.sugar.coerceAnonTo(allocator, []dida.core.ChangeBatch, anon_change_batches);
    const row = dida.sugar.coerceAnonTo(allocator, dida.core.Row, anon_row);
    const timestamp = dida.sugar.coerceAnonTo(allocator, dida.core.Timestamp, anon_timestamp);
    for (change_batches) |change_batch| {
        try index.addChangeBatch(change_batch);
    }
    const actual_count = index.getCountForRowAsOf(row, timestamp);
    try std.testing.expectEqual(expected_count, actual_count);
}

test "test index get count for row as of" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = &arena.allocator;
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
    try testIndexGetCountForRowAsOf(a, changes, .{"a"}, .{0}, 1);
    try testIndexGetCountForRowAsOf(a, changes, .{"a"}, .{1}, 2);
    try testIndexGetCountForRowAsOf(a, changes, .{"a"}, .{2}, 4);
    try testIndexGetCountForRowAsOf(a, changes, .{"a"}, .{3}, 4);
    try testIndexGetCountForRowAsOf(a, changes, .{"c"}, .{0}, 3);
    try testIndexGetCountForRowAsOf(a, changes, .{"c"}, .{1}, 0);
    try testIndexGetCountForRowAsOf(a, changes, .{"h"}, .{0}, 0);
    try testIndexGetCountForRowAsOf(a, changes, .{"h"}, .{3}, 1);
    try testIndexGetCountForRowAsOf(a, changes, .{"z"}, .{3}, 0);
}

// TODO:
//   ProgressTracker
//   Graph (make validate return error?)
//   whole dataflows
