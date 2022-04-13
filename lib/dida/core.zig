//! The core of dida handles all the actual computation.
//! It exposes an api that is maximally flexible but also verbose and error-prone.
//! See ./sugar.zig for a friendlier layer on top of the core.
//!
//! Assume that all struct parameters are owned unless otherwise stated.
//! Assume all function arguments are borrowed unless otherwise stated.

const std = @import("std");
const dida = @import("../dida.zig");
const u = dida.util;

/// The basic unit of data in dida.
pub const Value = union(enum) {
    String: []const u8,
    Number: f64,

    pub fn deinit(self: *Value, allocator: u.Allocator) void {
        switch (self.*) {
            .String => |string| allocator.free(string),
            .Number => {},
        }
        self.* = undefined;
    }
};

/// Every operation takes rows as inputs and produces rows as outputs.
// TODO This will eventually be replaced by raw bytes plus an optional type tag, so that users of dida can use whatever values and serde scheme they want.
pub const Row = struct {
    values: []const Value,

    pub fn deinit(self: *Row, allocator: u.Allocator) void {
        for (self.values) |_value| {
            // can't deinit through []const
            var value = _value;
            value.deinit(allocator);
        }
        allocator.free(self.values);
        self.* = undefined;
    }
};

/// A [bag](https://en.wikipedia.org/wiki/Multiset) of rows.
/// The dataflow is a graph of operations, each of which takes one or more bags of rows as inputs and produces a bag of rows as outputs.
pub const Bag = struct {
    /// Rows are all borrowed.
    rows: u.DeepHashMap(Row, isize),

    pub fn init(allocator: u.Allocator) Bag {
        return .{
            .rows = u.DeepHashMap(Row, isize).init(allocator),
        };
    }

    pub fn deinit(self: *Bag) void {
        // rows are all borrowed so no need to free them
        self.rows.deinit();
        self.* = undefined;
    }

    pub fn update(self: *Bag, row: Row, diff: isize) !void {
        const entry = try self.rows.getOrPutValue(row, 0);
        entry.value_ptr.* += diff;
        _ = if (entry.value_ptr.* == 0) self.rows.remove(row);
    }
};

/// The result of comparing two elements in a [partially ordered set](https://en.wikipedia.org/wiki/Partially_ordered_set).
/// (Field names are weird to be consistent with std.math.Order)
pub const PartialOrder = enum {
    lt,
    eq,
    gt,
    none,

    pub fn isLessThanOrEqual(self: PartialOrder) bool {
        return switch (self) {
            .lt, .eq => true,
            .gt, .none => false,
        };
    }
};

/// > Time is what prevents everything from happening all at once.
///
/// Timestamps represent the logical time something happened.
/// The first coord represents the logical time in the dataflow as a whole.
/// Each extra coord represent the iteration number within some enclosing loop in the dataflow (outermost loop first, innermost loop last).
pub const Timestamp = struct {
    coords: []const usize,

    pub fn initLeast(allocator: u.Allocator, num_coords: usize) !Timestamp {
        var coords = try allocator.alloc(usize, num_coords);
        for (coords) |*coord| coord.* = 0;
        return Timestamp{ .coords = coords };
    }

    pub fn deinit(self: *Timestamp, allocator: u.Allocator) void {
        allocator.free(self.coords);
        self.* = undefined;
    }

    pub fn pushCoord(self: Timestamp, allocator: u.Allocator) !Timestamp {
        var new_coords = try allocator.alloc(usize, self.coords.len + 1);
        std.mem.copy(usize, new_coords, self.coords);
        new_coords[new_coords.len - 1] = 0;
        return Timestamp{ .coords = new_coords };
    }

    pub fn incrementCoord(self: Timestamp, allocator: u.Allocator) !Timestamp {
        var new_coords = try allocator.dupe(usize, self.coords[0..self.coords.len]);
        new_coords[new_coords.len - 1] += 1;
        return Timestamp{ .coords = new_coords };
    }

    pub fn popCoord(self: Timestamp, allocator: u.Allocator) !Timestamp {
        u.assert(self.coords.len > 0, "Tried to call popCoord on a timestamp with length 0", .{});
        const new_coords = try allocator.dupe(usize, self.coords[0 .. self.coords.len - 1]);
        return Timestamp{ .coords = new_coords };
    }

    /// A partial ordering on timestamps such that if a change at timestamp A could ever cause a change at timestamp B, then A <= B.
    /// This is used to process changes in an order that is guaranteed to converge, and to define the behavior of frontiers.
    pub fn causalOrder(self: Timestamp, other: Timestamp) PartialOrder {
        u.assert(self.coords.len == other.coords.len, "Tried to compute causalOrder of timestamps with different lengths: {} vs {}", .{ self.coords.len, other.coords.len });
        var lt: usize = 0;
        var gt: usize = 0;
        var eq: usize = 0;
        for (self.coords) |self_coord, i| {
            const other_coord = other.coords[i];
            switch (std.math.order(self_coord, other_coord)) {
                .lt => lt += 1,
                .eq => eq += 1,
                .gt => gt += 1,
            }
        }
        if (eq == self.coords.len) return .eq;
        if (lt + eq == self.coords.len) return .lt;
        if (gt + eq == self.coords.len) return .gt;
        return .none;
    }

    /// A total ordering on timestamps that is compatible with the causal order.
    /// ie If `a.causalOrder(b) != .none` then `a.causalOrder(b) == a.lexicalOrder(b)`.
    /// This is useful if you want to sort Timestamps by causal order - standard sorting algorithms don't always work well on partial orders.
    pub fn lexicalOrder(self: Timestamp, other: Timestamp) std.math.Order {
        u.assert(self.coords.len == other.coords.len, "Tried to compute lexicalOrder of timestamps with different lengths: {} vs {}", .{ self.coords.len, other.coords.len });
        for (self.coords) |self_coord, i| {
            const other_coord = other.coords[i];
            switch (std.math.order(self_coord, other_coord)) {
                .lt => return .lt,
                .eq => {},
                .gt => return .gt,
            }
        }
        return .eq;
    }

    /// Returns the earliest timestamp that is greater than both the inputs (in the causal ordering).
    pub fn leastUpperBound(allocator: u.Allocator, self: Timestamp, other: Timestamp) !Timestamp {
        u.assert(self.coords.len == other.coords.len, "Tried to compute leastUpperBound of timestamps with different lengths: {} vs {}", .{ self.coords.len, other.coords.len });
        var output_coords = try allocator.alloc(usize, self.coords.len);
        for (self.coords) |self_coord, i| {
            const other_coord = other.coords[i];
            output_coords[i] = u.max(self_coord, other_coord);
        }
        return Timestamp{ .coords = output_coords };
    }
};

/// A frontier represents the earliest timestamps in some set of timestamps (by causal order).
/// It's used to track progress in the dataflow and also to summarize the contents of a change batch.
pub const Frontier = struct {
    allocator: u.Allocator,
    /// Invariant: timestamps don't overlap - for any two timestamps t1 and t2 in timestamps `t1.causalOrder(t2) == .none`
    timestamps: u.DeepHashSet(Timestamp),

    pub fn init(allocator: u.Allocator) Frontier {
        return Frontier{
            .allocator = allocator,
            .timestamps = u.DeepHashSet(Timestamp).init(allocator),
        };
    }

    pub fn deinit(self: *Frontier) void {
        {
            var iter = self.timestamps.iterator();
            while (iter.next()) |entry| {
                entry.key_ptr.deinit(self.allocator);
            }
        }
        self.timestamps.deinit();
        self.* = undefined;
    }

    /// Compares `timestamp` to `self.timestamps`.
    pub fn causalOrder(self: Frontier, timestamp: Timestamp) PartialOrder {
        var iter = self.timestamps.iterator();
        while (iter.next()) |entry| {
            const order = entry.key_ptr.causalOrder(timestamp);
            // Since the timestamps in `self.timestamps` are always mututally incomparable, we can never have `t1 < timestamp < t2`.
            // So it's safe to return as soon as we find some comparison.
            switch (order) {
                .lt => return .lt,
                .eq => return .eq,
                .gt => return .gt,
                .none => {},
            }
        }
        return .none;
    }

    pub const Direction = enum { Later, Earlier };

    /// Mutate `self` to a later (or earlier) frontier.
    /// Remove any timestamps that are earlier (or later) than `timestamp`.
    /// Reports any changes to the frontier into `changes_into`.
    pub fn move(self: *Frontier, comptime direction: Direction, timestamp: Timestamp, changes_into: *u.ArrayList(FrontierChange)) !void {
        u.assert(changes_into.items.len == 0, "Need to start with an empty changes_into buffer so can use it to remove timestamps", .{});
        var iter = self.timestamps.iterator();
        while (iter.next()) |entry| {
            switch (timestamp.causalOrder(entry.key_ptr.*)) {
                .eq, if (direction == .Later) .lt else .gt => {
                    // Moved in the wrong direction
                    u.assert(changes_into.items.len == 0, "Frontier timestamps invariant was broken", .{});
                    return;
                },
                if (direction == .Later) .gt else .lt => {
                    try changes_into.append(.{ .timestamp = entry.key_ptr.*, .diff = -1 });
                },
                .none => {},
            }
        }
        // If we got this far, timestamp is being added to the frontier and might also be replacing some other timestamps that are currently on the frontier
        for (changes_into.items) |frontier_change| {
            _ = self.timestamps.remove(frontier_change.timestamp);
        }
        try changes_into.append(.{ .timestamp = try u.deepClone(timestamp, self.allocator), .diff = 1 });
        try self.timestamps.put(try u.deepClone(timestamp, self.allocator), {});
    }
};

/// Tracks both a bag of timestamps and the frontier of that bag.
/// This is used to incrementally compute the frontiers of each node in the graph as the dataflow progresses.
pub const SupportedFrontier = struct {
    allocator: u.Allocator,
    support: u.DeepHashMap(Timestamp, usize),
    // Invariant: frontier contains exactly the least timestamps from support
    frontier: Frontier,

    pub fn init(allocator: u.Allocator) !SupportedFrontier {
        return SupportedFrontier{
            .allocator = allocator,
            .support = u.DeepHashMap(Timestamp, usize).init(allocator),
            .frontier = Frontier.init(allocator),
        };
    }

    pub fn deinit(self: *SupportedFrontier) void {
        {
            var iter = self.support.iterator();
            while (iter.next()) |entry| entry.key_ptr.deinit(self.allocator);
        }
        self.support.deinit();
        self.frontier.deinit();
        self.* = undefined;
    }

    /// Change the count of `timestamp` by `diff`.
    /// Reports any changes to the frontier into `changes_into`.
    /// Changes are owned by the caller.
    pub fn update(self: *SupportedFrontier, timestamp: Timestamp, diff: isize, changes_into: *u.ArrayList(FrontierChange)) !void {
        const support_entry = try self.support.getOrPut(timestamp);
        if (!support_entry.found_existing) {
            support_entry.key_ptr.* = try u.deepClone(support_entry.key_ptr.*, self.allocator);
            support_entry.value_ptr.* = 0;
        }
        support_entry.value_ptr.* = @intCast(usize, @intCast(isize, support_entry.value_ptr.*) + diff);

        if (support_entry.value_ptr.* == 0) {
            // Timestamp was just removed, might have been in frontier
            if (self.support.fetchRemove(timestamp)) |*remove_entry| {
                remove_entry.key.deinit(self.allocator);
            }
            if (self.frontier.timestamps.fetchRemove(timestamp)) |*remove_entry| {
                remove_entry.key.deinit(self.allocator);

                // Removed this timestamp from frontier
                try changes_into.append(.{ .timestamp = try u.deepClone(timestamp, self.allocator), .diff = -1 });

                // Find timestamps in support that might now be on the frontier
                var candidates = u.ArrayList(Timestamp).init(self.allocator);
                defer candidates.deinit();
                var iter = self.support.iterator();
                while (iter.next()) |entry| {
                    if (timestamp.causalOrder(entry.key_ptr.*) == .lt)
                        try candidates.append(entry.key_ptr.*);
                }

                // Add in lexical order any candidates that are not past the current frontier (or past any earlier candidates)
                std.sort.sort(Timestamp, candidates.items, {}, struct {
                    fn lessThan(_: void, a: Timestamp, b: Timestamp) bool {
                        return a.lexicalOrder(b) == .lt;
                    }
                }.lessThan);
                for (candidates.items) |candidate| {
                    if (self.frontier.causalOrder(candidate) == .none) {
                        try self.frontier.timestamps.put(try u.deepClone(candidate, self.allocator), {});
                        try changes_into.append(.{ .timestamp = try u.deepClone(candidate, self.allocator), .diff = 1 });
                    }
                }
            }
        }

        if (support_entry.value_ptr.* == diff) {
            // Timestamp was just added, might be in frontier
            if (self.frontier.causalOrder(timestamp) != .lt) {
                // Add to frontier
                try self.frontier.timestamps.put(try u.deepClone(timestamp, self.allocator), {});
                try changes_into.append(.{ .timestamp = try u.deepClone(timestamp, self.allocator), .diff = 1 });

                // Remove any other timestamp that is greater than the new timestamp
                var to_remove = u.ArrayList(Timestamp).init(self.allocator);
                defer to_remove.deinit();
                var iter = self.frontier.timestamps.iterator();
                while (iter.next()) |frontier_entry| {
                    if (frontier_entry.key_ptr.causalOrder(timestamp) == .gt)
                        try to_remove.append(frontier_entry.key_ptr.*);
                }
                for (to_remove.items) |other_timestamp| {
                    _ = self.frontier.timestamps.remove(other_timestamp);
                    try changes_into.append(.{ .timestamp = other_timestamp, .diff = -1 });
                }
            }
        }
    }
};

/// Represents a single change to the set of earliest timestamps in a frontier.
pub const FrontierChange = struct {
    timestamp: Timestamp,
    diff: isize,

    pub fn deinit(self: *FrontierChange, allocator: u.Allocator) void {
        self.timestamp.deinit(allocator);
        self.* = undefined;
    }
};

/// Represents a change to some bag in the dataflow.
/// The count of `row` changed by `diff` at `timestamp`.
pub const Change = struct {
    row: Row,
    timestamp: Timestamp,
    diff: isize,

    pub fn deinit(self: *Change, allocator: u.Allocator) void {
        self.row.deinit(allocator);
        self.timestamp.deinit(allocator);
        self.* = undefined;
    }
};

pub const ConcatOrder = enum { LeftThenRight, RightThenLeft };

/// A batch of changes, conveniently pre-sorted and de-duplicated.
pub const ChangeBatch = struct {
    /// Invariant: for every change in changes, lower_bound.causalOrder(change).isLessThanOrEqual()
    lower_bound: Frontier,
    /// Invariant: non-empty,
    /// Invariant: sorted by row/timestamp
    /// Invariant: no two changes with same row/timestamp
    // TODO should be `[]const Change`?
    changes: []Change,

    pub fn empty(allocator: u.Allocator) ChangeBatch {
        var empty_changes = [0]Change{};
        return ChangeBatch{
            .lower_bound = Frontier.init(allocator),
            .changes = &empty_changes,
        };
    }

    pub fn deinit(self: *ChangeBatch, allocator: u.Allocator) void {
        for (self.changes) |*change| change.deinit(allocator);
        allocator.free(self.changes);
        self.lower_bound.deinit();
        self.* = undefined;
    }

    /// Find the first row after `from` that starts with `row[0..key_columns]` or, if there is no such row, the position where it would be.
    /// IE returns `ix` such that:
    /// * `self.changes[ix].row[0..key_columns] >= row[0..key_columns]` (or `ix == self.changes.len`)
    /// * `self.changes[ix-1].row[0..key_columns] < row[0..key_columns]` (or `ix == 0`)
    /// Uses a binary search with increasing step size.
    /// If `from == self.changes.len`, then returns `from`.
    pub fn seekRowStart(self: ChangeBatch, from: usize, row: Row, key_columns: usize) usize {
        u.assert(
            from <= self.changes.len,
            "Can't seek to row from a start point that is beyond the end of the batch",
            .{},
        );
        if (from == self.changes.len or
            u.deepOrder(
            self.changes[from].row.values[0..key_columns],
            row.values[0..key_columns],
        ) != .lt)
            return from;
        var lo = from;
        var skip: usize = 1;
        while (true) {
            const next = lo + skip;
            if (next >= self.changes.len) {
                skip = self.changes.len - lo;
                break;
            }
            if (u.deepOrder(
                self.changes[next].row.values[0..key_columns],
                row.values[0..key_columns],
            ) != .lt)
                break;
            lo = next;
            skip *= 2;
        }
        var hi = lo + skip;
        // now lo is < row and hi is >= row
        u.assert(
            u.deepOrder(
                self.changes[lo].row.values[0..key_columns],
                row.values[0..key_columns],
            ) == .lt,
            "",
            .{},
        );
        u.assert(
            hi >= self.changes.len or
                u.deepOrder(
                self.changes[hi].row.values[0..key_columns],
                row.values[0..key_columns],
            ) != .lt,
            "",
            .{},
        );
        while (hi - lo > 1) {
            const mid = lo + @divTrunc(hi - lo, 2);
            if (u.deepOrder(
                self.changes[mid].row.values[0..key_columns],
                row.values[0..key_columns],
            ) == .lt) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        return hi;
    }

    /// Find the last row after `from` that starts with `row[0..key_columns]` or, if there is no such row, the position where it would be.
    /// IE returns `ix` such that:
    /// * `self.changes[ix].row[0..key_columns] > row[0..key_columns]` (or `ix == self.changes.len`)
    /// * `self.changes[ix-1].row[0..key_columns] <= row[0..key_columns]` (or `ix == 0`)
    /// Uses a linear scan.
    /// If `from == self.changes.len`, then returns `from`.
    pub fn seekRowEnd(self: ChangeBatch, from: usize, row: Row, key_columns: usize) usize {
        u.assert(
            from <= self.changes.len,
            "Can't seek to row from a start point that is beyond the end of the batch",
            .{},
        );
        if (from == self.changes.len)
            return from;
        var ix = from;
        while (ix < self.changes.len and
            u.deepOrder(
            self.changes[ix].row.values[0..key_columns],
            row.values[0..key_columns],
        ) != .gt) ix += 1;
        return ix;
    }

    /// Find the last row after `from` that starts with `self.changes[from].row[0..key_columns]`
    /// If `from == self.changes.len`, then returns `from`.
    pub fn seekCurrentRowEnd(self: ChangeBatch, from: usize, key_columns: usize) usize {
        u.assert(
            from <= self.changes.len,
            "Can't seek to row from a start point that is beyond the end of the batch",
            .{},
        );
        if (from == self.changes.len)
            return from;
        return self.seekRowEnd(from + 1, self.changes[from].row, key_columns);
    }

    /// Relational join on the first `key_columns` columns of self and other.
    /// Produces rows that look like `self_row ++ other_row[key_columns..]`.
    pub fn mergeJoin(
        self: ChangeBatch,
        self_frontier: Frontier,
        other: ChangeBatch,
        key_columns: usize,
        concat_order: ConcatOrder,
        into_builder: *ChangeBatchBuilder,
    ) !void {
        var ix_self: usize = 0;
        var ix_other: usize = 0;
        while (ix_self < self.changes.len and ix_other < other.changes.len) {
            switch (u.deepOrder(
                self.changes[ix_self].row.values[0..key_columns],
                other.changes[ix_other].row.values[0..key_columns],
            )) {
                .eq => {
                    const ix_self_end = self.seekCurrentRowEnd(ix_self, key_columns);
                    const ix_other_end = other.seekCurrentRowEnd(ix_other, key_columns);
                    const ix_other_start = ix_other;
                    while (ix_self < ix_self_end) : (ix_self += 1) {
                        if (self_frontier.causalOrder(self.changes[ix_self].timestamp) == .gt) {
                            ix_other = ix_other_start;
                            while (ix_other < ix_other_end) : (ix_other += 1) {
                                const change_self = self.changes[ix_self];
                                const change_other = other.changes[ix_other];
                                var values = try std.mem.concat(into_builder.allocator, Value, switch (concat_order) {
                                    .LeftThenRight => &[_][]const Value{
                                        change_self.row.values,
                                        change_other.row.values[key_columns..],
                                    },
                                    .RightThenLeft => &[_][]const Value{
                                        change_other.row.values,
                                        change_self.row.values[key_columns..],
                                    },
                                });
                                for (values) |*value| {
                                    value.* = try u.deepClone(value.*, into_builder.allocator);
                                }
                                try into_builder.changes.append(.{
                                    .row = .{ .values = values },
                                    .timestamp = try Timestamp.leastUpperBound(into_builder.allocator, change_self.timestamp, change_other.timestamp),
                                    .diff = change_self.diff * change_other.diff,
                                });
                            }
                        }
                    }
                    // now ix_self and ix_other are both at next row
                },
                .lt => {
                    ix_self = self.seekRowStart(ix_self, other.changes[ix_other].row, key_columns);
                },
                .gt => {
                    ix_other = other.seekRowStart(ix_other, self.changes[ix_self].row, key_columns);
                },
            }
        }
    }
};

/// A helper for building a ChangeBatch.
/// Append to `changes` as you like and call `finishAndReset` to produce a batch.
pub const ChangeBatchBuilder = struct {
    allocator: u.Allocator,
    changes: u.ArrayList(Change),

    pub fn init(allocator: u.Allocator) ChangeBatchBuilder {
        return ChangeBatchBuilder{
            .allocator = allocator,
            .changes = u.ArrayList(Change).init(allocator),
        };
    }

    pub fn deinit(self: *ChangeBatchBuilder) void {
        for (self.changes.items) |*change| change.deinit(self.allocator);
        self.changes.deinit();
        self.* = undefined;
    }

    /// Coalesce changes with identical rows and timestamps.
    pub fn coalesce(self: *ChangeBatchBuilder) void {
        if (self.changes.items.len == 0) return;

        std.sort.sort(Change, self.changes.items, {}, struct {
            fn lessThan(_: void, a: Change, b: Change) bool {
                return u.deepOrder(a, b) == .lt;
            }
        }.lessThan);

        var prev_i: usize = 0;
        for (self.changes.items[1..]) |*change| {
            const prev_change = &self.changes.items[prev_i];
            if (u.deepEqual(prev_change.row, change.row) and u.deepEqual(prev_change.timestamp, change.timestamp)) {
                prev_change.diff += change.diff;
            } else {
                if (prev_change.diff != 0) prev_i += 1;
                std.mem.swap(Change, &self.changes.items[prev_i], change);
            }
        }
        if (self.changes.items[prev_i].diff != 0) prev_i += 1;
        for (self.changes.items[prev_i..]) |*change| change.deinit(self.allocator);
        self.changes.shrinkRetainingCapacity(prev_i);
    }

    /// Produce a change batch.
    /// If the batch would have been empty, return null instead.
    /// Resets `self` so that it can be used again.
    pub fn finishAndReset(self: *ChangeBatchBuilder) !?ChangeBatch {
        self.coalesce();
        if (self.changes.items.len == 0) return null;

        var lower_bound = Frontier.init(self.allocator);
        var changes_into = u.ArrayList(FrontierChange).init(self.allocator);
        defer changes_into.deinit();
        for (self.changes.items) |change| {
            try lower_bound.move(.Earlier, change.timestamp, &changes_into);
            for (changes_into.items) |*frontier_change| frontier_change.deinit(self.allocator);
            try changes_into.resize(0);
        }

        return ChangeBatch{
            .lower_bound = lower_bound,
            .changes = self.changes.toOwnedSlice(),
        };
    }
};

/// Represents the state of a bag at a variety of timestamps.
/// Allows efficiently adding new changes and querying previous changes.
pub const Index = struct {
    allocator: u.Allocator,
    /// Invariant: each batch is at most half the size of it's left neighbour
    change_batches: u.ArrayList(ChangeBatch),

    pub fn init(allocator: u.Allocator) Index {
        return .{
            .allocator = allocator,
            .change_batches = u.ArrayList(ChangeBatch).init(allocator),
        };
    }

    pub fn deinit(self: *Index) void {
        for (self.change_batches.items) |*change_batch| change_batch.deinit(self.allocator);
        self.change_batches.deinit();
        self.* = undefined;
    }

    /// Takes ownership of `change_batch`
    // TODO merge incrementally to avoid latency spikes
    pub fn addChangeBatch(self: *Index, change_batch: ChangeBatch) !void {
        try self.change_batches.append(change_batch);
        while (true) {
            const len = self.change_batches.items.len;
            if (len <= 1 or @divFloor(self.change_batches.items[len - 2].changes.len, 2) >= self.change_batches.items[len - 1].changes.len) break;
            var batch_a = self.change_batches.pop();
            defer {
                batch_a.lower_bound.deinit();
                self.allocator.free(batch_a.changes);
            }
            var batch_b = self.change_batches.pop();
            defer {
                batch_b.lower_bound.deinit();
                self.allocator.free(batch_b.changes);
            }
            var builder = ChangeBatchBuilder.init(self.allocator);
            defer builder.deinit();
            try builder.changes.ensureTotalCapacity(batch_a.changes.len + batch_b.changes.len);
            try builder.changes.appendSlice(batch_a.changes);
            try builder.changes.appendSlice(batch_b.changes);
            if (try builder.finishAndReset()) |batch_ab| {
                try self.change_batches.append(batch_ab);
            }
        }
    }

    /// Relational join on the first `key_columns` columns of self and change_batch.
    /// Produces rows that look like:
    /// * `self_row ++ other_row[key_columns..]` if `concat_order == .LeftThenRight`
    /// * `other_row ++ self_row[key_columns..]` if `concat_order == .RightThenLeft`
    // TODO would it be better to merge against a cursor, to avoid touching change_batch multiple times?
    pub fn mergeJoin(
        self: *const Index,
        self_frontier: Frontier,
        change_batch: ChangeBatch,
        key_columns: usize,
        concat_order: ConcatOrder,
        into_builder: *ChangeBatchBuilder,
    ) !void {
        for (self.change_batches.items) |self_change_batch| {
            try self_change_batch.mergeJoin(self_frontier, change_batch, key_columns, concat_order, into_builder);
        }
    }

    /// Appends every change where `row.values[0..key_columns] == change.row.values[0..key_columns]` into `into_changes`.
    /// Changes are borrowed from the index.
    pub fn getChangesForKey(self: *Index, row: Row, key_columns: usize, into_changes: *u.ArrayList(Change)) !void {
        for (self.change_batches.items) |change_batch| {
            var start_ix = change_batch.seekRowStart(0, row, key_columns);
            const end_ix = change_batch.seekRowEnd(start_ix, row, key_columns);
            while (start_ix < end_ix) : (start_ix += 1)
                try into_changes.append(change_batch.changes[start_ix]);
        }
    }

    pub fn getCountForRowAsOf(self: *Index, row: Row, timestamp: Timestamp) isize {
        var count: isize = 0;
        for (self.change_batches.items) |change_batch| {
            var start_ix = change_batch.seekRowStart(0, row, row.values.len);
            const end_ix = change_batch.seekRowEnd(start_ix, row, row.values.len);
            while (start_ix < end_ix) : (start_ix += 1) {
                const change = change_batch.changes[start_ix];
                if (change.timestamp.causalOrder(timestamp).isLessThanOrEqual())
                    count += change.diff;
            }
        }
        return count;
    }
};

/// A node in the dataflow graph.
pub const Node = struct {
    id: usize,
};

/// One of the input edges to some node in a dataflow graph.
pub const NodeInput = struct {
    node: Node,
    input_ix: usize,
};

pub const NodeSpecTag = enum {
    Input,
    Map,
    Index,
    Join,
    Output,
    TimestampPush,
    TimestampIncrement,
    TimestampPop,
    Union,
    Distinct,
    Reduce,

    pub fn hasIndex(self: NodeSpecTag) bool {
        return switch (self) {
            .Index, .Distinct, .Reduce => true,
            else => false,
        };
    }

    pub fn needsIndex(self: NodeSpecTag) bool {
        return switch (self) {
            .Distinct, .Reduce => true,
            else => false,
        };
    }
};

/// Specifies how a node should transform inputs bags into an output bag.
pub const NodeSpec = union(NodeSpecTag) {
    Input,
    Map: MapSpec,
    Index: IndexSpec,
    Join: JoinSpec,
    Output: OutputSpec,
    TimestampPush: TimestampPushSpec,
    TimestampIncrement: TimestampIncrementSpec,
    TimestampPop: TimestampPopSpec,
    Union: UnionSpec,
    Distinct: DistinctSpec,
    Reduce: ReduceSpec,

    pub const MapSpec = struct {
        input: Node,
        mapper: *Mapper,

        pub const Mapper = struct {
            map_fn: fn (self: *Mapper, row: Row) error{OutOfMemory}!Row,
        };
    };

    pub const IndexSpec = struct {
        input: Node,
    };

    pub const JoinSpec = struct {
        inputs: [2]Node,
        key_columns: usize,
    };

    pub const OutputSpec = struct {
        input: Node,
    };

    pub const TimestampPushSpec = struct {
        input: Node,
    };

    pub const TimestampIncrementSpec = struct {
        // Initially null, will be set later to a future edge
        input: ?Node,
    };

    pub const TimestampPopSpec = struct {
        input: Node,
    };

    pub const UnionSpec = struct {
        inputs: [2]Node,
    };

    pub const DistinctSpec = struct {
        input: Node,
    };

    pub const ReduceSpec = struct {
        input: Node,
        key_columns: usize,
        init_value: Value,
        reducer: *Reducer,

        pub const Reducer = struct {
            reduce_fn: fn (self: *Reducer, reduced_value: Value, row: Row, count: usize) error{OutOfMemory}!Value,
        };
    };

    pub fn getInputs(self: *const NodeSpec) []const Node {
        return switch (self.*) {
            .Input => |_| &[_]Node{},
            .Map => |*spec| u.ptrToSlice(Node, &spec.input),
            .Index => |*spec| u.ptrToSlice(Node, &spec.input),
            .Output => |*spec| u.ptrToSlice(Node, &spec.input),
            .TimestampPush => |*spec| u.ptrToSlice(Node, &spec.input),
            .TimestampIncrement => |*spec| u.ptrToSlice(Node, &spec.input.?),
            .TimestampPop => |*spec| u.ptrToSlice(Node, &spec.input),
            .Distinct => |*spec| u.ptrToSlice(Node, &spec.input),
            .Reduce => |*spec| u.ptrToSlice(Node, &spec.input),
            .Join => |*spec| &spec.inputs,
            .Union => |*spec| &spec.inputs,
        };
    }
};

/// The internal state of a node in a running dataflow.
pub const NodeState = union(enum) {
    Input: InputState,
    Map,
    Index: IndexState,
    Join: JoinState,
    Output: OutputState,
    TimestampPush,
    TimestampIncrement,
    TimestampPop,
    Union,
    Distinct: DistinctState,
    Reduce: ReduceState,

    pub const InputState = struct {
        frontier: Frontier,
        /// These changes are being buffered.
        /// When flushed they will form a change batch.
        unflushed_changes: ChangeBatchBuilder,
    };

    pub const IndexState = struct {
        index: Index,
        /// These changes are waiting for the frontier to move past them, at which point they will be added to the index.
        pending_changes: u.ArrayList(Change),
    };

    pub const JoinState = struct {
        // The input frontier of the input indexes, as of the last ChangeBatch processed from them.
        // We use these to ensure we ignore Changes that exist in the index already but haven't yet been processed by this join.
        index_input_frontiers: [2]Frontier,
    };

    pub const OutputState = struct {
        unpopped_change_batches: u.Queue(ChangeBatch),
    };

    pub const DistinctState = struct {
        index: Index,
        /// These are rows/timestamps at which the output might change even if there is no new input.
        /// For example, if a distinct row appears at two different timestamps, then at the leastUpperBound of those timestamps the total count would be 2 and we need to correct that.
        /// To calculate:
        /// * For each row in the input, take the leastUpperBound of every possible subset of timestamps at which that row changed.
        /// * Filter out timestamps that are before the output frontier of this node.
        // TODO If Index supported cheap single updates, it would maybe be a suitable data structure here.
        pending_corrections: u.DeepHashMap(Row, u.DeepHashSet(Timestamp)),
    };

    pub const ReduceState = struct {
        index: Index,
        /// These are keys/timestamps at which the output might change even if there is no new input.
        /// For example, if a key appears at two different timestamps, then at the leastUpperBound of those timestamps the there will be two output values and we need to replace that with the correct single output.
        /// To calculate:
        /// * For each key in the input, take the leastUpperBound of every possible subset of timestamps at which that key changed.
        /// * Filter out timestamps that are before the output frontier of this node.
        // TODO If Index supported cheap single updates, it would maybe be a suitable data structure here.
        pending_corrections: u.DeepHashMap(Row, u.DeepHashSet(Timestamp)),
    };

    pub fn init(allocator: u.Allocator, node_spec: NodeSpec) NodeState {
        return switch (node_spec) {
            .Input => .{
                .Input = .{
                    .frontier = Frontier.init(allocator),
                    .unflushed_changes = ChangeBatchBuilder.init(allocator),
                },
            },
            .Map => .Map,
            .Index => .{
                .Index = .{
                    .index = Index.init(allocator),
                    .pending_changes = u.ArrayList(Change).init(allocator),
                },
            },
            .Join => .{
                .Join = .{
                    .index_input_frontiers = .{
                        Frontier.init(allocator),
                        Frontier.init(allocator),
                    },
                },
            },
            .Output => .{
                .Output = .{
                    .unpopped_change_batches = u.Queue(ChangeBatch).init(allocator),
                },
            },
            .TimestampPush => .TimestampPush,
            .TimestampIncrement => .TimestampIncrement,
            .TimestampPop => .TimestampPop,
            .Union => .Union,
            .Distinct => .{
                .Distinct = .{
                    .index = Index.init(allocator),
                    .pending_corrections = u.DeepHashMap(Row, u.DeepHashSet(Timestamp)).init(allocator),
                },
            },
            .Reduce => .{
                .Reduce = .{
                    .index = Index.init(allocator),
                    .pending_corrections = u.DeepHashMap(Row, u.DeepHashSet(Timestamp)).init(allocator),
                },
            },
        };
    }

    pub fn deinit(self: *NodeState, allocator: u.Allocator) void {
        switch (self.*) {
            .Input => |*input| {
                input.frontier.deinit();
                input.unflushed_changes.deinit();
            },
            .Index => |*index| {
                index.index.deinit();
                for (index.pending_changes.items) |*change| change.deinit(allocator);
                index.pending_changes.deinit();
            },
            .Join => |*join| {
                for (join.index_input_frontiers) |*frontier| frontier.deinit();
            },
            .Output => |*output| {
                for (output.unpopped_change_batches.in.items) |*change_batch| change_batch.deinit(allocator);
                for (output.unpopped_change_batches.out.items) |*change_batch| change_batch.deinit(allocator);
                output.unpopped_change_batches.deinit();
            },
            .Distinct => |*distinct| {
                distinct.index.deinit();
                {
                    var iter = distinct.pending_corrections.iterator();
                    while (iter.next()) |entry| {
                        entry.key_ptr.deinit(allocator);
                        {
                            var value_iter = entry.value_ptr.iterator();
                            while (value_iter.next()) |value_entry| {
                                value_entry.key_ptr.deinit(allocator);
                            }
                        }
                        entry.value_ptr.deinit();
                    }
                }
                distinct.pending_corrections.deinit();
            },
            .Reduce => |*reduce| {
                reduce.index.deinit();
                {
                    var iter = reduce.pending_corrections.iterator();
                    while (iter.next()) |entry| {
                        entry.key_ptr.deinit(allocator);
                        {
                            var value_iter = entry.value_ptr.iterator();
                            while (value_iter.next()) |value_entry| {
                                value_entry.key_ptr.deinit(allocator);
                            }
                        }
                        entry.value_ptr.deinit();
                    }
                }
                reduce.pending_corrections.deinit();
            },
            .Map, .TimestampPush, .TimestampIncrement, .TimestampPop, .Union => {},
        }
        self.* = undefined;
    }

    pub fn getIndex(self: *NodeState) ?*Index {
        return switch (self.*) {
            .Index => |*state| &state.index,
            .Distinct => |*state| &state.index,
            .Reduce => |*state| &state.index,
            // TODO should be able to follow TimestampPush/Pop to an index and wrap it
            else => null,
        };
    }
};

/// A subgraph of the dataflow graph.
/// Every node in a subgraph has the same number of timestamp coordinates.
/// Every loop in the graph must be contained entirely by a single subgraph.
/// Subgraphs must be nested hierarchically - no overlaps.
pub const Subgraph = struct {
    id: usize,
};

/// A description of a dataflow graph.
pub const Graph = struct {
    allocator: u.Allocator,
    /// For each node, the spec that determines its behavior
    node_specs: []const NodeSpec,
    /// For each node, the subgraphs that it belongs to (outermost first, innermost last).
    node_subgraphs: []const []const Subgraph,
    /// For each subgraph, the parent subgraph that it is nested within
    /// (Indexed by subgraph.id-1, because subgraph 0 has no parent)
    subgraph_parents: []const Subgraph,
    /// For each node, the nodes that are immediately downstream (ie have this node as an input).
    downstream_node_inputs: []const []const NodeInput,

    /// Takes ownership of `node_specs` and `subgraph_parents`. 
    pub fn init(allocator: u.Allocator, node_specs: []const NodeSpec, node_immediate_subgraphs: []const Subgraph, subgraph_parents: []const Subgraph) !Graph {
        const num_nodes = node_specs.len;
        u.assert(
            node_immediate_subgraphs.len == num_nodes,
            "node_specs and node_immediate_subgraphs should have same length, got {} vs {}",
            .{ node_immediate_subgraphs.len, num_nodes },
        );

        // For each node, store its subgraph, its subgraphs parent, its subgraphs parents parent etc
        var node_subgraphs = try allocator.alloc([]Subgraph, num_nodes);
        for (node_immediate_subgraphs) |immediate_subgraph, node_id| {
            var subgraphs = u.ArrayList(Subgraph).init(allocator);
            defer subgraphs.deinit();
            var subgraph = immediate_subgraph;
            while (true) {
                try subgraphs.append(subgraph);
                if (subgraph.id == 0) break;
                subgraph = subgraph_parents[subgraph.id - 1];
            }
            std.mem.reverse(Subgraph, subgraphs.items);
            node_subgraphs[node_id] = subgraphs.toOwnedSlice();
        }

        // Collect downstream nodes
        var downstream_node_inputs = try allocator.alloc(u.ArrayList(NodeInput), num_nodes);
        defer allocator.free(downstream_node_inputs);
        for (node_specs) |_, node_id| {
            downstream_node_inputs[node_id] = u.ArrayList(NodeInput).init(allocator);
        }
        for (node_specs) |node_spec, node_id| {
            for (node_spec.getInputs()) |input_node, input_ix| {
                try downstream_node_inputs[input_node.id].append(.{ .node = .{ .id = node_id }, .input_ix = input_ix });
            }
        }
        var frozen_downstream_node_inputs = try allocator.alloc([]NodeInput, node_specs.len);
        for (downstream_node_inputs) |*node_inputs, node_id|
            frozen_downstream_node_inputs[node_id] = node_inputs.toOwnedSlice();

        var self = Graph{
            .allocator = allocator,
            .node_specs = node_specs,
            .node_subgraphs = node_subgraphs,
            .subgraph_parents = subgraph_parents,
            .downstream_node_inputs = frozen_downstream_node_inputs,
        };

        return self;
    }

    pub fn deinit(self: *Graph) void {
        for (self.downstream_node_inputs) |downstream_node_inputs| self.allocator.free(downstream_node_inputs);
        self.allocator.free(self.downstream_node_inputs);
        self.allocator.free(self.subgraph_parents);
        for (self.node_subgraphs) |node_subgraphs| self.allocator.free(node_subgraphs);
        self.allocator.free(self.node_subgraphs);
        self.allocator.free(self.node_specs);
        self.* = undefined;
    }

    /// Assert that the graph obeys all the constraints required to make the progress tracking algorithm work.
    pub fn validate(self: Graph) !void {
        const num_nodes = self.node_specs.len;

        for (self.subgraph_parents) |parent, subgraph_id_minus_one| {
            u.assert(
                parent.id < subgraph_id_minus_one + 1,
                "The parent of a subgraph must have a smaller id than its child",
                .{},
            );
        }

        for (self.node_specs) |node_spec, node_id| {
            for (node_spec.getInputs()) |input_node| {
                u.assert(input_node.id < num_nodes, "All input nodes must exist", .{});
                if (node_spec == .TimestampIncrement) {
                    u.assert(
                        input_node.id > node_id,
                        "TimestampIncrement nodes must have a later node as input",
                        .{},
                    );
                } else {
                    u.assert(
                        input_node.id < node_id,
                        "All nodes (other than TimestampIncrement) must have an earlier node as input",
                        .{},
                    );
                }
                if (std.meta.activeTag(node_spec).needsIndex())
                    u.assert(
                        std.meta.activeTag(self.node_specs[input_node.id]).hasIndex(),
                        "Inputs to {} node must contain an index",
                        .{std.meta.activeTag(node_spec)},
                    );
                switch (node_spec) {
                    .TimestampPush => {
                        const input_subgraph = u.last(Subgraph, self.node_subgraphs[input_node.id]);
                        const output_subgraph = u.last(Subgraph, self.node_subgraphs[node_id]);
                        u.assert(
                            output_subgraph.id > 0,
                            "TimestampPush nodes cannot have an output on subgraph 0",
                            .{},
                        );
                        u.assert(
                            self.subgraph_parents[output_subgraph.id - 1].id == input_subgraph.id,
                            "TimestampPush nodes must cross from a parent subgraph to a child subgraph",
                            .{},
                        );
                    },
                    .TimestampPop => {
                        const input_subgraph = u.last(Subgraph, self.node_subgraphs[input_node.id]);
                        const output_subgraph = u.last(Subgraph, self.node_subgraphs[node_id]);
                        u.assert(
                            input_subgraph.id > 0,
                            "TimestampPop nodes cannot have an input on subgraph 0",
                            .{},
                        );
                        u.assert(
                            self.subgraph_parents[input_subgraph.id - 1].id == output_subgraph.id,
                            "TimestampPop nodes must cross from a child subgraph to a parent subgraph",
                            .{},
                        );
                    },
                    else => {
                        const input_subgraph = u.last(Subgraph, self.node_subgraphs[input_node.id]);
                        const output_subgraph = u.last(Subgraph, self.node_subgraphs[node_id]);
                        u.assert(
                            input_subgraph.id == output_subgraph.id,
                            "Nodes (other than TimestampPop and TimestampPush) must be on the same subgraph as their inputs",
                            .{},
                        );
                    },
                }
            }
        }

        var earliest_subgraph_pops = u.DeepHashMap(Subgraph, Node).init(self.allocator);
        defer earliest_subgraph_pops.deinit();
        var latest_subgraph_pushes = u.DeepHashMap(Subgraph, Node).init(self.allocator);
        defer latest_subgraph_pushes.deinit();
        for (self.node_specs) |node_spec, node_id| {
            switch (node_spec) {
                .TimestampPush => {
                    const subgraph = u.last(Subgraph, self.node_subgraphs[node_id]);
                    const entry = try latest_subgraph_pushes.getOrPutValue(subgraph, .{ .id = node_id });
                    entry.value_ptr.id = u.max(entry.value_ptr.id, node_id);
                },
                .TimestampPop => |spec| {
                    const subgraph = u.last(Subgraph, self.node_subgraphs[spec.input.id]);
                    const entry = try earliest_subgraph_pops.getOrPutValue(subgraph, .{ .id = node_id });
                    entry.value_ptr.id = u.min(entry.value_ptr.id, node_id);
                },
                else => {},
            }
        }
        var subgraph_id: usize = 1;
        while (subgraph_id - 1 < self.subgraph_parents.len) : (subgraph_id += 1) {
            if (earliest_subgraph_pops.get(.{ .id = subgraph_id })) |earliest| {
                if (latest_subgraph_pushes.get(.{ .id = subgraph_id })) |latest| {
                    // TODO This constraint works, but is clunky. Could instead test directly for the case where a path exits and re-enters the subgraph without going backwards.
                    u.assert(
                        earliest.id >= latest.id,
                        "Every TimestampPush into a subgraph must have an earlier node id than every TimestampPop from that subgraph. Found TimestampPush at {} later than TimestampPop at {}",
                        .{ latest, earliest },
                    );
                }
            }
        }
    }
};

/// A helper for building a graph.
/// Call `addSubgraph` and `addNode` to build it up.
/// Call `connectLoop` to connect backwards edges in loops.
/// Call `finishAndReset` to produce the graph.
pub const GraphBuilder = struct {
    allocator: u.Allocator,
    node_specs: u.ArrayList(NodeSpec),
    node_subgraphs: u.ArrayList(Subgraph),
    subgraph_parents: u.ArrayList(Subgraph),

    pub fn init(allocator: u.Allocator) GraphBuilder {
        return GraphBuilder{
            .allocator = allocator,
            .node_specs = u.ArrayList(NodeSpec).init(allocator),
            .node_subgraphs = u.ArrayList(Subgraph).init(allocator),
            .subgraph_parents = u.ArrayList(Subgraph).init(allocator),
        };
    }

    pub fn deinit(self: *GraphBuilder) void {
        self.subgraph_parents.deinit();
        self.node_subgraphs.deinit();
        self.node_specs.deinit();
        self.* = undefined;
    }

    pub fn addSubgraph(self: *GraphBuilder, parent: Subgraph) !Subgraph {
        try self.subgraph_parents.append(parent);
        return Subgraph{ .id = self.subgraph_parents.items.len };
    }

    /// Add a new node to the graph.
    /// When adding a `TimestampIncrement` node, set its input to null initially and then later use `connectLoop` once the input node has been added.
    pub fn addNode(self: *GraphBuilder, subgraph: Subgraph, node_spec: NodeSpec) !Node {
        const node = Node{ .id = self.node_specs.items.len };
        try self.node_specs.append(node_spec);
        try self.node_subgraphs.append(subgraph);
        return node;
    }

    /// Sets the input of `earlier_node` to `later_node`.
    /// `earlier_node` must be a `TimestampIncrement` node - the only node that is allowed to have backwards edges.
    pub fn connectLoop(self: *GraphBuilder, later_node: Node, earlier_node: Node) void {
        self.node_specs.items[earlier_node.id].TimestampIncrement.input = later_node;
    }

    /// Produce the final graph.
    /// Resets `self` so it can be used again.
    pub fn finishAndReset(self: *GraphBuilder) !Graph {
        const node_subgraphs = self.node_subgraphs.toOwnedSlice();
        defer self.allocator.free(node_subgraphs);
        return Graph.init(
            self.allocator,
            self.node_specs.toOwnedSlice(),
            node_subgraphs,
            self.subgraph_parents.toOwnedSlice(),
        );
    }
};

/// Part of a running dataflow.
/// In a single-threaded dataflow there will be only one shard.
/// In a multi-threaded dataflow (TODO) there will be one shard per thread.
pub const Shard = struct {
    allocator: u.Allocator,
    /// Borrowed from caller of init.
    graph: *const Graph,
    /// For each node, the internal state of that node.
    node_states: []NodeState,
    /// For each node, the frontier for the nodes output.
    /// Invariant: any change emitted from a node has a timestamp that is not earlier than the frontier: node_frontiers[node.id].frontier.causalOrder(change.timestamp).isLessThanOrEqual()
    node_frontiers: []SupportedFrontier,
    /// An unordered list of change batches that have not yet been processed by some node.
    unprocessed_change_batches: u.ArrayList(ChangeBatchAtNodeInput),
    /// Frontier updates that have not yet been applied to some node's input frontier.
    /// (The input frontier is never materialized, so when these changes are processed they will be immediately transformed to apply to the ouput frontier).
    unprocessed_frontier_updates: u.DeepHashMap(Pointstamp, isize),

    pub const ChangeBatchAtNodeInput = struct {
        change_batch: ChangeBatch,
        input_frontier: ?Frontier,
        node_input: NodeInput,
    };

    pub const Pointstamp = struct {
        node_input: NodeInput,
        /// Borrowed from self.graph
        subgraphs: []const Subgraph,
        timestamp: Timestamp,

        pub fn deinit(self: *Pointstamp, allocator: u.Allocator) void {
            self.timestamp.deinit(allocator);
            self.* = undefined;
        }
    };

    pub fn init(allocator: u.Allocator, graph: *const Graph) !Shard {
        const num_nodes = graph.node_specs.len;

        var node_states = try allocator.alloc(NodeState, num_nodes);
        for (node_states) |*node_state, node_id|
            node_state.* = NodeState.init(allocator, graph.node_specs[node_id]);

        var node_frontiers = try allocator.alloc(SupportedFrontier, num_nodes);
        for (node_frontiers) |*node_frontier|
            node_frontier.* = try SupportedFrontier.init(allocator);

        var unprocessed_frontier_updates = u.DeepHashMap(Pointstamp, isize).init(allocator);

        var self = Shard{
            .allocator = allocator,
            .graph = graph,
            .node_states = node_states,
            .node_frontiers = node_frontiers,
            .unprocessed_change_batches = u.ArrayList(ChangeBatchAtNodeInput).init(allocator),
            .unprocessed_frontier_updates = unprocessed_frontier_updates,
        };

        // Init input frontiers
        for (graph.node_specs) |node_spec, node_id| {
            if (node_spec == .Input) {
                var timestamp = try Timestamp.initLeast(allocator, graph.node_subgraphs[node_id].len);
                _ = try self.applyFrontierSupportChange(.{ .id = node_id }, timestamp, 1);
                try self.node_states[node_id].Input.frontier.timestamps.put(timestamp, {});
            }
        }
        while (self.hasWork()) try self.doWork();

        return self;
    }

    pub fn deinit(self: *Shard) void {
        {
            var iter = self.unprocessed_frontier_updates.iterator();
            while (iter.next()) |entry| {
                entry.key_ptr.deinit(self.allocator);
            }
        }
        self.unprocessed_frontier_updates.deinit();
        for (self.unprocessed_change_batches.items) |*change_batch_at_node_input| {
            change_batch_at_node_input.change_batch.deinit(self.allocator);
        }
        self.unprocessed_change_batches.deinit();
        for (self.node_frontiers) |*node_frontier| node_frontier.deinit();
        self.allocator.free(self.node_frontiers);
        for (self.node_states) |*node_state| node_state.deinit(self.allocator);
        self.allocator.free(self.node_states);
        // self.graph is borrowed
        self.* = undefined;
    }

    /// Add a new change to an input node.
    /// These changes will not be processed by `hasWork`/`doWork` until `flushInput` is called.
    /// Takes ownership of `change`.
    pub fn pushInput(self: *Shard, node: Node, change: Change) !void {
        dida.debug.emitDebugEvent(self, .{ .PushInput = .{ .node = node, .change = change } });

        u.assert(
            self.node_states[node.id].Input.frontier.causalOrder(change.timestamp).isLessThanOrEqual(),
            "May not push inputs that are less than the Input node frontier set by Shard.advanceInput",
            .{},
        );
        try self.node_states[node.id].Input.unflushed_changes.changes.append(change);
    }

    /// Flush all of the changes at an input node into a change batch.
    pub fn flushInput(self: *Shard, node: Node) !void {
        dida.debug.emitDebugEvent(self, .{ .FlushInput = .{ .node = node } });

        var unflushed_changes = &self.node_states[node.id].Input.unflushed_changes;
        if (try unflushed_changes.finishAndReset()) |change_batch| {
            try self.emitChangeBatch(node, change_batch);
        }
    }

    /// Promise that you will never call `pushInput` on `node` with a change whose timestamp is earlier than `timestamp`.
    /// Doing this allows operations which need to see all the input at a given timestamp to progress.
    /// (This also implicitly flushes `node`.)
    // TODO Is advance the best verb? Would prefer to stay consistent with Earlier/Later used elsewhere.
    pub fn advanceInput(self: *Shard, node: Node, timestamp: Timestamp) !void {
        dida.debug.emitDebugEvent(self, .{ .AdvanceInput = .{ .node = node, .timestamp = timestamp } });

        // Have to flush input so that there aren't any pending changes with timestamps less than the new frontier
        try self.flushInput(node);

        var changes = u.ArrayList(FrontierChange).init(self.allocator);
        defer changes.deinit();
        try self.node_states[node.id].Input.frontier.move(.Later, timestamp, &changes);
        for (changes.items) |*change| {
            _ = try self.applyFrontierSupportChange(node, change.timestamp, change.diff);
            change.deinit(self.allocator);
        }
    }

    /// Report that `from_node` produced `change_batch` as an output.
    /// Takes ownership of `change_batch` and `output_frontier`.
    fn emitChangeBatch(self: *Shard, from_node: Node, change_batch: ChangeBatch) !void {
        var input_frontier: ?Frontier = null;
        {
            const node_spec = self.graph.node_specs[from_node.id];
            if (NodeSpecTag.hasIndex(node_spec)) {
                u.assert(
                    node_spec.getInputs().len == 1,
                    "At present all nodes with indexes have only one input. If this changed for {}, need to rethink this code.",
                    .{std.meta.activeTag(node_spec)},
                );
                input_frontier = self.node_frontiers[node_spec.getInputs()[0].id].frontier;
            }
        }
        dida.debug.emitDebugEvent(self, .{ .EmitChangeBatch = .{
            .from_node = from_node,
            .change_batch = change_batch,
            .input_frontier = input_frontier,
        } });

        // Check that this emission is legal
        {
            const output_frontier = self.node_frontiers[from_node.id];
            var iter = change_batch.lower_bound.timestamps.iterator();
            while (iter.next()) |entry| {
                u.assert(
                    output_frontier.frontier.causalOrder(entry.key_ptr.*).isLessThanOrEqual(),
                    "Emitted a change at a timestamp that is behind the output frontier. Node {}, timestamp {}.",
                    .{ from_node, entry.key_ptr.* },
                );
            }
        }

        var cloned_change_batch = change_batch;
        var cloned_input_frontier = input_frontier;
        for (self.graph.downstream_node_inputs[from_node.id]) |to_node_input, i| {
            if (i != 0)
                // We take ownership of change_batch so we don't have to clone it the first time we add it to the queue
                cloned_change_batch = try u.deepClone(cloned_change_batch, self.allocator);
            cloned_input_frontier = try u.deepClone(cloned_input_frontier, self.allocator);
            var iter = cloned_change_batch.lower_bound.timestamps.iterator();
            while (iter.next()) |entry| {
                try self.queueFrontierSupportChange(to_node_input, entry.key_ptr.*, 1);
            }
            try self.unprocessed_change_batches.append(.{
                .change_batch = cloned_change_batch,
                .input_frontier = cloned_input_frontier,
                .node_input = to_node_input,
            });
        }
    }

    /// Process one unprocessed change batch from the queue.
    fn processChangeBatch(self: *Shard) !void {
        const change_batch_at_node_input = self.unprocessed_change_batches.popOrNull() orelse return;
        var input_frontier = change_batch_at_node_input.input_frontier;
        defer if (input_frontier) |*_input_frontier| _input_frontier.deinit();
        var change_batch = change_batch_at_node_input.change_batch;
        defer change_batch.deinit(self.allocator);
        const node_input = change_batch_at_node_input.node_input;
        const node = node_input.node;
        const node_spec = self.graph.node_specs[node.id];
        const node_state = &self.node_states[node.id];

        dida.debug.emitDebugEvent(self, .{ .ProcessChangeBatch = .{ .node_input = node_input, .change_batch = change_batch } });

        // Remove change_batch from progress tracking
        {
            var iter = change_batch.lower_bound.timestamps.iterator();
            while (iter.next()) |entry| {
                try self.queueFrontierSupportChange(node_input, entry.key_ptr.*, -1);
            }
        }

        switch (node_spec) {
            .Input => u.panic("Input nodes should not have work pending on their input", .{}),
            .Map => |map| {
                var output_change_batch_builder = ChangeBatchBuilder.init(self.allocator);
                defer output_change_batch_builder.deinit();
                for (change_batch.changes) |change| {
                    const output_row = try map.mapper.map_fn(map.mapper, change.row);
                    try output_change_batch_builder.changes.append(.{
                        .row = output_row,
                        .timestamp = try u.deepClone(change.timestamp, self.allocator),
                        .diff = change.diff,
                    });
                }
                if (try output_change_batch_builder.finishAndReset()) |output_change_batch| {
                    try self.emitChangeBatch(node_input.node, output_change_batch);
                }
            },
            .Index => {
                // These won't be emitted until the frontier passes them
                // TODO this is a lot of timestamps - is there a cheaper way to maintain the support for the index frontier?
                for (change_batch.changes) |change| {
                    u.assert(
                        self.node_frontiers[node.id].frontier.causalOrder(change.timestamp).isLessThanOrEqual(),
                        "Index received a change that was behind its output frontier. Node {}, timestamp {}.",
                        .{ node, change.timestamp },
                    );
                    _ = try self.applyFrontierSupportChange(node, change.timestamp, 1);
                }
                try node_state.Index.pending_changes.appendSlice(change_batch.changes);
                // Took ownership of rows in changes, so don't deinit them
                self.allocator.free(change_batch.changes);
                change_batch.changes = &[0]Change{};
            },
            .Join => |join| {
                const index = self.node_states[join.inputs[1 - node_input.input_ix].id].getIndex().?;
                var output_change_batch_builder = ChangeBatchBuilder.init(self.allocator);
                defer output_change_batch_builder.deinit();
                try index.mergeJoin(
                    node_state.Join.index_input_frontiers[1 - node_input.input_ix],
                    change_batch,
                    join.key_columns,
                    switch (node_input.input_ix) {
                        0 => .RightThenLeft,
                        1 => .LeftThenRight,
                        else => u.panic("Bad input_ix for join: {}", .{node_input.input_ix}),
                    },
                    &output_change_batch_builder,
                );
                if (try output_change_batch_builder.finishAndReset()) |output_change_batch| {
                    try self.emitChangeBatch(node_input.node, output_change_batch);
                }
                node_state.Join.index_input_frontiers[node_input.input_ix].deinit();
                node_state.Join.index_input_frontiers[node_input.input_ix] = input_frontier.?;
                // Took ownership of input_frontier, so don't deinit it
                input_frontier = null;
            },
            .Output => {
                try node_state.Output.unpopped_change_batches.push(change_batch);
                // Took ownership of change_batch so don't deinit it
                change_batch = ChangeBatch.empty(self.allocator);
            },
            .TimestampPush => {
                var output_change_batch_builder = ChangeBatchBuilder.init(self.allocator);
                defer output_change_batch_builder.deinit();
                for (change_batch.changes) |change| {
                    const output_timestamp = try change.timestamp.pushCoord(self.allocator);
                    try output_change_batch_builder.changes.append(.{
                        .row = try u.deepClone(change.row, self.allocator),
                        .timestamp = output_timestamp,
                        .diff = change.diff,
                    });
                }
                try self.emitChangeBatch(node_input.node, (try output_change_batch_builder.finishAndReset()).?);
            },
            .TimestampIncrement => {
                var output_change_batch_builder = ChangeBatchBuilder.init(self.allocator);
                defer output_change_batch_builder.deinit();
                for (change_batch.changes) |change| {
                    const output_timestamp = try change.timestamp.incrementCoord(self.allocator);
                    try output_change_batch_builder.changes.append(.{
                        .row = try u.deepClone(change.row, self.allocator),
                        .timestamp = output_timestamp,
                        .diff = change.diff,
                    });
                }
                try self.emitChangeBatch(node_input.node, (try output_change_batch_builder.finishAndReset()).?);
            },
            .TimestampPop => {
                var output_change_batch_builder = ChangeBatchBuilder.init(self.allocator);
                defer output_change_batch_builder.deinit();
                for (change_batch.changes) |change| {
                    const output_timestamp = try change.timestamp.popCoord(self.allocator);
                    try output_change_batch_builder.changes.append(.{
                        .row = try u.deepClone(change.row, self.allocator),
                        .timestamp = output_timestamp,
                        .diff = change.diff,
                    });
                }
                if (try output_change_batch_builder.finishAndReset()) |output_change_batch| {
                    try self.emitChangeBatch(node_input.node, output_change_batch);
                }
            },
            .Union => {
                // Pass straight through
                try self.emitChangeBatch(node_input.node, change_batch);
                // Took ownership of change_batch so don't deinit it
                change_batch = ChangeBatch.empty(self.allocator);
            },
            .Distinct, .Reduce => {
                // Figure out which new rows/timestamps might need later corrections
                const pending_corrections = switch (node_state.*) {
                    .Distinct => |*state| &state.pending_corrections,
                    .Reduce => |*state| &state.pending_corrections,
                    else => unreachable,
                };
                for (change_batch.changes) |change| {
                    const key = switch (node_spec) {
                        .Distinct => change.row,
                        .Reduce => |spec| Row{ .values = change.row.values[0..spec.key_columns] },
                        else => unreachable,
                    };
                    const timestamps_entry = try pending_corrections.getOrPut(key);
                    if (!timestamps_entry.found_existing) {
                        timestamps_entry.key_ptr.* = try u.deepClone(timestamps_entry.key_ptr.*, self.allocator);
                        timestamps_entry.value_ptr.* = u.DeepHashSet(Timestamp).init(self.allocator);
                    }
                    const timestamps = timestamps_entry.value_ptr;

                    {
                        // change.timestamp is pending
                        const old_entry = try timestamps.getOrPut(change.timestamp);

                        // if was already pending, nothing more to do
                        if (old_entry.found_existing) continue;
                        old_entry.key_ptr.* = try u.deepClone(old_entry.key_ptr.*, self.allocator);

                        // otherwise, update frontier
                        _ = try self.applyFrontierSupportChange(node, change.timestamp, 1);
                    }

                    // for any other pending timestamp on this row, leastUpperBound(change.timestamp, other_timestamp) is pending
                    var buffer = u.ArrayList(Timestamp).init(self.allocator);
                    defer buffer.deinit();
                    var iter = timestamps.iterator();
                    while (iter.next()) |entry| {
                        const timestamp = try Timestamp.leastUpperBound(
                            self.allocator,
                            change.timestamp,
                            entry.key_ptr.*,
                        );
                        try buffer.append(timestamp);
                    }
                    for (buffer.items) |*timestamp| {
                        const old_entry = try timestamps.getOrPut(timestamp.*);
                        if (old_entry.found_existing) {
                            timestamp.deinit(self.allocator);
                        } else {
                            _ = try self.applyFrontierSupportChange(node, timestamp.*, 1);
                        }
                    }
                }
            },
        }
    }

    /// Report that the input frontier at `node_input` has changed, so the output frontier might need updating.
    fn queueFrontierSupportChange(self: *Shard, node_input: NodeInput, timestamp: Timestamp, diff: isize) !void {
        dida.debug.emitDebugEvent(self, .{ .QueueFrontierUpdate = .{ .node_input = node_input, .timestamp = timestamp, .diff = diff } });

        const node_spec = self.graph.node_specs[node_input.node.id];
        const input_node = node_spec.getInputs()[node_input.input_ix];
        var entry = try self.unprocessed_frontier_updates.getOrPut(.{
            .node_input = node_input,
            .subgraphs = self.graph.node_subgraphs[input_node.id],
            .timestamp = timestamp,
        });
        if (!entry.found_existing) {
            entry.key_ptr.timestamp = try u.deepClone(entry.key_ptr.timestamp, self.allocator);
            entry.value_ptr.* = 0;
        }
        entry.value_ptr.* += diff;
        if (entry.value_ptr.* == 0) {
            var removed = self.unprocessed_frontier_updates.fetchRemove(entry.key_ptr.*).?;
            removed.key.deinit(self.allocator);
        }
    }

    /// Change the output frontier at `node` and report the change to any downstream nodes.
    fn applyFrontierSupportChange(self: *Shard, node: Node, timestamp: Timestamp, diff: isize) !enum { Updated, NotUpdated } {
        dida.debug.emitDebugEvent(self, .{ .ApplyFrontierUpdate = .{ .node = node, .timestamp = timestamp, .diff = diff } });

        var frontier_changes = u.ArrayList(FrontierChange).init(self.allocator);
        defer frontier_changes.deinit();
        try self.node_frontiers[node.id].update(timestamp, diff, &frontier_changes);
        for (frontier_changes.items) |*frontier_change| {
            for (self.graph.downstream_node_inputs[node.id]) |downstream_node_input| {
                try self.queueFrontierSupportChange(downstream_node_input, frontier_change.timestamp, frontier_change.diff);
            }
            frontier_change.deinit(self.allocator);
        }
        return if (frontier_changes.items.len > 0) .Updated else .NotUpdated;
    }

    // An ordering on Pointstamp that is compatible with causality.
    // IE if the existence of a change at `this` causes a change to later be produced at `that`, then we need to have `orderPointstamps(this, that) == .lt`.
    // The invariants enforced for the graph structure guarantee that this is possible.
    fn orderPointstamps(this: Pointstamp, that: Pointstamp) std.math.Order {
        const min_len = u.min(this.subgraphs.len, that.subgraphs.len);
        var i: usize = 0;
        while (i < min_len) : (i += 1) {
            // If `this` and `that` are in different subgraphs then there is no way for a change to travel from a later node to an earlier node without incrementing the timestamp coord at `i-1`.
            if (this.subgraphs[i].id != that.subgraphs[i].id)
                return u.deepOrder(this.node_input, that.node_input);

            // If `this` and `that` are in the same subgraph but one has a higher timestamp coord at `i` than the other then there is no way the higher timestamp could be decremented to produce the lower timestamp.
            const timestamp_order = std.math.order(this.timestamp.coords[i], that.timestamp.coords[i]);
            if (timestamp_order != .eq) return timestamp_order;
        }
        // If we get this far, either `this` and `that` are in the same subgraph or one is in a subgraph that is nested inside the other.
        // Either way there is no way for a change to travel from a later node to an earlier node without incrementing the timestamp coord at `min_len-1`.
        return u.deepOrder(this.node_input, that.node_input);
    }

    /// Process all unprocessed frontier updates.
    fn processFrontierUpdates(self: *Shard) !void {
        dida.debug.emitDebugEvent(self, .ProcessFrontierUpdates);

        // Nodes whose input frontiers have changed
        // TODO is it worth tracking the actual changes? might catch cases where the total diff is zero
        var updated_nodes = u.DeepHashSet(Node).init(self.allocator);
        defer updated_nodes.deinit();

        // Process frontier updates
        // NOTE We have to process all of these before doing anything else - the intermediate states can be invalid
        while (self.unprocessed_frontier_updates.count() > 0) {

            // Find min pointstamp
            // (We have to process pointstamps in causal order to ensure that this algorithm terminates. See [/docs/why.md](/docs/why.md) for more detail.)
            // TODO use a sorted data structure for unprocessed_frontier_updates
            var iter = self.unprocessed_frontier_updates.iterator();
            var min_entry = iter.next().?;
            while (iter.next()) |entry| {
                if (orderPointstamps(entry.key_ptr.*, min_entry.key_ptr.*) == .lt)
                    min_entry = entry;
            }
            const node = min_entry.key_ptr.node_input.node;
            var input_timestamp = min_entry.key_ptr.timestamp;
            const diff = min_entry.value_ptr.*;
            _ = self.unprocessed_frontier_updates.remove(min_entry.key_ptr.*);

            dida.debug.emitDebugEvent(self, .{ .ProcessFrontierUpdate = .{ .node = node, .input_timestamp = input_timestamp, .diff = diff } });

            // An input frontier for this node changed, so we may need to take some action on it later
            try updated_nodes.put(node, {});

            // Work out how this node changes the timestamp
            var output_timestamp = switch (self.graph.node_specs[node.id]) {
                .TimestampPush => try input_timestamp.pushCoord(self.allocator),
                .TimestampIncrement => try input_timestamp.incrementCoord(self.allocator),
                .TimestampPop => try input_timestamp.popCoord(self.allocator),
                else => input_timestamp,
            };
            switch (self.graph.node_specs[node.id]) {
                .TimestampPush, .TimestampIncrement, .TimestampPop => input_timestamp.deinit(self.allocator),
                else => {},
            }
            defer output_timestamp.deinit(self.allocator);

            // Apply change to frontier
            _ = try self.applyFrontierSupportChange(node, output_timestamp, diff);
        }

        // Trigger special actions at nodes whose frontier has changed.
        // TODO Probably should pop these one at a time to avoid doWork being unbounded
        var updated_nodes_iter = updated_nodes.iterator();
        while (updated_nodes_iter.next()) |updated_nodes_entry| {
            const node = updated_nodes_entry.key_ptr.*;
            const node_spec = self.graph.node_specs[node.id];
            const node_state = &self.node_states[node.id];

            dida.debug.emitDebugEvent(self, .{ .ProcessFrontierUpdateReaction = .{ .node = node } });

            // Index-specific stuff
            if (node_spec == .Index) {
                // Might be able to produce an output batch now that the frontier has moved later
                var timestamps_to_remove = u.ArrayList(Timestamp).init(self.allocator);
                defer {
                    for (timestamps_to_remove.items) |*timestamp| timestamp.deinit(self.allocator);
                    timestamps_to_remove.deinit();
                }
                var change_batch_builder = ChangeBatchBuilder.init(self.allocator);
                defer change_batch_builder.deinit();
                var pending_changes = u.ArrayList(Change).init(self.allocator);
                defer pending_changes.deinit();
                const input_frontier = self.node_frontiers[node_spec.Index.input.id];
                for (node_state.Index.pending_changes.items) |change| {
                    if (input_frontier.frontier.causalOrder(change.timestamp) == .gt) {
                        // Have to store the timestamps separately, because we need access to all of them after the change_batch may have coalesced and freed some of them
                        try timestamps_to_remove.append(try u.deepClone(change.timestamp, self.allocator));
                        try change_batch_builder.changes.append(change);
                    } else {
                        try pending_changes.append(change);
                    }
                }
                std.mem.swap(u.ArrayList(Change), &node_state.Index.pending_changes, &pending_changes);
                if (try change_batch_builder.finishAndReset()) |change_batch| {
                    try node_state.Index.index.addChangeBatch(try u.deepClone(change_batch, self.allocator));
                    try self.emitChangeBatch(node, change_batch);
                }
                for (timestamps_to_remove.items) |timestamp| {
                    _ = try self.applyFrontierSupportChange(node, timestamp, -1);
                }
            }

            // Distinct/Reduce-specific stuff
            // TODO this is somewhat inefficient
            // TODO I think this should be looking at index input frontier
            if (node_spec == .Distinct or node_spec == .Reduce) {
                const input_node = node_spec.getInputs()[0];
                const input_frontier = self.node_frontiers[input_node.id];
                const input_index = self.node_states[input_node.id].getIndex().?;
                const output_index = node_state.getIndex().?;

                var change_batch_builder = ChangeBatchBuilder.init(self.allocator);
                defer change_batch_builder.deinit();

                var frontier_support_changes = u.ArrayList(FrontierChange).init(self.allocator);
                defer {
                    for (frontier_support_changes.items) |*frontier_support_change| frontier_support_change.deinit(self.allocator);
                    frontier_support_changes.deinit();
                }

                const pending_corrections = switch (node_state.*) {
                    .Distinct => |*state| &state.pending_corrections,
                    .Reduce => |*state| &state.pending_corrections,
                    else => unreachable,
                };
                var key_iter = pending_corrections.iterator();
                while (key_iter.next()) |key_entry| {
                    const key = key_entry.key_ptr.*;
                    const timestamps = key_entry.value_ptr;

                    // Going to check any pending timestamp that is before the new input frontier
                    var timestamps_to_check = u.ArrayList(Timestamp).init(self.allocator);
                    defer {
                        for (timestamps_to_check.items) |*timestamp_to_check| timestamp_to_check.deinit(self.allocator);
                        timestamps_to_check.deinit();
                    }
                    {
                        var timestamp_iter = timestamps.iterator();
                        while (timestamp_iter.next()) |timestamp_entry| {
                            const timestamp = timestamp_entry.key_ptr.*;
                            if (input_frontier.frontier.causalOrder(timestamp) == .gt) {
                                try timestamps_to_check.append(try u.deepClone(timestamp, self.allocator));
                                try frontier_support_changes.append(.{ .timestamp = timestamp, .diff = -1 });
                            }
                        }
                    }
                    for (timestamps_to_check.items) |timestamp_to_check| {
                        _ = timestamps.remove(timestamp_to_check);
                    }

                    // Sort timestamps so that when we reach each one we've already taken into account previous corrections
                    std.sort.sort(Timestamp, timestamps_to_check.items, {}, struct {
                        fn lessThan(_: void, a: Timestamp, b: Timestamp) bool {
                            return a.lexicalOrder(b) == .lt;
                        }
                    }.lessThan);

                    // Get past inputs for this key
                    // TODO a sorted iterator would be nicer for this
                    var input_changes = u.ArrayList(Change).init(self.allocator);
                    defer input_changes.deinit();
                    try input_index.getChangesForKey(key, key.values.len, &input_changes);

                    // Figure out correction for each timestamp
                    var new_output_changes = ChangeBatchBuilder.init(self.allocator);
                    defer new_output_changes.deinit();
                    for (timestamps_to_check.items) |timestamp_to_check| {
                        switch (node_spec) {
                            .Distinct => {
                                // Calculate the correct count for this row
                                var input_count: isize = 0;
                                for (input_changes.items) |input_change| {
                                    if (input_change.timestamp.causalOrder(timestamp_to_check).isLessThanOrEqual())
                                        input_count += input_change.diff;
                                }

                                // Calculate what we're currently saying the count is for this row
                                var output_count = output_index.getCountForRowAsOf(key, timestamp_to_check);

                                // If needed, issue a correction
                                const correct_output_count: isize = if (input_count == 0) 0 else 1;
                                const diff = correct_output_count - output_count;
                                if (diff != 0) {
                                    try new_output_changes.changes.append(.{
                                        .row = try u.deepClone(key, self.allocator),
                                        .timestamp = try u.deepClone(timestamp_to_check, self.allocator),
                                        .diff = diff,
                                    });
                                }
                            },
                            .Reduce => |spec| {
                                // Coalesce inputs so reduce_fn only has to deal with positive diffs
                                var input_bag = Bag.init(self.allocator);
                                defer input_bag.deinit();
                                for (input_changes.items) |input_change| {
                                    if (input_change.timestamp.causalOrder(timestamp_to_check).isLessThanOrEqual())
                                        try input_bag.update(input_change.row, input_change.diff);
                                }

                                // Reduce fn might not be commutative, so it has to process changes in some well-defined order
                                const RowAndCount = struct { row: Row, count: usize };
                                var sorted_inputs = u.ArrayList(RowAndCount).init(self.allocator);
                                defer sorted_inputs.deinit();
                                var input_bag_iter = input_bag.rows.iterator();
                                while (input_bag_iter.next()) |input_bag_entry|
                                    try sorted_inputs.append(.{
                                        .row = input_bag_entry.key_ptr.*,
                                        .count = @intCast(usize, input_bag_entry.value_ptr.*),
                                    });
                                std.sort.sort(RowAndCount, sorted_inputs.items, {}, (struct {
                                    fn lessThan(_: void, a: RowAndCount, b: RowAndCount) bool {
                                        return u.deepOrder(a, b) == .lt;
                                    }
                                }).lessThan);

                                // Calculate the correct reduced value
                                var input_value = try u.deepClone(spec.init_value, self.allocator);
                                for (sorted_inputs.items) |input| {
                                    const new_input_value = try spec.reducer.reduce_fn(spec.reducer, input_value, input.row, input.count);
                                    input_value.deinit(self.allocator);
                                    input_value = new_input_value;
                                }

                                // Cancel all previous outputs for this key
                                var output_changes = u.ArrayList(Change).init(self.allocator);
                                defer output_changes.deinit();
                                // TODO query index as of timestamp_to_check
                                try output_index.getChangesForKey(key, key.values.len, &output_changes);
                                for (output_changes.items) |candidate_output_change| {
                                    if (candidate_output_change.timestamp.causalOrder(timestamp_to_check).isLessThanOrEqual())
                                        try new_output_changes.changes.append(.{
                                            .row = try u.deepClone(candidate_output_change.row, self.allocator),
                                            .timestamp = try u.deepClone(timestamp_to_check, self.allocator),
                                            .diff = -candidate_output_change.diff,
                                        });
                                }

                                // Add the new output
                                var values = try std.mem.concat(self.allocator, Value, &[_][]const Value{
                                    key.values,
                                    &[_]Value{input_value},
                                });
                                for (values[0..key.values.len]) |*value| value.* = try u.deepClone(value.*, self.allocator);
                                try new_output_changes.changes.append(.{
                                    .row = Row{ .values = values },
                                    .timestamp = try u.deepClone(timestamp_to_check, self.allocator),
                                    .diff = 1,
                                });
                            },
                            else => unreachable,
                        }

                        if (try new_output_changes.finishAndReset()) |change_batch| {
                            try change_batch_builder.changes.appendSlice(change_batch.changes);
                            for (change_batch.changes) |*change|
                                change.* = try u.deepClone(change.*, self.allocator);
                            try output_index.addChangeBatch(change_batch);
                        }
                    }
                }
                // TODO if timestamps now empty for a row, remove entry

                // Emit changes
                if (try change_batch_builder.finishAndReset()) |change_batch| {
                    try self.emitChangeBatch(node, change_batch);
                }

                // Remove frontier support
                for (frontier_support_changes.items) |frontier_support_change|
                    _ = try self.applyFrontierSupportChange(node, frontier_support_change.timestamp, frontier_support_change.diff);
            }
        }
    }

    /// Check whether the shard has any work that it could do.
    pub fn hasWork(self: *const Shard) bool {
        return (self.unprocessed_change_batches.items.len > 0) or
            (self.unprocessed_frontier_updates.count() > 0);
    }

    /// Do some work.
    // TODO ideally the runtime of this function would be roughly bounded, so that dida can run cooperatively inside other event loops.
    pub fn doWork(self: *Shard) !void {
        dida.debug.emitDebugEvent(self, .DoWork);

        if (self.unprocessed_change_batches.items.len > 0) {
            try self.processChangeBatch();
        } else if (self.unprocessed_frontier_updates.count() > 0) {
            try self.processFrontierUpdates();
        }
    }

    /// Pop a change batch from an output node.
    /// Caller takes ownership of the result.
    pub fn popOutput(self: *Shard, node: Node) ?ChangeBatch {
        const change_batch = self.node_states[node.id].Output.unpopped_change_batches.popOrNull();

        dida.debug.emitDebugEvent(self, .{ .PopOutput = .{ .node = node, .change_batch = change_batch } });

        return change_batch;
    }
};

// TODO It's currently possible to remove from HashMap without invalidating iterator which would simplify some of the code in this file. But might not be true forever.

// TODO Need to decide which types store allocators vs taking them as args, and be careful to allocate/free from the correct allocator
