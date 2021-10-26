const std = @import("std");
const zt = @import("zt");
const ig = @import("imgui");
const zg = zt.custom_components;

const global_allocator = std.heap.c_allocator;

pub fn main() !void {
    try run_test();
    run();
}

pub fn run() void {
    var i: usize = 0;

    const Context = zt.App(void);
    var context = Context.begin(global_allocator, {});
    context.settings.energySaving = false;
    while (context.open) {
        context.beginFrame();
        const viewport = ig.igGetMainViewport();
        ig.igSetNextWindowPos(viewport.*.Pos, 0, .{});
        ig.igSetNextWindowSize(viewport.*.Size, 0);
        var open = true;
        if (ig.igBegin(
            "The window",
            &open,
            ig.ImGuiWindowFlags_NoDecoration |
                ig.ImGuiWindowFlags_NoBackground |
                ig.ImGuiWindowFlags_AlwaysAutoResize |
                ig.ImGuiWindowFlags_NoSavedSettings |
                ig.ImGuiWindowFlags_NoFocusOnAppearing |
                ig.ImGuiWindowFlags_NoNav,
        )) {
            if (ig.igButton("<<", .{}))
                i = 0;
            ig.igSameLine(0, 0);
            if (ig.igButton("<", .{}) and i > 0)
                i -= 1;
            ig.igSameLine(0, 0);
            var c_i = @intCast(c_int, i);
            if (ig.igDragInt("##i", &c_i, 1.0, 0, @intCast(c_int, debug_events.items.len - 1), "%d", 0))
                i = @intCast(usize, c_i);
            ig.igSameLine(0, 0);
            if (ig.igButton(">", .{}) and i < debug_events.items.len - 1)
                i += 1;
            ig.igSameLine(0, 0);
            if (ig.igButton(">>", .{}))
                i = debug_events.items.len - 1;

            const State = struct {
                prev_event: dida.debug.DebugEvent,
                next_event: ?dida.debug.DebugEvent,
                validation_errors: []const dida.debug.ValidationError,
                shard: dida.core.Shard,
            };
            inspect(global_allocator, "root", State{
                .prev_event = debug_events.items[i],
                .next_event = if (i + 1 == debug_events.items.len) null else debug_events.items[i + 1],
                .shard = shards.items[i],
                .validation_errors = validation_errors.items[i],
            });
        }
        ig.igEnd();
        context.endFrame();

        // Hacky way to check if stderr was closed
        const stderr = std.io.getStdErr().writer();
        stderr.print("Still alive!\n", .{}) catch return;
    }
    context.deinit();
}

fn inspect(allocator: *std.mem.Allocator, name: []const u8, thing: anytype) void {
    const T = @TypeOf(thing);
    if (treeNodeFmt("{s}", .{name})) {
        ig.igSameLine(0, 0);
        zg.ztText(": {s}", .{@typeName(T)});
        switch (@typeInfo(T)) {
            .Int => zg.ztText("{d} 0o{o} 0b{b}", .{ thing, thing, thing }),
            .Struct => |info| {
                if (comptime std.mem.startsWith(u8, @typeName(T), "std.array_list.ArrayList")) {
                    for (thing.items) |elem, i| {
                        inspect(allocator, zg.fmtTextForImgui("{}", .{i}), elem);
                    }
                } else if (comptime std.mem.startsWith(u8, @typeName(T), "std.hash_map.HashMap")) {
                    var iter = thing.iterator();
                    var i: usize = 0;
                    while (iter.next()) |entry| {
                        // TODO is there a better way to name these?
                        inspect(allocator, zg.fmtTextForImgui("{}", .{i}), T.KV{
                            .key = entry.key_ptr.*,
                            .value = entry.value_ptr.*,
                        });
                        i += 1;
                    }
                } else inline for (info.fields) |field_info| {
                    inspect(allocator, field_info.name, @field(thing, field_info.name));
                }
            },
            .Union => |info| {
                if (info.tag_type) |tag_type| {
                    inline for (@typeInfo(tag_type).Enum.fields) |field_info| {
                        if (std.meta.activeTag(thing) == @intToEnum(tag_type, field_info.value)) {
                            inspect(allocator, field_info.name, @field(thing, field_info.name));
                        }
                    }
                }
            },
            .Pointer => |info| {
                switch (info.size) {
                    .One => inspect(allocator, "*", thing.*),
                    .Many => zg.ztText("{any}", .{thing}),
                    .Slice => for (thing) |elem, i| {
                        inspect(allocator, zg.fmtTextForImgui("{}", .{i}), elem);
                    },
                    .C => zg.ztText("{any}", .{thing}),
                }
            },
            else => zg.ztText("{any}", .{thing}),
        }
        ig.igTreePop();
    } else {
        ig.igSameLine(0, 0);
        if (@typeInfo(T) == .Pointer and
            @typeInfo(T).Pointer.size == .Slice and
            @typeInfo(T).Pointer.child == u8)
            zg.ztText(" = {s}", .{thing})
        else
            zg.ztText(" = {any}", .{thing});
    }
}

fn treeNodeFmt(comptime fmt: []const u8, args: anytype) bool {
    const text = zg.fmtTextForImgui(fmt, args);
    return ig.igTreeNode_Str(text);
}

const dida = @import("../lib/dida.zig");
const dida_test = @import("../test/core.zig");

var shards = std.ArrayList(dida.core.Shard).init(global_allocator);
var debug_events = std.ArrayList(dida.debug.DebugEvent).init(global_allocator);
var validation_errors = std.ArrayList([]const dida.debug.ValidationError).init(global_allocator);

// Called from dida.debug
pub fn emitDebugEvent(shard: *const dida.core.Shard, debug_event: dida.debug.DebugEvent) void {
    tryEmitDebugEvent(shard, debug_event) catch
        dida.common.panic("OOM", .{});
}
pub fn tryEmitDebugEvent(shard: *const dida.core.Shard, debug_event: dida.debug.DebugEvent) error{OutOfMemory}!void {
    try shards.append(try dida.meta.deepClone(shard.*, global_allocator));
    try debug_events.append(try dida.meta.deepClone(debug_event, global_allocator));
    try validation_errors.append(dida.debug.validate(global_allocator, shard));
}

fn run_test() !void {
    var graph_builder = dida.core.GraphBuilder.init(global_allocator);
    defer graph_builder.deinit();

    const subgraph_0 = dida.core.Subgraph{ .id = 0 };

    // transactions look like (from, to, amount)
    const transactions = try graph_builder.addNode(subgraph_0, .Input);

    var credits_mapper = dida.core.NodeSpec.MapSpec.Mapper{
        .map_fn = (struct {
            fn map(_: *dida.core.NodeSpec.MapSpec.Mapper, input: dida.core.Row) error{OutOfMemory}!dida.core.Row {
                // (to, amount)
                var output_values = try global_allocator.alloc(dida.core.Value, 2);
                output_values[0] = try dida.meta.deepClone(input.values[1], global_allocator);
                output_values[1] = try dida.meta.deepClone(input.values[2], global_allocator);
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
                var output_values = try global_allocator.alloc(dida.core.Value, 2);
                output_values[0] = try dida.meta.deepClone(input.values[0], global_allocator);
                output_values[1] = try dida.meta.deepClone(input.values[2], global_allocator);
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
                var output_values = try global_allocator.alloc(dida.core.Value, 2);
                output_values[0] = try dida.meta.deepClone(input.values[0], global_allocator);
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

    var shard = try dida.core.Shard.init(global_allocator, &graph);
    defer shard.deinit();

    // TODO this is a hack to get around the fact that empty reduces don't return any results, which makes the join not work out
    var account: usize = 0;
    while (account < std.math.maxInt(u4)) : (account += 1) {
        const row = dida.core.Row{ .values = &[_]dida.core.Value{
            .{ .Number = @intToFloat(f64, account) },
            .{ .Number = @intToFloat(f64, account) },
            .{ .Number = @intToFloat(f64, 0) },
        } };
        const timestamp = dida.core.Timestamp{ .coords = &[_]usize{0} };
        try shard.pushInput(transactions, .{ .row = try dida.meta.deepClone(row, global_allocator), .timestamp = try dida.meta.deepClone(timestamp, global_allocator), .diff = 1 });
    }
    try shard.advanceInput(transactions, .{ .coords = &[_]usize{1} });

    while (shard.hasWork()) try shard.doWork();

    _ = total_balance_out;
    //try dida_test.testNodeOutput(&shard, total_balance_out, .{.{.{ .{0}, .{0}, 1 }}});

    //var rng = std.rand.DefaultPrng.init(0);
    //var time: usize = 1;
    // TODO this test actually fails for larger values of time
    //while (time < 10) : (time += 1) {
    //const from_account = rng.random.int(u4);
    //const to_account = rng.random.int(u4);
    //const amount = rng.random.int(u8);
    //const skew = @intCast(usize, 0); // TODO rng.random.int(u2);
    //const row = dida.core.Row{ .values = &[_]dida.core.Value{
    //.{ .Number = @intToFloat(f64, from_account) },
    //.{ .Number = @intToFloat(f64, to_account) },
    //.{ .Number = @intToFloat(f64, amount) },
    //} };
    //const timestamp = dida.core.Timestamp{ .coords = &[_]usize{time + @as(usize, skew)} };
    //try shard.pushInput(transactions, .{ .row = try dida.meta.deepClone(row,global_allocator), .timestamp = try dida.meta.deepClone(timestamp.clone, global_allocator), .diff = 1 });
    //try shard.advanceInput(transactions, .{ .coords = &[_]usize{time + 1} });
    //while (shard.hasWork()) try shard.doWork();
    //
    //_ = total_balance_out;
    ////try dida_test.testNodeOutput(&shard, total_balance_out, .{});
    //}
}
