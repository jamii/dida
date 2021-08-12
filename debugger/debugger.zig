usingnamespace @import("../bindings/js_common.zig");
pub const abi = @import("../bindings/wasm/abi.zig");

pub const dida_test = @import("../test/core.zig");

var shards = std.ArrayList(dida.core.Shard).init(allocator);
var debug_events = std.ArrayList(dida.debug.DebugEvent).init(allocator);

// Called from dida.debug
pub fn emitDebugEvent(shard: *const dida.core.Shard, debug_event: dida.debug.DebugEvent) void {
    abi.js.consoleLog(abi.createString({}, "emit"));
    tryEmitDebugEvent(shard, debug_event) catch
        dida.common.panic("OOM", .{});
}
pub fn tryEmitDebugEvent(shard: *const dida.core.Shard, debug_event: dida.debug.DebugEvent) error{OutOfMemory}!void {
    try shards.append(try dida.meta.deepClone(shard.*, allocator));
    try debug_events.append(try dida.meta.deepClone(debug_event, allocator));
}

comptime {
    {
        const name = "frame";
        const function = frame;
        const num_args = @typeInfo(@TypeOf(function)).Fn.args.len;
        const exported_function = abi.handleAbiForFunction(num_args, handleSerdeForFunction(function));
        @export(
            exported_function,
            .{
                .name = name,
                .linkage = .Strong,
            },
        );
    }
}

pub const panic = @import("../bindings/wasm/runtime.zig").panic;

fn frame() []const u8 {
    run_test() catch |err| dida.common.panic("{}", .{err});

    var string = std.ArrayList(u8).init(allocator);
    dida.debug.dumpInto(string.writer(), 0, debug_events.items) catch |err| dida.common.panic("{}", .{err});
    dida.debug.dumpInto(string.writer(), 0, shards.items) catch |err| dida.common.panic("{}", .{err});
    return string.items;
}

fn run_test() !void {
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
                output_values[0] = try dida.meta.deepClone(input.values[1], allocator);
                output_values[1] = try dida.meta.deepClone(input.values[2], allocator);
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
                output_values[0] = try dida.meta.deepClone(input.values[0], allocator);
                output_values[1] = try dida.meta.deepClone(input.values[2], allocator);
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
                output_values[0] = try dida.meta.deepClone(input.values[0], allocator);
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
    while (account < std.math.maxInt(u4)) : (account += 1) {
        const row = dida.core.Row{ .values = &[_]dida.core.Value{
            .{ .Number = @intToFloat(f64, account) },
            .{ .Number = @intToFloat(f64, account) },
            .{ .Number = @intToFloat(f64, 0) },
        } };
        const timestamp = dida.core.Timestamp{ .coords = &[_]usize{0} };
        try shard.pushInput(transactions, .{ .row = try dida.meta.deepClone(row, allocator), .timestamp = try dida.meta.deepClone(timestamp, allocator), .diff = 1 });
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
    //try shard.pushInput(transactions, .{ .row = try dida.meta.deepClone(row,allocator), .timestamp = try dida.meta.deepClone(timestamp.clone, allocator), .diff = 1 });
    //try shard.advanceInput(transactions, .{ .coords = &[_]usize{time + 1} });
    //while (shard.hasWork()) try shard.doWork();
    //
    //_ = total_balance_out;
    ////try dida_test.testNodeOutput(&shard, total_balance_out, .{});
    //}
}
