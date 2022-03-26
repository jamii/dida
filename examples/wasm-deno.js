import Abi from "../bindings/wasm/abi-with-bytes.js";
import Dida from "../bindings/wasm/zig-out/lib/dida.mjs";
import { Buffer } from "https://deno.land/std/io/mod.ts";

function probeHelper(dida, graph_builder, name, subgraph, node) {
  const probe_out = graph_builder.addNode(
    subgraph,
    new dida.NodeSpec.Output(node),
  );

  return function (shard) {
    while (true) {
      const change_batch = shard.popOutput(probe_out);
      if (change_batch == undefined) break;
      console.log(name);
      console.log(change_batch);
    }
  }
}

function exampleQuery(dida) {
  const probeFns = [];
  var graph_builder = new dida.GraphBuilder();
  const probe = (...args) => probeHelper(dida, graph_builder, ...args);

  const subgraph_0 = new dida.Subgraph(0);
  const subgraph_1 = graph_builder.addSubgraph(subgraph_0);

  const edges = graph_builder.addNode(subgraph_0, new dida.NodeSpec.Input());
  const edges_1 = graph_builder.addNode(
    subgraph_1,
    new dida.NodeSpec.TimestampPush(edges),
  );

  probeFns.push(probe("edges", subgraph_0, edges));

  const reach_future = graph_builder.addNode(
    subgraph_1,
    new dida.NodeSpec.TimestampIncrement(null),
  );

  probeFns.push(probe("reach_future", subgraph_1, reach_future));

  const reach_index = graph_builder.addNode(
    subgraph_1,
    new dida.NodeSpec.Index(reach_future),
  );

  const distinct_reach_index = graph_builder.addNode(
    subgraph_1,
    new dida.NodeSpec.Distinct(reach_index),
  );

  const swapped_edges = graph_builder.addNode(
    subgraph_1,
    new dida.NodeSpec.Map(edges_1, (input) => [input[1], input[0]]),
  );
  const swapped_edges_index = graph_builder.addNode(
    subgraph_1,
    new dida.NodeSpec.Index(swapped_edges),
  );

  const joined = graph_builder.addNode(
    subgraph_1,
    new dida.NodeSpec.Join([distinct_reach_index, swapped_edges_index], 1),
  );

  const without_middle = graph_builder.addNode(
    subgraph_1,
    new dida.NodeSpec.Map(joined, (input) => [input[2], input[1]]),
  );
  const reach = graph_builder.addNode(
    subgraph_1,
    new dida.NodeSpec.Union([edges_1, without_middle]),
  );
  graph_builder.connectLoop(reach, reach_future);
  const reach_pop = graph_builder.addNode(
    subgraph_0,
    new dida.NodeSpec.TimestampPop(distinct_reach_index),
  );
  const reach_out = graph_builder.addNode(
    subgraph_0,
    new dida.NodeSpec.Output(reach_pop),
  );

  const reach_summary = graph_builder.addNode(
    subgraph_1,
    new dida.NodeSpec.Reduce(
      distinct_reach_index,
      1,
      "",
      function (reduced_value, row, count) {
        for (var i = 0; i < count; i++) {
          reduced_value += row[1];
        }
        return reduced_value;
      },
    ),
  );
  const reach_summary_out = graph_builder.addNode(
    subgraph_1,
    new dida.NodeSpec.Output(reach_summary),
  );

  const graph = graph_builder.finishAndReset();

  var shard = new dida.Shard(graph);

  shard.pushInput(edges, new dida.Change(["a", "b"], [0], 1));
  shard.pushInput(edges, new dida.Change(["b", "c"], [0], 1));
  shard.pushInput(edges, new dida.Change(["b", "d"], [0], 1));
  shard.pushInput(edges, new dida.Change(["c", "a"], [0], 1));
  shard.pushInput(edges, new dida.Change(["b", "c"], [1], -1));
  shard.flushInput(edges);

  shard.advanceInput(edges, [1]);
  while (shard.hasWork()) {
    shard.doWork();
    while (true) {
      const change_batch = shard.popOutput(reach_out);
      if (change_batch == undefined) break;
      console.log("reach_out");
      console.log(change_batch);
    }
    while (true) {
      const change_batch = shard.popOutput(reach_summary_out);
      if (change_batch == undefined) break;
      console.log("reach_summary_out");
      console.log(change_batch);
    }
  }

  console.log("Advancing!");

  shard.advanceInput(edges, [2]);

  while (shard.hasWork()) {
    shard.doWork();
    for (const fn of probeFns) {
      fn(shard);
    }
    while (true) {
      const change_batch = shard.popOutput(reach_out);
      if (change_batch == undefined) break;
      console.log("reach_out");
      console.log(change_batch);
    }
    while (true) {
      const change_batch = shard.popOutput(reach_summary_out);
      if (change_batch == undefined) break;
      console.log("reach_summary_out");
      console.log(change_batch);
    }
  }

  console.log("Advancing!");
  shard.advanceInput(edges, [3]);
  while (shard.hasWork()) {
    shard.doWork();
    while (true) {
      const change_batch = shard.popOutput(reach_out);
      if (change_batch == undefined) break;
      console.log("reach_out");
      console.log(change_batch);
    }
    while (true) {
      const change_batch = shard.popOutput(reach_summary_out);
      if (change_batch == undefined) break;
      console.log("reach_out_summary");
      console.log(change_batch);
    }
  }
}

async function run() {
  const file = await Deno.open("./bindings/wasm/zig-out/lib/dida.wasm");
  const b = new Buffer();
  await b.readFrom(file);
  console.log(typeof b, typeof b.bytes());

  const abi = await Abi(b.bytes());
  const dida = new Dida(abi);
  exampleQuery(dida);
}
run();
