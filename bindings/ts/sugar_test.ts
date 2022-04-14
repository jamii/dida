import { assertEquals } from "https://deno.land/std@0.132.0/testing/asserts.ts";
import Abi from "../wasm/abi-with-bytes.js";
import Dida from "../wasm/zig-out/lib/dida.mjs";
import { Buffer } from "https://deno.land/std@0.128.0/io/mod.ts";
import { Sugar, Subgraph, Node, OutputNode } from "./sugar.ts";

async function loadDida(): Promise<Dida> {
  const file = await Deno.open("../wasm/zig-out/lib/dida.wasm");
  const b = new Buffer();
  await b.readFrom(file);
  file.close()

  const abi = await Abi(b.bytes());
  return new Dida(abi);
}

Deno.test("sugar example", async () => {
  const dida = await loadDida()

  const sugar = new Sugar(dida);

  const edges = sugar.input<[string, string]>()

  const loop = sugar.loop()

  const edges1 = loop.importNode<[string, string]>(edges)

  const reach = loop.loopNode<[string, string]>()

  reach.fixpoint(
    reach
      .index()
      .join(
        // Swap columns
        edges1.map(v => [v[1], v[0]]).index(),
        1
      )
      // Without the first key, and flip order again
      .map(v => [v[2], v[1]])
      .union(edges1)
      .index()
      .distinct()
  )


  const out = loop.exportNode(reach).output()

  sugar.build()

  edges.push(["a", "b"], [0], 1);
  edges.push(["b", "c"], [0], 1);
  edges.push(["b", "d"], [0], 1);
  edges.push(["c", "a"], [0], 1);
  edges.push(["b", "c"], [1], -1);
  edges.flush()

  const sugarChanges = []

  for (const i of [1, 2, 3]) {
    edges.advance([i])
    sugar.doAllWork()
    for (const v of out) {
      console.log("reach_out");
      console.log(v)
      sugarChanges.push(v)
    }
  }

  assertEquals(await unsweetDidaOutput(), sugarChanges)
});

async function unsweetDidaOutput() {
  const changes = []

  const dida = await loadDida()

  var graph_builder = new dida.GraphBuilder();

  const subgraph_0 = new dida.Subgraph(0);
  const subgraph_1 = graph_builder.addSubgraph(subgraph_0);

  const edges = graph_builder.addNode(subgraph_0, new dida.NodeSpec.Input());
  const edges_1 = graph_builder.addNode(
    subgraph_1,
    new dida.NodeSpec.TimestampPush(edges),
  );


  const reach_future = graph_builder.addNode(
    subgraph_1,
    new dida.NodeSpec.TimestampIncrement(null),
  );


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
    new dida.NodeSpec.Map(edges_1, (input: any) => [input[1], input[0]]),
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
    new dida.NodeSpec.Map(joined, (input: any) => [input[2], input[1]]),
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
      changes.push(change_batch)
    }
  }

  console.log("Advancing!");
  shard.advanceInput(edges, [2]);

  while (shard.hasWork()) {
    shard.doWork();
    while (true) {
      const change_batch = shard.popOutput(reach_out);
      if (change_batch == undefined) break;
      changes.push(change_batch)
    }
  }

  console.log("Advancing!");
  shard.advanceInput(edges, [3]);
  while (shard.hasWork()) {
    shard.doWork();
    while (true) {
      const change_batch = shard.popOutput(reach_out);
      if (change_batch == undefined) break;
      changes.push(change_batch)
    }
  }

  return changes
}
