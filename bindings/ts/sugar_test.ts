import { assertEquals } from "https://deno.land/std@0.132.0/testing/asserts.ts";
import Abi from "../wasm/abi-with-bytes.js";
import Dida from "../wasm/zig-out/lib/dida.mjs";
import { Buffer } from "https://deno.land/std/io/mod.ts";
import { Sugar, Subgraph, Node, OutputNode } from "./sugar.ts";

async function loadDida(): Promise<Dida> {
  const file = await Deno.open("../wasm/zig-out/lib/dida.wasm");
  const b = new Buffer();
  await b.readFrom(file);

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
        edges1.map(v => [v[1], v[0]]
        ).index(),
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
  edges.advance([1])

  for (const v of out) {
    console.log("reach_out");
    console.log(v)
  }


});

Deno.test("add test", () => {
  const a = 1 + 1
  assertEquals(a, 2);
  // test 2 + 2 = 4
  const b = 2 + 2
  assertEquals(b, 4);
});

