import { assertEquals } from "https://deno.land/std@0.132.0/testing/asserts.ts";
import Abi from "../wasm/abi-with-bytes.js";
import Dida from "../wasm/zig-out/lib/dida.mjs";
import { Buffer } from "https://deno.land/std/io/mod.ts";
import { Sugar, Subgraph, Node, OutputNode } from "./sugar.ts";

Deno.test("sugar example", async () => {
  const file = await Deno.open("../wasm/zig-out/lib/dida.wasm");
  const b = new Buffer();
  await b.readFrom(file);
  console.log(typeof b, typeof b.bytes());
  file.close()

  const abi = await Abi(b.bytes());
  const dida = new Dida(abi);

  const sugar = new Sugar(dida);

  const edges = sugar.input()
  const loop = sugar.loop()
  const edges1 = loop.importNode(edges)

  // const edges = sugar.input()

});

Deno.test("add test", () => {
  const a = 1 + 1
  assertEquals(a, 2);
  // test 2 + 2 = 4
  const b = 2 + 2
  assertEquals(b, 4);
});

