import Dida from '../wasm/zig-out/lib/types/dida'


// type Dida = {
//   GraphBuilder: DidaBuilderClass
// } & any
type DidaBuilder = any
type DidaSubgraph = any
type DidaNode = any
type DidaBuilderClass = {
  new(): DidaBuilder
}

type SugarState =
  { state: "building", builder: DidaBuilder, rootSubGraph: Subgraph }
  | { state: "running" }

export class Sugar {
  state: SugarState
  dida: Dida

  constructor(dida: Dida) {
    const builder = new dida.GraphBuilder()
    this.state = { state: "building", builder: new dida.GraphBuilder(), rootSubGraph: builder }
    this.dida = dida
  }

  get builder(): DidaBuilder {
    if (this.state.state !== "building") {
      throw new Error("Sugar is not in building state")
    }
    return this.state.builder
  }

  get rootSubGraph(): Subgraph {
    if (this.state.state !== "building") {
      throw new Error("Sugar is not in building state")
    }
    return this.state.rootSubGraph
  }


  input(): InputNode {
    const builder = this.builder()
    const rootGraph = this.rootSubGraph()

    const node = builder.addNode(rootGraph, new this.dida.NodeSpec.Input());
    const edges = graph_builder.addNode(subgraph_0, new dida.NodeSpec.Input());

    const edges = this.s.addNode(subgraph_0, new dida.NodeSpec.Input());
    return new InputNode(this, null)
  }

  loop(): Subgraph {
    if (this.state.state !== "building") {
      throw new Error("Cannot loop on a graph that is not being built")
    }

    this.state.builder.add
    const subgraph_1 = this.state.builder.addSubgraph(this.state.rootSubGraph);
    return new Subgraph(this, subgraph_1)
  }

  build() { }
  doAllWork() { }
}

export class Subgraph {
  readonly sugar: Sugar
  readonly inner: DidaSubgraph

  constructor(sugar: Sugar, inner: DidaSubgraph) {
    this.sugar = sugar
    this.inner = inner
  }

  // loop(): Subgraph {
  //   return new Subgraph()
  // }

  // loopNode(): Node {
  //   return new Node()
  // }

  importNode(n: Node): Node {
    if (this.sugar.state.state !== "building") {
      throw new Error("Not in building state")
    }

    // TODO add check for
    // "Can only import from parent subgraph into child subgraph",
    console.log("asdf")
    console.log(this.inner)
    console.log(this.inner.id)

    return new Node(this.sugar, this.inner)
  }

  // exportNode(n: Node): Node {
  //   return new Node()
  // }
}

export class Node {
  readonly sugar: Sugar
  readonly inner: DidaNode

  constructor(sugar: Sugar, inner: DidaNode) {
    this.sugar = sugar
    this.inner = inner
  }

  // index(): Node {
  //   return new Node(this.sugar)
  // }

  map(f: any): this {
    return this
  }
  reduce(): this {
    return this
  }
  project(): this {
    return this
  }
  union(): this {
    return this
  }

  // output(f: any): OutputNode {
  //   return new OutputNode()
  // }
}

export class InputNode extends Node {

  push() { }
  flush() { }
  advance() { }
}

export class OutputNode extends Node {
  pop() { }
}

export class JoinedNode extends Node { }

export class IndexedNode extends Node {
  distinct(): this {
    return this
  }
  join(): this {
    return this
  }
}

export class DistinctNode extends IndexedNode { }
export class ReduceNode extends IndexedNode { }

export class TimestampPushedNode extends Node { }

// type HasIndex<T: Node> = T extends IndexedNode ? true : T extends DistinctNode ? T exte