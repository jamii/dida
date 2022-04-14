import Dida from '../wasm/zig-out/lib/types/dida.d.mts'

type DidaBuilder = InstanceType<Dida["GraphBuilder"]>
type DidaGraph = any

type SugarState =
  { state: "building", builder: Builder, rootSubGraph: Subgraph }
  | { state: "running", shard: InstanceType<Dida["Shard"]> }

type UglyHackToPreserverType<A, T extends Array<any>, G extends Array<any>> =
  A extends IndexedNode<T> ? IndexedNode<G> :
  A extends Node<T> ? Node<G> : A


type Any1<T> = any


export class Sugar {
  state: SugarState
  dida: Dida

  constructor(dida: Dida) {
    const rootID = 0
    const builder = new dida.GraphBuilder()
    const rootSubGraphInner = new dida.Subgraph(rootID);
    const rootSubGraph = new Subgraph(this, null, rootSubGraphInner)

    this.state = { state: "building", builder: new Builder(this, builder), rootSubGraph }
    this.dida = dida
  }

  get shard(): InstanceType<Dida["Shard"]> {
    if (this.state.state !== "running") {
      throw new Error("Sugar is not in building state")
    }
    return this.state.shard
  }

  get builder(): Builder {
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


  input<T extends Array<any>>(): InputNode<T> {
    const builder = this.builder
    const rootGraph = this.rootSubGraph

    const node = builder.addNode(rootGraph, new this.dida.NodeSpec.Input());
    return new InputNode<T>(this, node, rootGraph)
  }

  loop(): Subgraph {
    if (this.state.state !== "building") {
      throw new Error("Cannot loop on a graph that is not being built")
    }

    return this.builder.addSubgraph(this.state.rootSubGraph)
  }

  build() {
    const graph = this.builder.finishAndReset();
    this.state = { state: "running", shard: new this.dida.Shard(graph) }
  }

  doAllWork() {
    while (this.shard.hasWork()) {
      this.shard.doWork()
    }
  }
}

export class Subgraph {
  readonly sugar: Sugar
  readonly inner: InstanceType<Dida["Subgraph"]>
  readonly parent: Subgraph | null

  constructor(sugar: Sugar, parent: Subgraph["parent"], inner: Subgraph["inner"]) {
    this.sugar = sugar
    this.inner = inner
    this.parent = parent
  }

  loopNode<T extends Array<any>>(): TimestampIncrementedNode<T> {
    const builder = this.sugar.builder

    const nodeInner = builder.addNode(this, new this.sugar.dida.NodeSpec.TimestampIncrement(null))

    return new TimestampIncrementedNode(this.sugar, nodeInner, this)
  }

  importNode<T extends Array<any>>(n: Node<T>): TimestampPushedNode<T> {
    const builder = this.sugar.builder
    // TODO add check for
    // "Can only import from parent subgraph into child subgraph",

    const nodeInner = builder.addNode(
      this,
      new this.sugar.dida.NodeSpec.TimestampPush(n.inner)
    )

    return new TimestampPushedNode(this.sugar, nodeInner, this)
  }

  exportNode<T extends Array<any>>(n: Node<T>): TimestampPoppedNode<T> {
    const builder = this.sugar.builder
    // TODO add check for
    // "Can only export from child subgraph into parent subgraph",

    if (this.parent == null) {
      throw new Error("Cannot export from root subgraph")
    }

    const nodeInner = builder.addNode(
      this.parent,
      new this.sugar.dida.NodeSpec.TimestampPop(n.inner)
    )

    return new TimestampPoppedNode(this.sugar, nodeInner, this.parent)
  }
}

export class Node<T extends Array<any>> {
  readonly sugar: Sugar
  readonly inner: NodeInner
  readonly subgraph: Subgraph

  constructor(sugar: Sugar, inner: Node<T>["inner"], subgraph: Node<T>["subgraph"]) {
    this.sugar = sugar
    this.inner = inner
    this.subgraph = subgraph
  }

  index(): IndexedNode<T> {
    const nodeInner = this.sugar.builder.addNode(
      this.subgraph,
      new this.sugar.dida.NodeSpec.Index(this.inner)
    )

    return new IndexedNode(this.sugar, nodeInner, this.subgraph)
  }

  mapInner<G extends Array<any>>(f: (input: T) => G): Node<G> {
    const nodeInner = this.sugar.builder.addNode(
      this.subgraph,
      new this.sugar.dida.NodeSpec.Map(this.inner, f),
    );

    return new Node<G>(this.sugar, nodeInner, this.subgraph)
  }

  // Each subclass needs to implement this if they want to map into their own type
  map<G extends Array<any>>(f: (input: T) => G): Node<G> {
    return this.mapInner(f)
  }

  // reduce(): this {
  //   return this
  // }

  reduce<V>(keyColumns: number, reduceFn: (v: V, row: T, count: number) => V): Node<T> {
    const nodeInner = this.sugar.builder.addNode(
      this.subgraph,
      new this.sugar.dida.NodeSpec.Reduce(
        this.inner,
        keyColumns,
        reduceFn
      ),
    );

    return new Node<T>(this.sugar, nodeInner, this.subgraph)
  }

  union<O extends Array<any>, G extends Array<any>>(other: Node<O>): Node<G> {
    const nodeInner = this.sugar.builder.addNode(
      this.subgraph,
      new this.sugar.dida.NodeSpec.Union([this.inner, other.inner]),
    );

    return new Node<G>(this.sugar, nodeInner, this.subgraph)
  }

  timestampPop(): Node<T> {
    const nodeInner = this.sugar.builder.addNode(
      this.subgraph,
      new this.sugar.dida.NodeSpec.TimestampPop(this.inner),
    );

    return new Node<T>(this.sugar, nodeInner, this.subgraph)
  }

  output(): OutputNode<T> {
    const nodeInner = this.sugar.builder.addNode(
      this.subgraph,
      new this.sugar.dida.NodeSpec.Output(this.inner),
    );

    return new OutputNode<T>(this.sugar, nodeInner, this.subgraph)
  }
}

export class InputNode<T extends Array<any>> extends Node<T> {
  push(v: T, timestamp: Array<number>, diff: number) {
    this.sugar.shard.pushInput(
      this.inner,
      new this.sugar.dida.Change(v, timestamp, diff)
    )
  }
  flush() {
    this.sugar.shard.flushInput(this.inner)
  }

  advance(timestamp: Array<number>) {
    this.sugar.shard.advanceInput(this.inner, timestamp)
  }
}

export class OutputNode<T extends Array<any>> extends Node<T> {
  readonly inner: NodeInner & { pop: () => T | undefined }

  constructor(sugar: Sugar, inner: Node<T>["inner"], subgraph: Node<T>["subgraph"]) {
    super(sugar, inner, subgraph)
    this.inner = inner as any
  }

  pop(): T | undefined {
    return this.inner.pop()
  }

  *[Symbol.iterator]() {
    while (true) {
      const change_batch = this.sugar.shard.popOutput(this.inner);
      if (change_batch == undefined) break;
      yield change_batch
    }
  }
}

export class JoinedNode<T extends Array<any>> extends Node<T> { }

export class IndexedNode<T extends Array<any>> extends Node<T> {
  map<G extends Array<any>>(f: (input: T) => G): IndexedNode<G> {
    return this.mapInner(f) as IndexedNode<G>
  }

  distinct(): DistinctNode<T> {
    const nodeInner = this.sugar.builder.addNode(
      this.subgraph,
      new this.sugar.dida.NodeSpec.Distinct(this.inner)
    )

    console.log("distinct", nodeInner)
    console.log("sg", this.subgraph.inner)

    return new DistinctNode<T>(this.sugar, nodeInner, this.subgraph)
  }

  join<G extends Array<any>, O extends Array<any>>(
    other: IndexedNode<G>,
    joinFirstNCols: number,
  ): IndexedNode<O> {
    //TODO
    const nodeInner = this.sugar.builder.addNode(
      this.subgraph,
      new this.sugar.dida.NodeSpec.Join([this.inner, other.inner], joinFirstNCols),
    )

    return new IndexedNode<O>(this.sugar, nodeInner, this.subgraph)
  }
}

export class DistinctNode<T extends Array<any>> extends IndexedNode<T> { }
export class ReduceNode<T extends Array<any>> extends IndexedNode<T> { }

export class TimestampPushedNode<T extends Array<any>> extends Node<T> { }
export class TimestampPoppedNode<T extends Array<any>> extends Node<T> { }

export class TimestampIncrementedNode<T extends Array<any>> extends Node<T> {
  // TODO fix type
  fixpoint(future: IndexedNode<any[]>) {
    const builder = this.sugar.builder
    builder.connectLoop(future, this)
  }
}

// type NodeInner = ReturnType<DidaBuilder["addNode"]> & {}
class NodeInner { }

type NodeTag = InstanceType<(Dida["NodeSpec"][keyof Dida["NodeSpec"]])>
class Builder {
  private readonly sugar: Sugar
  private readonly inner: DidaBuilder
  constructor(sugar: Sugar, inner: DidaBuilder) {
    this.sugar = sugar
    this.inner = inner
  }

  addNode(subgraph: Subgraph, spec: NodeTag): NodeInner {
    return this.inner.addNode(subgraph.inner, spec)
  }

  addSubgraph(parentGraph: Subgraph): Subgraph {
    const subgraph = this.inner.addSubgraph(parentGraph.inner);
    return new Subgraph(this.sugar, parentGraph, subgraph)
  }

  finishAndReset(): DidaGraph {
    return this.inner.finishAndReset()
  }

  connectLoop<T extends Array<any>>(a: Node<T>, b: Node<T>) {
    this.inner.connectLoop(a.inner, b.inner)
  }
}

// type HasIndex<T: Node> = T extends IndexedNode ? true : T extends DistinctNode ? T exte