Dida is a (WIP) library for streaming, incremental, iterative, internally-consistent computation on time-varying collections.

The jargon-free version: You write code that manipulates collections using familiar operations like `map`, `join` and `loop`. You run the code on some input and get some output. Then when the input changes, you get changes to the output, much faster than recomputing the whole thing from scratch. (And the outputs will be [correct](https://scattered-thoughts.net/writing/internal-consistency-in-streaming-systems/)!)

Dida is heavily based on [differential dataflow](https://github.com/TimelyDataflow/differential-dataflow/). Why start a new implementation? The goals are to:

* Be [easier to use and understand](https://scattered-thoughts.net/writing/why-isnt-differential-dataflow-more-popular/). This is naturally very subjective, but some guiding principles
 are:
   * Prefer readability and forkability over modularity and flexibility (and especially preserve jump-to-definition in the source rather than using extension traits for everything).
   * Make it clear where state resides.
   * Provide well-documented default implementations for common tasks (eg writing output to a file).
* Provide interactive graphical debuggers for every component. Many of the complaints I've heard about differential dataflow are about struggling to understand where state is stored, when things happen, how different ways of writing a computation affect performance etc. Debuggers can answer this question directly, but I suspect will also help by teaching useful mental representations.
* Write a short book that uses the debuggers to explain both the theory of differential dataflow and the details of this implementation. Differential dataflow suffers from having the theory spread across myriad papers with limited space and which each describe different versions of the algorithms.
* Better support use as an interpreter backend and for binding to other languages.  
  * Don't rely on specialization for performance, since that requires compilation and also doesn't work well cross-language. This will require rethinking eg how functions are lifted over batches.
  * Support storing data inline in indexes when the size is only known at runtime. (Differential dataflow can support this, but materialize currently stores each row in a separate heap allocation even if the row is all fixed-width types).
  * Support reference-counted values without adding overhead for non-reference-counted values. This is needed for pinning eg javascript objects. (Materialize could reference-count eg strings but would then pay for the row-by-row Drop impl on all columns, not just string columns.)
  * Support being embedded in another event loop. (Differential dataflow can be run a step at a time, but the amount of work per step is not bounded).
  * Support cpu and memory limits. This makes it much easier to safely support live evaluation eg embedding a query repl in a tracing dashboard.