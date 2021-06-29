To build:

```
nix-shell
zig build install -Drelease-safe=true
zig build run-codegen
```

To run example:

```
nix-shell
node ../../examples/core.js
```