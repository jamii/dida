To build:

```
nix-shell
zig build install -Drelease-safe=true
zig build run-codegen
```

To run example:

```
nix-shell -p python3
cd ../../
python3 -m http.server &
$BROWSER http://localhost:8000/examples/core.html
```