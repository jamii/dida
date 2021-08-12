TODO:

* do UI from inside zig, spit out html
  * return a giant html string
  * do serde for events
  * put events in the html callbacks
* need to get log data out of crashing instances
  * for now can import dida locally to get a working example
  * later do binary serialization between wasm modules - swizzle into []u8

To build:

```
nix-shell
zig build install -Drelease-safe=true
```

To run example:

```
cd ../
nix-shell -p python3
python3 -m http.server &
$BROWSER http://localhost:8000/debugger/debugger.html
```