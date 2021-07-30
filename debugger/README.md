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