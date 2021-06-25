```
nix-shell
./build.sh
nix-shell --pure -p python3
cd ../../
python3 -m http.server &
$BROWSER http://localhost:8000/examples/core.html
```