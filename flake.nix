{
  description = "A very basic flake";
  inputs.flake-utils.url = "github:numtide/flake-utils";

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { system = system; };
        deps = (import ./dependencies.nix { inherit system; });
        buildWasmBindings = pkgs.writeScriptBin "buildWasmBindings" ''
          cd bindings/wasm
          ${deps.zig}/bin/zig build install && ${deps.zig}/bin/zig build run-codegen
        '';

      in
      {
        packages.hello = pkgs.hello;
        defaultPackage = self.packages.${system}.hello;
        devShell =
          pkgs.mkShell rec {
            buildInputs = [
              buildWasmBindings
              deps.zig
              pkgs.nodejs-17_x
              pkgs.deno
            ];
          };
      });
}
