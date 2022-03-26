{
  description = "A very basic flake";
  inputs.nixpkgs.url = "github:nixos/nixpkgs/release-21.11";
  inputs.nixpkgs-unstable.url = "github:nixos/nixpkgs/nixpkgs-unstable";
  inputs.flake-utils.url = "github:numtide/flake-utils";
  # inputs.zls = {
  #   url = "github:zigtools/zls?submodules=1";
  #   flake = false;
  #   fetchSubmodules = true;
  # };

  outputs = { self, nixpkgs, nixpkgs-unstable, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { system = system; };
        pkgs-unstable = import nixpkgs-unstable { system = system; };
        deps = (import ./dependencies.nix { inherit system; });

      in
      {
        packages.hello = pkgs.hello;
        # packages.zls = (import zls {inherit system pkgs;});
        packages.zls = pkgs.stdenvNoCC.mkDerivation {
          name = "zls";
          version = "master";
          src = pkgs.fetchFromGitHub {
            owner = "zigtools";
            repo = "zls";
            # rev = "4e6564d7daec95b4fb51cadfe25973a87cac181a";
            rev = "0.9.0";
            fetchSubmodules = true;
            sha256 = "sha256-MVo21qNCZop/HXBqrPcosGbRY+W69KNCc1DfnH47GsI=";
          };
          nativeBuildInputs = [ deps.zig ];
          dontConfigure = true;
          dontInstall = true;
          buildPhase = ''
                        mkdir -p $out
                        ls -lha .
                        pwd
            echo "!!!"
                        zig build install -Drelease-safe=true -Ddata_version=master --prefix $out
          '';
          XDG_CACHE_HOME = ".cache";
        };
        defaultPackage = self.packages.${system}.hello;
        packages.buildWasmBindings = pkgs.writeScriptBin "buildWasmBindings" ''
          cd $(git rev-parse --show-toplevel)
          cd bindings/wasm
          ${deps.zig}/bin/zig build install && ${deps.zig}/bin/zig build run-codegen
          cd zig-out/lib
           ${self.packages.${system}.tsc}/bin/tsc 'dida.mjs' --declaration --allowJs --emitDeclarationOnly --outDir types
        '';
        #     packages.tsc =     pkgs.lib.mkDerivation{
        #   name = "typescript";
        #   packageName = "typescript";
        #   version = "4.4.4";
        #   src = fetchurl {
        #     url = "https://registry.npmjs.org/typescript/-/typescript-4.4.4.tgz";
        #     sha512 = "DqGhF5IKoBl8WNf8C1gu8q0xZSInh9j1kJJMqT3a94w1JzVaBU4EXOSMrz9yDqMT0xt3selp83fuFMQ0uzv6qA==";
        #   };
        # };
        packages.tsc = (import ./tsc { inherit system pkgs; }).nodeDependencies;
        devShell =
          pkgs.mkShell rec {
            buildInputs = [
              self.packages.${system}.buildWasmBindings
              deps.zig
              pkgs.nodejs-17_x
              pkgs-unstable.deno
              self.packages.${system}.zls
              self.packages.${system}.tsc
            ];
          };
      });
}
