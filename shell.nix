{ cross ? false }:

let

  pkgs = import (builtins.fetchTarball {
    name = "nixos-20.09";
    url = "https://github.com/NixOS/nixpkgs/archive/20.09.tar.gz";
    sha256 = "1wg61h4gndm3vcprdcg7rc4s1v3jkm5xd7lw8r2f67w502y94gcy";
  }) {};

  zig = pkgs.stdenv.mkDerivation {
    name = "zig";
    src = fetchTarball (if (pkgs.system == "x86_64-linux") then {
        url = "https://ziglang.org/download/0.7.1/zig-linux-x86_64-0.7.1.tar.xz";
        sha256 = "1jpp46y9989kkzavh73yyd4ch50sccqgcn4xzcflm8g96l3azl40";
    } else if (pkgs.system == "aarch64-linux") then {
        url = "https://ziglang.org/download/0.7.1/zig-linux-aarch64-0.7.1.tar.xz";
        sha256 = "02fvph5hvn5mrr847z8zhs35kafhw5pik6jfkx3rimjr65pqpd9v";
    } else throw ("Unknown system " ++ pkgs.system));
    dontConfigure = true;
    dontBuild = true;
    installPhase = ''
      mkdir -p $out
      mv ./lib $out/
      mkdir -p $out/bin
      mv ./zig $out/bin
      mkdir -p $out/doc
      #mv ./langref.html $out/doc
    '';
  };

in

pkgs.mkShell rec {
  buildInputs = [
    zig
  ];
}
