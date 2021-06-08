rec {
    pkgs = import (builtins.fetchTarball {
        name = "nixos-20.09";
        url = "https://github.com/NixOS/nixpkgs/archive/20.09.tar.gz";
        sha256 = "1wg61h4gndm3vcprdcg7rc4s1v3jkm5xd7lw8r2f67w502y94gcy";
    }) {};
    
    zig = pkgs.stdenv.mkDerivation {
        name = "zig";
        src = fetchTarball (if (pkgs.system == "x86_64-linux") then {
            url = "https://ziglang.org/download/0.8.0/zig-linux-x86_64-0.8.0.tar.xz";
            sha256 = "0bljc69lzjg9k0i84xq3xwqxz2x1zhh67bsxw1h9pzlk99zk9v7a";
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
}