with (import ../../dependencies.nix);

pkgs.mkShell rec {
    buildInputs = [
      pkgs.nodejs
      zig
    ];
    
    NIX_NODEJS=pkgs.nodejs;
}