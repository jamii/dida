with (import ../dependencies.nix);

pkgs.mkShell rec {
    buildInputs = [
      zig
    ];
}