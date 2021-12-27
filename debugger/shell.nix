with (import ../dependencies.nix);

pkgs.mkShell rec {
  buildInputs = [
    zig
    pkgs.pkg-config
    pkgs.libGL.all
    pkgs.xorg.libX11.dev
    pkgs.xorg.libXrandr.all
    pkgs.xorg.libXcursor
    pkgs.xorg.libXinerama
    pkgs.xorg.xinput
    pkgs.xlibs.xorgproto
    pkgs.xlibs.libXi.all
    pkgs.xlibs.libXext.all
    pkgs.glew.all
  ];
}
