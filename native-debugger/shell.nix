{ cross ? false }:

let

  nixpkgs = <nixpkgs>;

  hostPkgs = import nixpkgs {};

  armPkgs = import nixpkgs {
    system = "aarch64-linux";
  };

  crossPkgs = import nixpkgs {
    overlays = [(self: super: {
      inherit (armPkgs)
        gcc
        mesa
        libGL
        SDL2
        SDL_ttf
      ;
    })];
    crossSystem = hostPkgs.lib.systems.examples.aarch64-multiplatform;
  };

  targetPkgs = if cross then crossPkgs else hostPkgs;

  zig = hostPkgs.stdenv.mkDerivation {
    name = "zig";
    src = fetchTarball (if (hostPkgs.system == "x86_64-linux") then {
        url = "https://ziglang.org/builds/zig-linux-x86_64-0.9.0-dev.687+7aaea20e7.tar.xz";
        sha256 = "1s64pkv6jf70gw58lvsizv1wxcp2055cb540l4xpbgcqc749g6bl";
    } else throw ("Unknown system " ++ hostPkgs.system));
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

hostPkgs.mkShell rec {
  buildInputs = [
    zig
    hostPkgs.pkg-config
    hostPkgs.patchelf
    targetPkgs.libGL.all
    targetPkgs.xorg.libX11.dev
    targetPkgs.xorg.libXrandr.all
    targetPkgs.xlibs.xorgproto
    targetPkgs.SDL2.all
    targetPkgs.SDL2_ttf.all
    targetPkgs.pcre2.all
    targetPkgs.glew.all
    targetPkgs.xorg.libXcursor
    targetPkgs.xorg.libXinerama
    targetPkgs.xorg.xinput
    targetPkgs.xlibs.libXi.all
    targetPkgs.xlibs.libXext.all
  ];
  FOCUS="nixos@192.168.1.83";
  NIX_GCC=targetPkgs.gcc;
  NIX_LIBGL_DEV=targetPkgs.libGL.dev;
  NIX_LIBX11_DEV=targetPkgs.xorg.libX11.dev;
  NIX_XORGPROTO_DEV=targetPkgs.xlibs.xorgproto;
  NIX_SDL2_DEV=targetPkgs.SDL2.dev;
  NIX_SDL2_TTF_DEV=targetPkgs.SDL2_ttf; # no .dev
  # TODO with SDL_VIDEODRIVER=wayland, SDL doesn't seem to respect xkb settings eg caps vs ctrl
  # but without, sometimes causes https://github.com/swaywm/sway/issues/5227
  # SDL_VIDEODRIVER="wayland";
  NIX_PCRE2_DEV = targetPkgs.pcre2.dev;
}
