# This file has been generated by node2nix 1.9.0. Do not edit!

{nodeEnv, fetchurl, fetchgit, nix-gitignore, stdenv, lib, globalBuildInputs ? []}:

let
  sources = {
    "typescript-4.7.0-dev.20220325" = {
      name = "typescript";
      packageName = "typescript";
      version = "4.7.0-dev.20220325";
      src = fetchurl {
        url = "https://registry.npmjs.org/typescript/-/typescript-4.7.0-dev.20220325.tgz";
        sha512 = "3S7MAr3P2vkrpptkhoEuiATOU1kPa9+3FUrJAAYZ4/6mnI1SZWeVPeBIbhzIOUij27QrRMgSwN8aHrqtgGVqLA==";
      };
    };
  };
  args = {
    name = "unused";
    packageName = "unused";
    version = "1.0.0";
    src = ./.;
    dependencies = [
      sources."typescript-4.7.0-dev.20220325"
    ];
    buildInputs = globalBuildInputs;
    meta = {
      description = "";
      license = "ISC";
    };
    production = true;
    bypassCache = true;
    reconstructLock = false;
  };
in
{
  args = args;
  sources = sources;
  tarball = nodeEnv.buildNodeSourceDist args;
  package = nodeEnv.buildNodePackage args;
  shell = nodeEnv.buildNodeShell args;
  nodeDependencies = nodeEnv.buildNodeDependencies (lib.overrideExisting args {
    src = stdenv.mkDerivation {
      name = args.name + "-package-json";
      src = nix-gitignore.gitignoreSourcePure [
        "*"
        "!package.json"
        "!package-lock.json"
      ] args.src;
      dontBuild = true;
      installPhase = "mkdir -p $out; cp -r ./* $out;";
    };
  });
}
