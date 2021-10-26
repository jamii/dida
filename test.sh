#! /usr/bin/env bash

set -ue 

echo 'Running unit tests'
nix-shell ./shell.nix --run 'zig test test/core.zig --main-pkg-path ./'

echo 'Checking that node bindings build'
pushd bindings/node
nix-shell ./shell.nix --run 'zig build install && zig build run-codegen'
popd

echo 'Checking that wasm bindings build'
pushd bindings/wasm
nix-shell ./shell.nix --run 'zig build install && zig build run-codegen'
popd

echo 'Checking that debugger builds'
pushd native-debugger
nix-shell ./shell.nix --run 'zig build install'
popd