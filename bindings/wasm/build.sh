#! /usr/bin/env bash

zig build-lib runtime.zig -femit-bin=dida.wasm -target wasm32-freestanding -dynamic --single-threaded --main-pkg-path ../../
zig run codegen.zig --main-pkg-path ../../ > ./dida.js