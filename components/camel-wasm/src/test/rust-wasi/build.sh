#! /bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euxo pipefail

# rustup target add wasm32-unknown-unknown
#
# 64 KiB of stack and no other data, so the module starts with 2 pages of
# linear memory and the memory limit tests can use small numbers.
export RUSTFLAGS="-C link-arg=-zstack-size=65536"

cargo build --target wasm32-unknown-unknown --release --features command
cp target/wasm32-unknown-unknown/release/wasi_guest.wasm ../resources/wasi_guest.wasm

cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/wasi_guest.wasm ../resources/limits_guest.wasm
