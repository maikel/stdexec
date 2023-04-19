/*
 * Copyright (c) 2023 Maikel Nadolski
 * Copyright (c) 2023 NVIDIA Corporation
 *
 * Licensed under the Apache License Version 2.0 with LLVM Exceptions
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *   https://llvm.org/LICENSE.txt
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "exec/net/ip/address.hpp"

#include "catch2/catch.hpp"

TEST_CASE("Construct any", "[net]") {
  auto any = exec::net::ip::address_v4::any();
  CHECK(any.to_string() == "0.0.0.0");
}

TEST_CASE("Construct loopback", "[net]") {
  auto loopback = exec::net::ip::address_v4::loopback();
  CHECK(loopback.to_string() == "127.0.0.1");
}

TEST_CASE("Construct broadcast", "[net]") {
  auto broadcast = exec::net::ip::address_v4::broadcast();
  CHECK(broadcast.to_string() == "255.255.255.255");
}