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

#include "exec/net/ip/resolve.hpp"

#include "exec/sequence/first_value.hpp"
#include "exec/sequence/then_each.hpp"
#include "exec/net/ip/tcp.hpp"
#include "exec/inline_scheduler.hpp"

#include "catch2/catch.hpp"

#include <iostream>

TEST_CASE("async::resolve is a sequence sender", "[net][resolve]") {
  using namespace exec;
  inline_scheduler sched{};
  auto r = async::resolve(sched, "localhost", "");
  auto [result] = stdexec::sync_wait(first_value(r)).value();
  CHECK(result.host_name() == "localhost");
  CHECK(result.endpoint().address().to_string() == "127.0.0.1");
}