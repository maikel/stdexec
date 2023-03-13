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

#if __has_include(<linux/io_uring.h>)
#include "exec/linux/io_uring_operations.hpp"

#include <catch2/catch.hpp>

using namespace stdexec;
using namespace exec;

TEST_CASE("Open temporary file", "[io_uring]") {
  io_uring_context __context{};

  auto open_file = async_open(
    __context.get_scheduler(), "/tmp", O_TMPFILE | O_RDWR, S_IRUSR | S_IWUSR);
  __debug_sender(open_file);
  auto [h] = sync_wait(open_file).value();
  CHECK(h.native_handle() > 0);
}

#endif