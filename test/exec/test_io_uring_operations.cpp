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

#include <string_view>

#include <catch2/catch.hpp>

using namespace stdexec;
using namespace exec;

TEST_CASE("Open temporary file", "[io_uring]") {
  io_uring_context __context{};
  auto [h] = __context
               .sync_wait(async_open(
                 __context.get_scheduler(), "/tmp", O_TMPFILE | O_RDWR, S_IRUSR | S_IWUSR))
               .value();
  CHECK(h.native_handle() > 0);
  __context.sync_wait(async_close(h));
}

TEST_CASE("Write and Read temporary file", "[io_uring]") {
  io_uring_context __context{};
  auto [h] = __context
               .sync_wait(async_open(
                 __context.get_scheduler(), "/tmp", O_TMPFILE | O_RDWR, S_IRUSR | S_IWUSR))
               .value();
  REQUIRE(h.native_handle() > 0);
  std::string_view data = "Hello World";
  ::iovec write_iov[] = {
    {const_cast<char*>(data.data()), data.size()}
  };
  auto [size] = __context.sync_wait(async_write_some(h, write_iov)).value();
  REQUIRE(size == data.size());
  char buffer[11]{};
  ::iovec iov[] = {
    {buffer, sizeof(buffer)}
  };
  auto [nbytes] = __context.sync_wait(async_read_some(h, iov, 0)).value();
  CHECK(nbytes == data.size());
  std::string_view result(buffer, nbytes);
  CHECK(result == data);
  __context.sync_wait(async_close(h));
}

#endif