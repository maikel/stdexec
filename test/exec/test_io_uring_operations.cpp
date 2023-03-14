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
#include "exec/finally.hpp"
#include "exec/task.hpp"
#include "exec/scope.hpp"

#include <string_view>

#include <catch2/catch.hpp>

using namespace stdexec;
using namespace exec;

TEST_CASE("Open temporary file", "[io_uring][io_uring_operations]") {
  io_uring_context context{};
  auto [h] =
    sync_wait(
      context, async_open(context.get_scheduler(), "/tmp", O_TMPFILE | O_RDWR, S_IRUSR | S_IWUSR))
      .value();
  CHECK(h.native_handle() > 0);
  sync_wait(context, async_close(h));
}

TEST_CASE("Write and Read temporary file", "[io_uring][io_uring_operations]") {
  io_uring_context context{};
  std::string_view data{"Hello, World!"};
  char buffer[1024];
  auto open_file = async_open(
    context.get_scheduler(), "/tmp", O_TMPFILE | O_RDWR, S_IRUSR | S_IWUSR);
  auto write_buffer = just(std::array<::iovec, 1>{
    ::iovec{.iov_base = const_cast<char*>(data.data()), .iov_len = data.size()}
  });
  auto read_buffer = just(std::array<::iovec, 1>{
    ::iovec{.iov_base = buffer, .iov_len = sizeof(buffer)}
  });
  auto algorithm = when_all(open_file, write_buffer, read_buffer)
                 | let_value([](auto handle, std::span<::iovec> wbuf, std::span<::iovec> rbuf) {
                     return finally(
                       async_write_some(handle, wbuf) //
                         | let_value([handle, rbuf](int written) {
                             return async_read_some(handle, rbuf, 0)
                                  | let_value([written](int read) { return just(written, read); });
                           }),
                       async_close(handle));
                   });
  auto [written, read] = sync_wait(context, algorithm).value();
  CHECK(written == static_cast<int>(data.size()));
  CHECK(read == static_cast<int>(data.size()));
}

namespace {
  task<std::tuple<int, int>> coro_write_and_read(
    io_uring_scheduler sched,
    std::span<::iovec> wbuf,
    std::span<::iovec> rbuf) {
    auto handle = co_await async_open(sched, "/tmp", O_TMPFILE | O_RDWR, S_IRUSR | S_IWUSR);
    co_await at_coroutine_exit(async_close, std::move(handle));
    auto written = co_await async_write_some(handle, std::span(wbuf));
    auto read = co_await async_read_some(handle, std::span(rbuf), 0);
    co_return std::make_tuple(written, read);
  }
} // namespace

TEST_CASE("Coro Write and Read temporary file", "[io_uring][io_uring_operations]") {
  io_uring_context context{};
  std::string_view data{"Hello, World!"};
  char buffer[1024];
  std::array<::iovec, 1> wbuf{
    ::iovec{.iov_base = const_cast<char*>(data.data()), .iov_len = data.size()}
  };
  std::array<::iovec, 1> rbuf{
    ::iovec{.iov_base = buffer, .iov_len = sizeof(buffer)}
  };
  auto [result] =
    sync_wait(context, coro_write_and_read(context.get_scheduler(), wbuf, rbuf)).value();
  auto [written, read] = result;
  CHECK(written == static_cast<int>(data.size()));
  CHECK(read == static_cast<int>(data.size()));
}

#endif