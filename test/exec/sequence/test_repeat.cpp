/*
 * Copyright (c) 2023 NVIDIA Corporation
 * Copyright (c) 2023 Maikel Nadolski
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

#include "exec/sequence/repeat.hpp"

#include <catch2/catch.hpp>

using namespace stdexec;
using namespace exec;

struct seq_receiver {
  using is_receiver = void;
  in_place_stop_source& source;
  int& count;

  friend auto tag_invoke(get_env_t, const seq_receiver& rcvr) noexcept {
    return __make_env(empty_env{}, __with_(get_stop_token, rcvr.source.get_token()));
  }

  friend auto tag_invoke(set_next_t, seq_receiver& rcvr, sender auto item) noexcept {
    return then(
             std::move(item),
             [&rcvr](int value) noexcept {
               rcvr.count += 1;
               if (rcvr.count >= 1000) {
                 rcvr.source.request_stop();
               }
             }) //
         | upon_error([&rcvr](auto&&...) noexcept { rcvr.source.request_stop(); });
  }

  friend void tag_invoke(set_value_t, seq_receiver&&) noexcept {
    CHECK(false);
  }

  friend void tag_invoke(set_stopped_t, seq_receiver&&) noexcept {
    CHECK(true);
  }

  friend void tag_invoke(set_error_t, seq_receiver&&, std::exception_ptr e) noexcept {
    std::rethrow_exception(e);
  }
};

TEST_CASE("repeat - is a sequence", "[sequence_senders][repeat]") {
  auto repeat = exec::repeat(stdexec::just(42));
  using Repeat = decltype(repeat);
  STATIC_REQUIRE(exec::sequence_sender_in<Repeat, stdexec::empty_env>);
  STATIC_REQUIRE(sequence_sender_to<Repeat, seq_receiver>);
  in_place_stop_source source{};
  int count = 0;
  auto op = sequence_connect(repeat, seq_receiver{source, count});
  stdexec::start(op);
  CHECK(count == 1000);
}

TEST_CASE("repeat - nested is a sequence", "[sequence_senders][repeat]") {
  auto nested_repeat = exec::repeat(exec::repeat(stdexec::just(42)));
  using Repeat = decltype(nested_repeat);
  STATIC_REQUIRE(exec::sequence_sender_in<Repeat, stdexec::empty_env>);
  STATIC_REQUIRE(sequence_sender_to<Repeat, seq_receiver>);
  in_place_stop_source source{};
  int count = 0;
  auto op = sequence_connect(nested_repeat, seq_receiver{source, count});
  stdexec::start(op);
  CHECK(count == 1000);
}