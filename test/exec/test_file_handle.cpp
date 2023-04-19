#include "exec/linux/file_handle.hpp"

#include "catch2/catch.hpp"

TEST_CASE("Open a file handle", "[exec][linux][file_handle]") {
  exec::io_uring_context context{};

  auto work = exec::use_resources(
    [](exec::file_handle auto file) {
      REQUIRE(file);
      return exec::async_write(file, "Hello, world!");
    },
    exec::file(exec::get_io_scheduler(context), "/etc/hosts", exec::mode::write));

  auto [nbytes] = sync_wait(when_any(work, context.run())).value();
  CHECK(nbytes == 13);
} 