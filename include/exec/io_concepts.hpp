#pragma once

#include "../stdexec/execution.hpp"

namespace exec {
  namespace __async_open {
    using namespace stdexec;

    struct async_open_t {
      template <scheduler _Scheduler, class... Args>
        requires tag_invocable<async_open_t, _Scheduler, Args...>
      auto operator()(_Scheduler&& __sched, Args&&... args) const
        noexcept(nothrow_tag_invocable<async_open_t, _Scheduler, Args...>)
          -> tag_invoke_result_t<async_open_t, _Scheduler, Args...> {
        return tag_invoke(async_open_t{}, (_Scheduler&&) __sched, (Args&&) args...);
      }
    };
  }

  using __async_open::async_open_t;
  inline constexpr async_open_t async_open{};

  namespace __async_close {
    using namespace stdexec;

    struct async_close_t {
      template <class _Resource, class... Args>
        requires tag_invocable<async_close_t, _Resource, Args...>
      auto operator()(_Resource&& __resource, Args&&... args) const
        noexcept(nothrow_tag_invocable<async_close_t, _Resource, Args...>)
          -> tag_invoke_result_t<async_close_t, _Resource, Args...> {
        return tag_invoke(async_close_t{}, (_Resource&&) __resource, (Args&&) args...);
      }
    };
  }

  using __async_close::async_close_t;
  inline constexpr async_close_t async_close{};

  namespace __async_read_some {
    using namespace stdexec;

    struct async_read_some_t {
      template <class _Resource, class... Args>
        requires tag_invocable<async_read_some_t, _Resource, Args...>
      auto operator()(_Resource&& __res, Args&&... args) const
        noexcept(nothrow_tag_invocable<async_read_some_t, _Resource, Args...>)
          -> tag_invoke_result_t<async_read_some_t, _Resource, Args...> {
        return tag_invoke(async_read_some_t{}, (_Resource&&) __res, (Args&&) args...);
      }
    };
  }

  using __async_read_some::async_read_some_t;
  inline constexpr async_read_some_t async_read_some{};

  namespace __async_write_some {
    using namespace stdexec;

    struct async_write_some_t {
      template <class _Resource, class... Args>
        requires tag_invocable<async_write_some_t, _Resource, Args...>
      auto operator()(_Resource&& __res, Args&&... args) const
        noexcept(nothrow_tag_invocable<async_write_some_t, _Resource, Args...>)
          -> tag_invoke_result_t<async_write_some_t, _Resource, Args...> {
        return tag_invoke(async_write_some_t{}, (_Resource&&) __res, (Args&&) args...);
      }
    };
  }

  using __async_write_some::async_write_some_t;
  inline constexpr async_write_some_t async_write_some{};
}