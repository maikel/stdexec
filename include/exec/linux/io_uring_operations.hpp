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
#pragma once

#include "./io_uring_context.hpp"
#include "../io_concepts.hpp"

#include <unistd.h>
#include <fcntl.h>

namespace exec::__io_uring {
  class handle {
   public:
    explicit handle(io_uring_scheduler __sched, int __fd = -1)
      : __sched_{__sched}
      , __fd_{__fd} {
    }

    handle(handle&& __other) noexcept
      : __sched_{__other.__sched_}
      , __fd_{std::exchange(__other.__fd_, -1)} {
    }

    handle& operator=(handle&& __other) noexcept {
      __sched_ = __other.__sched_;
      __fd_ = std::exchange(__other.__fd_, -1);
      return *this;
    }

    int native_handle() const noexcept {
      return __fd_;
    }

    io_uring_scheduler get_scheduler() const noexcept {
      return __sched_;
    }

   private:
    io_uring_scheduler __sched_;
    int __fd_{-1};
  };

  namespace __open {
    struct __operation_data {
      int __dirfd_;
      const char* __path_;
      int __flags_;
      ::mode_t __mode_;
    };

    template <class _ReceiverId>
    struct __operation {
      using _Receiver = stdexec::__t<_ReceiverId>;

      struct __impl : __stoppable_op_base<_Receiver> {
        __operation_data __data_;

        __impl(__operation_data __data, __context& __ctx, _Receiver&& __receiver)
          : __stoppable_op_base<_Receiver>{__ctx, __receiver}
          , __data_{__data} {
        }

        static constexpr std::false_type ready() noexcept {
          return {};
        }

        void submit(::io_uring_sqe& __sqe) const noexcept {
          __sqe = ::io_uring_sqe{};
          __sqe.opcode = IORING_OP_OPENAT;
          __sqe.fd = __data_.__dirfd_;
          __sqe.addr = bit_cast<__u64>(__data_.__path_);
          __sqe.len = __data_.__mode_;
          __sqe.open_flags = __data_.__flags_;
        };

        void complete(const ::io_uring_cqe& __cqe) noexcept {

          if (__cqe.res >= 0) {
            stdexec::set_value(
              (_Receiver&&) this->__receiver_, handle(this->context().get_scheduler(), __cqe.res));
          } else {
            stdexec::set_error(
              (_Receiver&&) this->__receiver_,
              std::make_exception_ptr(std::system_error(-__cqe.res, std::system_category())));
          }
        }
      };

      using __t = __stoppable_task_facade_t<__impl>;
    };

    template <class _Receiver>
    using __operation_t = stdexec::__t<__operation<stdexec::__id<_Receiver>>>;

    struct __sender {
      using completion_signatures = stdexec::completion_signatures<
        stdexec::set_value_t(handle),
        stdexec::set_stopped_t(),
        stdexec::set_error_t(std::exception_ptr)>;

      io_uring_scheduler __sched_;
      __operation_data __data_;

      __sender(io_uring_scheduler __sched, __operation_data __data)
        : __sched_{__sched}
        , __data_{__data} {
      }

      template <
        stdexec::__decays_to<__sender> _Self,
        stdexec::receiver_of<completion_signatures> _Receiver>
      friend __operation_t<_Receiver>
        tag_invoke(stdexec::connect_t, _Self&& __self, _Receiver&& __receiver) noexcept {
        return __operation_t<_Receiver>{
          __self.__data_, *__self.__sched_.__context_, (_Receiver&&) __receiver};
      }
    };
  }

  inline __open::__sender tag_invoke(
    async_open_t,
    io_uring_scheduler __scheduler,
    const char* __path,
    int __flags,
    ::mode_t __mode) {
    return __open::__sender{
      __scheduler, __open::__operation_data{AT_FDCWD, __path, __flags, __mode}
    };
  }

  namespace __close {
    template <class _ReceiverId>
    struct __operation {
      using _Receiver = stdexec::__t<_ReceiverId>;

      struct __impl : __stoppable_op_base<_Receiver> {
        int __fd_;

        explicit __impl(int __fd, __context& __ctx, _Receiver&& __receiver)
          : __stoppable_op_base<_Receiver>(__ctx, __receiver)
          , __fd_{__fd} {
        }

        template <stdexec::__decays_to<__impl> _Self>
        static stdexec::__copy_cvref_t<_Self, _Receiver> receiver(_Self&& __self) noexcept {
          return ((_Self&&) __self).__receiver_;
        }

        static constexpr std::false_type ready() noexcept {
          return {};
        }

        void submit(::io_uring_sqe& __sqe) const noexcept {
          __sqe = ::io_uring_sqe{.opcode = IORING_OP_CLOSE, .fd = __fd_};
        }

        void complete(const ::io_uring_cqe& __cqe) noexcept {
          if (__cqe.res >= 0) {
            stdexec::set_value((_Receiver&&) this->__receiver_);
          } else {
            stdexec::set_error(
              (_Receiver&&) this->__receiver_,
              std::make_exception_ptr(std::system_error(-__cqe.res, std::system_category())));
          }
        }
      };

      using __t = __stoppable_task_facade_t<__impl>;
    };

    template <class _Receiver>
    using __operation_t = stdexec::__t<__operation<stdexec::__id<_Receiver>>>;

    struct __sender {
      using completion_signatures = stdexec::completion_signatures<
        stdexec::set_value_t(),
        stdexec::set_stopped_t(),
        stdexec::set_error_t(std::exception_ptr)>;

      io_uring_scheduler __sched_;
      int __fd_;

      __sender(io_uring_scheduler __sched, int __fd)
        : __sched_{__sched}
        , __fd_{__fd} {
      }

      template <
        stdexec::__decays_to<__sender> _Self,
        stdexec::receiver_of<completion_signatures> _Receiver>
      friend __operation_t<_Receiver>
        tag_invoke(stdexec::connect_t, _Self&& __self, _Receiver&& __receiver) noexcept {
        return __operation_t<_Receiver>{
          __self.__fd_, *__self.__sched_.__context_, (_Receiver&&) __receiver};
      }
    };
  }

  inline __close::__sender tag_invoke(async_close_t, handle __h) noexcept {
    return __close::__sender{__h.get_scheduler(), __h.native_handle()};
  }
}