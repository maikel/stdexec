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

#include "../stdexec/execution.hpp"

#include "./timed_scheduler.hpp"
#include "./sequence/async_resource.hpp"

#include <filesystem>

namespace exec { namespace async {
  enum class mode : unsigned char {
    unchanged = 0,
    none = 2,
    attr_read = 4,
    attr_write = 5,
    read = 6,
    write = 7,
    append = 9
  };

  enum class creation : unsigned char {
    open_existing = 0,
    only_if_existing,
    if_needed,
    truncate_existing,
    always_new
  };

  enum class chaching : unsigned char {
    unchanged = 0,
    none = 1,
    only_metadata = 2,
    reads = 3,
    reads_and_metadata = 5,
    all = 6,
    safety_barriers = 7,
    temporary = 8
  };

  // namespace file {
  //   enum class flag : unsigned char {
  //     unlink_on_first_close = 0,
  //     disable_safety_barriers,
  //     disable_safety_unlinks,
  //     disable_prefetching,
  //     maximum_prefetching,
  //     win_disable_sparse_file_creation,
  //     win_create_case_sensitive_directory
  //   };
  // }

  template <class _Handle>
  concept path_handle = stdexec::regular<_Handle> && requires(_Handle __handle) {
    { __handle.path() } -> std::convertible_to<std::filesystem::path>;
  };

  template <class _Res, class _Env = stdexec::empty_env>
  concept path_resource = resource<_Res> && path_handle<resource_token_of_t<_Res, _Env>>;

  template <class _Sender, class _Tp, class _Env = stdexec::no_env>
  concept single_value_sender =                      //
    stdexec::__single_typed_sender<_Sender, _Env> && //
    std::same_as<_Tp, stdexec::__single_sender_value_t<_Sender, _Env>>;

  struct path_t {
    template <class _Factory, class... _Args>
      requires stdexec::tag_invocable<path_t, _Factory, _Args...>
    auto operator()(const _Factory& __factory, _Args&&... __args) const
      noexcept(stdexec::nothrow_tag_invocable<path_t, _Factory, _Args...>)
        -> stdexec::tag_invoke_result_t<path_t, _Factory, _Args...> {
      return tag_invoke(*this, __factory, static_cast<_Args&&>(__args)...);
    }
  };

  template <class _Factory>
  using path_factory = callable<path_t, _Factory, std::filesystem::path>;

  template <path_factory _Factory>
  using path_handle_of_t =
    resource_token_of_t<call_result_t<path_t, _Factory, std::filesystem::path>>;

  namespace __byte_stream {
    struct read_some_t {
      template <class _Handle, class _MutableBufferSequence>
        requires stdexec::tag_invocable<read_some_t, _Handle, _MutableBufferSequence>
      auto operator()(const _Handle& __handle, _MutableBufferSequence&& __buffers) const
        noexcept(stdexec::nothrow_tag_invocable<read_some_t, _Handle, _MutableBufferSequence>)
          -> stdexec::tag_invoke_result_t<read_some_t, _Handle, _MutableBufferSequence> {
        using __result_t = tag_invoke_result_t<read_some_t, _Handle, _MutableBufferSequence>;
        static_assert(
          single_value_sender<__result_t, _MutableBufferSequence>,
          "read must return a sender that completes with the input buffers type");
        return tag_invoke(*this, __handle, static_cast<_MutableBufferSequence&&>(__buffers));
      }
    };

    struct write_some_t {
      template <class _Handle, class _ConstBufferSequence>
        requires stdexec::tag_invocable<write_some_t, _Handle, _ConstBufferSequence>
      auto operator()(const _Handle& __handle, _ConstBufferSequence&& __buffers) const
        noexcept(stdexec::nothrow_tag_invocable<write_some_t, _Handle, _ConstBufferSequence>)
          -> stdexec::tag_invoke_result_t<write_some_t, _Handle, _ConstBufferSequence> {
        using __result_t = tag_invoke_result_t<read_some_t, _Handle, _ConstBufferSequence>;
        static_assert(
          single_value_sender<__result_t, _ConstBufferSequence>,
          "write must return a sender that completes with the input buffers type");
        return tag_invoke(*this, __handle, static_cast<_ConstBufferSequence&&>(__buffers));
      }
    };
  } // namespace __byte_stream

  using __byte_stream::read_some_t;
  using __byte_stream::write_some_t;
  inline constexpr read_some_t read_some;
  inline constexpr write_some_t write_some;

  template <class _Stream>
  concept __with_buffer_typedefs = requires {
    typename _Stream::buffer_type;
    typename _Stream::const_buffer_type;
    typename _Stream::buffers_type;
    typename _Stream::const_buffers_type;
  };

  template <class _Stream>
  concept __with_offset = requires { typename _Stream::offset_type; };

  template <__with_offset _Stream>
  using offset_type_of_t = typename _Stream::offset_type;

  template <__with_buffer_typedefs _Stream>
  using buffer_type_of_t = typename _Stream::buffer_type;

  template <__with_buffer_typedefs _Stream>
  using buffers_type_of_t = typename _Stream::buffers_type;

  template <__with_buffer_typedefs _Stream>
  using const_buffer_type_of_t = typename _Stream::const_buffer_type;

  template <__with_buffer_typedefs _Stream>
  using const_buffers_type_of_t = typename _Stream::const_buffers_type;

  template <class _ByteStream>
  concept readable_byte_stream =
    __with_buffer_typedefs<_ByteStream>
    && requires(_ByteStream __stream, buffers_type_of_t<_ByteStream> __buffers) {
         {
           async::read_some(__stream, __buffers)
         } -> single_value_sender<buffers_type_of_t<_ByteStream>>;
       };

  template <class _ByteStream>
  concept writable_byte_stream =
    __with_buffer_typedefs<_ByteStream>
    && requires(_ByteStream __stream, const_buffers_type_of_t<_ByteStream> __const_buffers) {
         {
           async::write_some(__stream, __const_buffers)
         } -> single_value_sender<const_buffers_type_of_t<_ByteStream>>;
       };

  template <class _ByteStream>
  concept byte_stream =                    //
    __with_buffer_typedefs<_ByteStream> && //
    readable_byte_stream<_ByteStream> &&   //
    writable_byte_stream<_ByteStream>;

  template <class _ByteStream>
  concept seekable_byte_stream =  //
    byte_stream<_ByteStream> &&   //
    __with_offset<_ByteStream> && //
    requires(
      _ByteStream __stream,
      buffers_type_of_t<_ByteStream> __buffers,
      const_buffers_type_of_t<_ByteStream> __const_buffers,
      offset_type_of_t<_ByteStream> __offset) {
      {
        async::read_some(__stream, __buffers, __offset)
      } -> single_value_sender<buffers_type_of_t<_ByteStream>>;
      {
        async::write_some(__stream, __const_buffers, __offset)
      } -> single_value_sender<buffers_type_of_t<_ByteStream>>;
    };

  template <class _FileHandle>
  concept file_handle = path_handle<_FileHandle> && seekable_byte_stream<_FileHandle>;

  template <class _Res, class _Env = stdexec::no_env>
  concept file_resource = resource<_Res> && file_handle<resource_token_of_t<_Res, _Env>>;

  struct file_t { };

  inline constexpr file_t file;

  template <class _Factory>
  concept file_factory =      //
    path_factory<_Factory> && //
    requires(
      _Factory __factory,
      path_handle_of_t<_Factory> __base,
      std::filesystem::path __path,
      mode __mode,
      creation __creation,
      chaching __chaching) {
      { async::file(__factory, __path) } -> file_resource;
      { async::file(__factory, __base, __path) } -> file_resource;
      { async::file(__factory, __base, __path, __mode) } -> file_resource;
      { async::file(__factory, __base, __path, __mode) } -> file_resource;
      { async::file(__factory, __base, __path, __mode, __creation) } -> file_resource;
      { async::file(__factory, __base, __path, __mode, __creation, __chaching) } -> file_resource;
    };

  template <class _Scheduler>
  concept io_scheduler = timed_scheduler<_Scheduler> && file_factory<_Scheduler>;

} // namespace async
} // namespace exec