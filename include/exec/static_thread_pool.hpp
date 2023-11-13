/*
 * Copyright (c) 2021-2022 Facebook, Inc. and its affiliates.
 * Copyright (c) 2021-2022 NVIDIA Corporation
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
#pragma once

#include "../stdexec/execution.hpp"
#include "../stdexec/__detail/__config.hpp"
#include "../stdexec/__detail/__intrusive_queue.hpp"
#include "../stdexec/__detail/__meta.hpp"

#include "./__detail/__atomic_intrusive_queue.hpp"
#include "./__detail/__bwos_lifo_queue.hpp"
#include "./__detail/__manual_lifetime.hpp"
#include "./__detail/__xorshift.hpp"
#include "./scope.hpp"

#include "./sequence_senders.hpp"
#include "./sequence/iterate.hpp"

#include <atomic>
#include <condition_variable>
#include <exception>
#include <latch>
#include <mutex>
#include <thread>
#include <type_traits>
#include <vector>

namespace exec {
  using stdexec::__intrusive_queue;

  template <class Allocator>
  class basic_static_thread_pool;

  using static_thread_pool = basic_static_thread_pool<std::allocator<std::byte>>;

  namespace __pool {
    inline thread_local const std::thread::id this_thread_id = std::this_thread::get_id();
  }

  // Splits `n` into `size` chunks distributing `n % size` evenly between ranks.
  // Returns `[begin, end)` range in `n` for a given `rank`.
  // Example:
  // ```cpp
  // //         n_items  thread  n_threads
  // even_share(     11,      0,         3); // -> [0,  4) -> 4 items
  // even_share(     11,      1,         3); // -> [4,  8) -> 4 items
  // even_share(     11,      2,         3); // -> [8, 11) -> 3 items
  // ```
  template <class Shape>
  std::pair<Shape, Shape> even_share(Shape n, std::uint32_t rank, std::uint32_t size) noexcept {
    const auto avg_per_thread = n / size;
    const auto n_big_share = avg_per_thread + 1;
    const auto big_shares = n % size;
    const auto is_big_share = rank < big_shares;
    const auto begin = is_big_share
                       ? n_big_share * rank
                       : n_big_share * big_shares + (rank - big_shares) * avg_per_thread;
    const auto end = begin + (is_big_share ? n_big_share : avg_per_thread);

    return std::make_pair(begin, end);
  }

#if STDEXEC_HAS_STD_RANGES()
  namespace schedule_all_ {
    template <class Pool, class Range>
    struct sequence {
      class __t;
    };
  }
#endif

  struct task_base {
    task_base* next;
    void (*__execute)(task_base*, std::uint32_t tid) noexcept;
  };

  struct bwos_params {
    std::size_t numBlocks{8};
    std::size_t blockSize{1024};
  };

  template <class Pool>
  class remote_queue;

  template <class Pool>
  class remote_queue_list;

  template <class Pool>
  class remote_queue_ref {
   public:
    explicit remote_queue_ref(remote_queue<Pool>& queue) noexcept
      : queue_(&queue) {
    }

    void enqueue(task_base* task) const noexcept {
      queue_->pool_->enqueue(*queue_, task);
    }

    void enqueue(task_base* task, std::uint32_t tid) const noexcept {
      queue_->pool_->enqueue(*queue_, task, tid);
    }

    void bulk_enqueue(
      __intrusive_queue<&task_base::next>&& tasks,
      std::size_t tasks_size) const noexcept {
      queue_->pool_->bulk_enqueue(*queue_, std::move(tasks), tasks_size);
    }

    Pool& pool() const noexcept {
      return *queue_->pool_;
    }

    friend bool operator==(const remote_queue_ref&, const remote_queue_ref&) noexcept = default;

   private:
    remote_queue<Pool>* queue_;
  };

  template <class Pool>
  class alignas(64) remote_queue {
   public:
    explicit remote_queue(Pool& pool) noexcept
      : pool_(&pool)
      , queues_(pool.available_parallelism()) {
    }

    explicit remote_queue(remote_queue* next, Pool& pool) noexcept
      : next_(next)
      , pool_(&pool)
      , queues_(pool.available_parallelism()) {
    }

    std::thread::id id() const noexcept {
      return id_;
    }

    std::size_t index() const noexcept {
      return index_;
    }

    __atomic_intrusive_queue<&task_base::next>& operator[](std::size_t index) noexcept {
      return queues_[index];
    }

   private:
    friend class remote_queue_ref<Pool>;
    friend class remote_queue_list<Pool>;

    remote_queue* next_{};
    Pool* pool_;
    std::vector<__atomic_intrusive_queue<&task_base::next>> queues_{};
    std::thread::id id_{std::this_thread::get_id()};
    // This marks whether the submitter is a thread in the pool or not.
    std::size_t index_{std::numeric_limits<std::size_t>::max()};
  };

  template <class Pool>
  class remote_queue_list {
   public:
    using allocator_type = typename std::allocator_traits<
      typename Pool::allocator_type>::template rebind_alloc<remote_queue<Pool>>;

    explicit remote_queue_list(Pool& pool) noexcept
      : head_{nullptr}
      , pool_(pool) {
    }

    ~remote_queue_list() noexcept;

    // @brief Returns an instrusive queue for the specified thread index.
    __intrusive_queue<&task_base::next> pop_all_reversed(std::size_t tid) noexcept;

    remote_queue<Pool>* get();

   private:
    std::atomic<remote_queue<Pool>*> head_;
    Pool& pool_;
  };

  template <class Allocator>
  class basic_static_thread_pool {
    template <class ReceiverId, class... Condition>
    class operation;

    friend class remote_queue_ref<basic_static_thread_pool>;

    friend class remote_queue_list<basic_static_thread_pool>;

    struct schedule_tag {
      // TODO: code to reconstitute a static_thread_pool schedule sender
    };

    template <class SenderId, std::integral Shape, class FunId>
    struct bulk_sender;

    template <stdexec::sender Sender, std::integral Shape, class Fun>
    using bulk_sender_t = //
      bulk_sender<
        stdexec::__x<stdexec::__decay_t<Sender>>,
        Shape,
        stdexec::__x<stdexec::__decay_t<Fun>>>;

#if STDEXEC_MSVC()
    // MSVCBUG https://developercommunity.visualstudio.com/t/Alias-template-with-pack-expansion-in-no/10437850

    template <class... Args>
    struct __bulk_non_throwing {
      using __t = stdexec::__decayed_tuple<Args...>;
      static constexpr bool __v = noexcept(__t(std::declval<Args>()...));
    };
#endif

    template <class Fun, class Shape, class... Args>
      requires stdexec::__callable<Fun, Shape, Args&...>
    using bulk_non_throwing = //
      stdexec::__mbool<
        // If function invocation doesn't throw
        stdexec::__nothrow_callable<Fun, Shape, Args&...> &&
    // and emplacing a tuple doesn't throw
#if STDEXEC_MSVC()
        __bulk_non_throwing<Args...>::__v
#else
        noexcept(stdexec::__decayed_tuple<Args...>(std::declval<Args>()...))
#endif
        // there's no need to advertise completion with `exception_ptr`
        >;

    template <class SenderId, class ReceiverId, class Shape, class Fun, bool MayThrow>
    struct bulk_shared_state;

    template <class SenderId, class ReceiverId, class Shape, class Fn, bool MayThrow>
    struct bulk_receiver;

    template <class SenderId, class ReceiverId, std::integral Shape, class Fun>
    struct bulk_op_state;

    struct transform_bulk {
      template <class Data, class Sender>
      auto operator()(stdexec::bulk_t, Data&& data, Sender&& sndr) {
        auto [shape, fun] = (Data&&) data;
        return bulk_sender_t<Sender, decltype(shape), decltype(fun)>{
          pool_, (Sender&&) sndr, shape, std::move(fun)};
      }

      basic_static_thread_pool& pool_;
    };

#if STDEXEC_HAS_STD_RANGES()
    struct transform_iterate {
      template <class Range>
      stdexec::__t<schedule_all_::sequence<basic_static_thread_pool, Range>>
        operator()(exec::iterate_t, Range&& range) {
        return {static_cast<Range&&>(range), pool_};
      }

      basic_static_thread_pool& pool_;
    };
#endif

    struct domain {
      // For eager customization
      template <stdexec::sender_expr_for<stdexec::bulk_t> Sender>
      auto transform_sender(Sender&& sndr) const noexcept {
        auto sched = stdexec::get_completion_scheduler<stdexec::set_value_t>(
          stdexec::get_env(sndr));
        return stdexec::__sexpr_apply((Sender&&) sndr, transform_bulk{sched.pool()});
      }

      // transform the generic bulk sender into a parallel thread-pool bulk sender
      template <stdexec::sender_expr_for<stdexec::bulk_t> Sender, class Env>
        requires stdexec::__callable<stdexec::get_scheduler_t, Env>
      auto transform_sender(Sender&& sndr, const Env& env) const noexcept {
        auto sched = stdexec::get_scheduler(env);
        return stdexec::__sexpr_apply((Sender&&) sndr, transform_bulk{sched.pool()});
      }

#if STDEXEC_HAS_STD_RANGES()
      template <stdexec::sender_expr_for<exec::iterate_t> Sender>
      auto transform_sender(Sender&& sndr) const noexcept {
        auto sched = stdexec::get_completion_scheduler<stdexec::set_value_t>(
          stdexec::get_env(sndr));
        return stdexec::__sexpr_apply((Sender&&) sndr, transform_iterate{sched.pool()});
      }

      template <stdexec::sender_expr_for<exec::iterate_t> Sender, class Env>
        requires stdexec::__callable<stdexec::get_scheduler_t, Env>
      auto transform_sender(Sender&& sndr, const Env& env) const noexcept {
        auto sched = stdexec::get_scheduler(env);
        return stdexec::__sexpr_apply((Sender&&) sndr, transform_iterate{sched.pool()});
      }
#endif
    };

   public:
    using allocator_type = Allocator;

    basic_static_thread_pool();
    basic_static_thread_pool(std::uint32_t threadCount, bwos_params params = {});
    ~basic_static_thread_pool();

    template <class... Condition>
    struct scheduler {
      using __t = scheduler;
      using __id = scheduler;
      bool operator==(const scheduler&) const = default;

     private:
      template <class ReceiverId, class... OpCondition>
      friend class operation;

      class sender {
       public:
        using __t = sender;
        using __id = sender;
        using is_sender = void;
        using completion_signatures =
          stdexec::completion_signatures< stdexec::set_value_t(), stdexec::set_stopped_t()>;
       private:
        template <typename Receiver>
        auto make_operation_(Receiver r) const -> operation<stdexec::__id<Receiver>, Condition...> {
          return operation<stdexec::__id<Receiver>, Condition...>{queue_ref_, condition_, (Receiver&&) r};
        }

        template <stdexec::receiver Receiver>
        friend auto tag_invoke(stdexec::connect_t, sender s, Receiver r)
          -> operation<stdexec::__id<Receiver>, Condition...> {
          return s.make_operation_((Receiver&&) r);
        }

        struct env {
          remote_queue_ref<basic_static_thread_pool> queue_ref_;
          std::tuple<Condition...> condition_;

          template <class CPO>
          friend scheduler
            tag_invoke(stdexec::get_completion_scheduler_t<CPO>, const env& self) noexcept {
            return self.make_scheduler_();
          }

          scheduler make_scheduler_() const {
            return basic_static_thread_pool::scheduler{queue_ref_};
          }
        };

        friend env tag_invoke(stdexec::get_env_t, const sender& self) noexcept {
          return env{self.queue_ref_, self.condition_};
        }

        friend struct scheduler;

        explicit sender(remote_queue_ref<basic_static_thread_pool> queue_ref, std::tuple<Condition...> condition) noexcept
          : queue_ref_{queue_ref}
          , condition_{condition} {
        }

        remote_queue_ref<basic_static_thread_pool> queue_ref_;
        std::tuple<Condition...> condition_;
      };

      sender make_sender_() const {
        return sender{queue_ref_, condition_};
      }

      friend sender tag_invoke(stdexec::schedule_t, const scheduler& s) noexcept {
        return s.make_sender_();
      }

      friend stdexec::forward_progress_guarantee
        tag_invoke(stdexec::get_forward_progress_guarantee_t, scheduler) noexcept {
        return stdexec::forward_progress_guarantee::parallel;
      }

      friend domain tag_invoke(stdexec::get_domain_t, scheduler) noexcept {
        return {};
      }

      friend class basic_static_thread_pool;

      explicit scheduler(remote_queue_ref<basic_static_thread_pool> queue_ref, Condition... condition) noexcept
        : queue_ref_{queue_ref}
        , condition_{condition...} {
      }

      basic_static_thread_pool& pool() const noexcept {
        return queue_ref_.pool();
      }

      remote_queue_ref<basic_static_thread_pool> queue_ref_;
      std::tuple<Condition...> condition_;
    };

    scheduler<> get_scheduler() noexcept {
      return scheduler<>{get_remote_queue()};
    }

    scheduler<std::uint32_t> get_scheduler_for_worder_id(std::uint32_t tid) noexcept {
      return scheduler<std::uint32_t>{get_remote_queue(), tid};
    }

    remote_queue_ref<basic_static_thread_pool> get_remote_queue() noexcept {
      return remote_queue_ref<basic_static_thread_pool>{*remotes_.get()};
    }

    void request_stop() noexcept;

    std::uint32_t available_parallelism() const {
      return threadCount_;
    }

    bwos_params params() const {
      return params_;
    }

    allocator_type get_allocator() const noexcept {
      return allocator_;
    }

   private:
    template <class Tp>
    using allocator_t = std::allocator_traits<Allocator>::template rebind_alloc<Tp>;

    template <class Tp>
    using vector_t = std::vector<Tp, allocator_t<Tp>>;

    using remote_queue_t = remote_queue<basic_static_thread_pool>;

    void enqueue(remote_queue_t& queue, task_base* task) noexcept;

    void enqueue(remote_queue_t& queue, task_base* task, std::uint32_t tid) noexcept;

    template <std::derived_from<task_base> TaskT>
    void bulk_enqueue(TaskT* task, std::uint32_t n_threads) noexcept;

    void bulk_enqueue(
      remote_queue_t& queue,
      __intrusive_queue<&task_base::next> tasks,
      std::size_t tasks_size) noexcept;

    class alignas(64) thread_state {
      class victim {
       public:
        explicit victim(bwos::lifo_queue<task_base*>* queue, std::uint32_t index) noexcept
          : queue_(queue)
          , index_(index) {
        }

        task_base* try_steal() noexcept {
          return queue_->steal_front();
        }

        std::uint32_t index() const noexcept {
          return index_;
        }

       private:
        bwos::lifo_queue<task_base*>* queue_;
        std::uint32_t index_;
      };

     public:
      using local_queue_type = bwos::lifo_queue<task_base*, allocator_t<task_base*>>;

      struct pop_result {
        task_base* task;
        std::uint32_t queueIndex;
      };

      explicit thread_state(
        static_thread_pool& pool,
        std::uint32_t index,
        bwos_params params) noexcept
        : local_queue_(
          params.numBlocks,
          params.blockSize,
          allocator_t<task_base*>{pool.get_allocator()})
        , state_(state::running)
        , pool_(pool)
        , index_(index) {
        std::random_device rd;
        rng_.seed(rd);
      }

      pop_result pop();
      void push_local(task_base* task);
      void push_local(__intrusive_queue<&task_base::next>&& tasks);

      bool notify();
      void request_stop();

      std::uint32_t index() const noexcept {
        return index_;
      }

      void set_victims();

     private:
      enum state {
        running,
        sleeping,
        notified
      };

      pop_result try_pop();
      pop_result try_remote();
      pop_result try_steal_from_victims();

      void notify_one_sleeping();
      void set_stealing();
      void clear_stealing() noexcept;
      pop_result explore_other_thread_states();

      local_queue_type local_queue_;
      __intrusive_queue<&task_base::next> pending_queue_{};
      std::mutex mut_{};
      std::condition_variable cv_{};
      vector_t<victim> victims_{};
      std::atomic<state> state_;
      static_thread_pool& pool_;
      xorshift rng_{};
      std::uint32_t index_;
      bool stopRequested_{false};
    };

    void run(std::uint32_t index) noexcept;
    void join() noexcept;

    alignas(64) std::atomic<std::uint32_t> numThiefs_{};
    alignas(64) remote_queue_list<basic_static_thread_pool> remotes_;
    std::uint32_t threadCount_;
    std::uint32_t maxSteals_{(threadCount_ + 1) << 1};
    bwos_params params_;
    vector_t<std::thread> threads_;
    std::latch start_latch_{threadCount_ + 1};
    std::latch stop_latch_{threadCount_ + 1};
    vector_t<thread_state*> threadStates_;
    STDEXEC_ATTRIBUTE((no_unique_address)) Allocator allocator_;
  };

  template <class Pool>
  remote_queue_list<Pool>::~remote_queue_list() noexcept {
    remote_queue<Pool>* head = head_.exchange(nullptr, std::memory_order_acq_rel);
    allocator_type allocator{pool_.get_allocator()};
    while (head != nullptr) {
      remote_queue<Pool>* tmp = std::exchange(head, head->next_);
      std::allocator_traits<allocator_type>::destroy(allocator, tmp);
      std::allocator_traits<allocator_type>::deallocate(allocator, tmp, 1);
    }
  }

  // @brief Returns an instrusive queue for the specified thread index.
  template <class Pool>
  __intrusive_queue<&task_base::next>
    remote_queue_list<Pool>::pop_all_reversed(std::size_t tid) noexcept {
    remote_queue<Pool>* head = head_.load(std::memory_order_acquire);
    __intrusive_queue<&task_base::next> tasks{};
    while (head != nullptr) {
      tasks.append(head->queues_[tid].pop_all_reversed());
      head = head->next_;
    }
    return tasks;
  }

  template <class Pool>
  remote_queue<Pool>* remote_queue_list<Pool>::get() {
    remote_queue<Pool>* head = head_.load(std::memory_order_acquire);
    remote_queue<Pool>* queue = head;
    while (queue != nullptr) {
      if (queue->id_ == __pool::this_thread_id) {
        return queue;
      }
      queue = queue->next_;
    }
    allocator_type allocator{pool_.get_allocator()};
    remote_queue<Pool>* new_head = std::allocator_traits<allocator_type>::allocate(allocator, 1);
    try {
      std::allocator_traits<allocator_type>::construct(allocator, new_head, head, pool_);
    } catch (...) {
      std::allocator_traits<allocator_type>::deallocate(allocator, new_head, 1);
      throw;
    }
    while (!head_.compare_exchange_weak(head, new_head, std::memory_order_acq_rel)) {
      new_head->next_ = head;
    }
    for (std::size_t i = 0; i < pool_.available_parallelism(); ++i) {
      if (pool_.threads_[i].get_id() == __pool::this_thread_id) {
        new_head->index_ = i;
        break;
      }
    }
    return new_head;
  }

  template <class Allocator>
  basic_static_thread_pool<Allocator>::basic_static_thread_pool()
    : basic_static_thread_pool(std::thread::hardware_concurrency()) {
  }

  template <class Allocator>
  basic_static_thread_pool<Allocator>::basic_static_thread_pool(
    std::uint32_t threadCount,
    bwos_params params)
    : remotes_(*this)
    , threadCount_(threadCount)
    , params_(params)
    , threadStates_(threadCount) {
    STDEXEC_ASSERT(threadCount > 0);

    threads_.reserve(threadCount);
    try {
      for (std::uint32_t i = 0; i < threadCount; ++i) {
        threads_.emplace_back([this, i] { run(i); });
      }
      start_latch_.arrive_and_wait();
    } catch (...) {
      request_stop();
      join();
      throw;
    }
  }

  template <class Allocator>
  basic_static_thread_pool<Allocator>::~basic_static_thread_pool() {
    request_stop();
    stop_latch_.arrive_and_wait();
    join();
    allocator_t<thread_state> allocator{allocator_};
    for (thread_state* state: threadStates_) {
      if (state) {
        std::allocator_traits<allocator_t<thread_state>>::destroy(allocator, state);
        std::allocator_traits<allocator_t<thread_state>>::deallocate(allocator, state, 1);
      }
    }
  }

  template <class Allocator>
  void basic_static_thread_pool<Allocator>::request_stop() noexcept {
    for (auto state: threadStates_) {
      state->request_stop();
    }
  }

  template <class Allocator>
  void basic_static_thread_pool<Allocator>::run(std::uint32_t threadIndex) noexcept {
    // TODO bind this thread to a specific numa noda if possible
    // Memory allocations happen on the numa node of the thread.
    allocator_t<thread_state> allocator{allocator_};
    thread_state* local_state = std::allocator_traits<allocator_t<thread_state>>::allocate(
      allocator, 1);
    try {
      std::allocator_traits<allocator_t<thread_state>>::construct(
        allocator, local_state, *this, threadIndex, params_);
    } catch (...) {
      std::allocator_traits<allocator_t<thread_state>>::deallocate(allocator, local_state, 1);
      throw;
    }
    STDEXEC_ASSERT(threadIndex < threadCount_);
    threadStates_[threadIndex] = local_state;
    start_latch_.arrive_and_wait();
    scope_guard stop_latch_guard{[this]() noexcept {
      stop_latch_.arrive_and_wait();
    }};
    local_state->set_victims();
    while (true) {
      // Make a blocking call to de-queue a task if we don't already have one.
      auto [task, queueIndex] = local_state->pop();
      if (!task) {
        return; // pop() only returns null when request_stop() was called.
      }
      task->__execute(task, queueIndex);
    }
  }

  template <class Allocator>
  void basic_static_thread_pool<Allocator>::join() noexcept {
    for (auto& t: threads_) {
      t.join();
    }
    threads_.clear();
  }

  template <class Allocator>
  void
    basic_static_thread_pool<Allocator>::enqueue(remote_queue_t& queue, task_base* task) noexcept {
    remote_queue_t* correct_queue = __pool::this_thread_id == queue.id() ? &queue : remotes_.get();
    std::size_t idx = correct_queue->index();
    if (idx < threadStates_.size()) {
      threadStates_[idx]->push_local(task);
    } else {
      thread_local std::uint32_t nextThreadIndex = std::random_device{}();
      const std::size_t threadIndex = nextThreadIndex++ % threadCount_;
      (*correct_queue)[threadIndex].push_front(task);
      threadStates_[threadIndex]->notify();
    }
  }

  template <class Allocator>
  void basic_static_thread_pool<Allocator>::enqueue(
    remote_queue_t& queue,
    task_base* task,
    std::uint32_t tid) noexcept {
    remote_queue_t* correct_queue = __pool::this_thread_id == queue.id() ? &queue : remotes_.get();
    std::size_t idx = correct_queue->index();
    if (idx < threadStates_.size() && idx == tid) {
      threadStates_[idx]->push_local(task);
    } else {
      const std::size_t threadIndex = tid % threadCount_;
      (*correct_queue)[threadIndex].push_front(task);
      threadStates_[threadIndex]->notify();
    }
  }

  template <class Allocator>
  template <std::derived_from<task_base> TaskT>
  void basic_static_thread_pool<Allocator>::bulk_enqueue(
    TaskT* task,
    std::uint32_t n_threads) noexcept {
    auto& queue = *remotes_.get();
    for (std::size_t i = 0; i < n_threads; ++i) {
      std::uint32_t index = i % available_parallelism();
      queue[index].push_front(task + i);
      threadStates_[index]->notify();
    }
  }

  template <class Allocator>
  void basic_static_thread_pool<Allocator>::bulk_enqueue(
    remote_queue_t& queue,
    __intrusive_queue<&task_base::next> tasks,
    std::size_t tasks_size) noexcept {
    remote_queue_t* correct_queue = __pool::this_thread_id == queue.id() ? &queue : remotes_.get();
    std::size_t idx = correct_queue->index();
    if (idx < threadStates_.size()) {
      threadStates_[idx]->push_local(std::move(tasks));
      return;
    }
    std::size_t nThreads = available_parallelism();
    for (std::size_t i = 0; i < nThreads; ++i) {
      auto [i0, iEnd] = even_share(tasks_size, i, available_parallelism());
      if (i0 == iEnd) {
        continue;
      }
      __intrusive_queue<&task_base::next> tmp{};
      for (std::size_t j = i0; j < iEnd; ++j) {
        tmp.push_back(tasks.pop_front());
      }
      (*correct_queue)[i].prepend(std::move(tmp));
      threadStates_[i]->notify();
    }
  }

  namespace __pool {
    template <class PendingQueue, class LocalQueue>
    void move_pending_to_local(PendingQueue& pending_queue, LocalQueue& local_queue) {
      auto last = local_queue.push_back(pending_queue.begin(), pending_queue.end());
      __intrusive_queue<&task_base::next> tmp{};
      tmp.splice(tmp.begin(), pending_queue, pending_queue.begin(), last);
      tmp.clear();
    }
  }

  template <class Allocator>
  basic_static_thread_pool<Allocator>::thread_state::pop_result
    basic_static_thread_pool<Allocator>::thread_state::try_remote() {
    pop_result result{nullptr, index_};
    __intrusive_queue<&task_base::next> remotes = pool_.remotes_.pop_all_reversed(index_);
    pending_queue_.append(std::move(remotes));
    if (!pending_queue_.empty()) {
      __pool::move_pending_to_local(pending_queue_, local_queue_);
      result.task = local_queue_.pop_back();
    }
    return result;
  }

  template <class Allocator>
  basic_static_thread_pool<Allocator>::thread_state::pop_result
    basic_static_thread_pool<Allocator>::thread_state::try_pop() {
    pop_result result{nullptr, index_};
    result.task = local_queue_.pop_back();
    if (result.task) [[likely]] {
      return result;
    }
    return try_remote();
  }

  template <class Allocator>
  void basic_static_thread_pool<Allocator>::thread_state::push_local(task_base* task) {
    if (!local_queue_.push_back(task)) {
      pending_queue_.push_back(task);
    }
  }

  template <class Allocator>
  void basic_static_thread_pool<Allocator>::thread_state::push_local(
    __intrusive_queue<&task_base::next>&& tasks) {
    pending_queue_.prepend(std::move(tasks));
  }

  template <class Allocator>
  void basic_static_thread_pool<Allocator>::thread_state::set_victims() {
    victims_.reserve(pool_.threadCount_ - 1);
    for (thread_state* state: pool_.threadStates_) {
      if (state == this) {
        continue;
      }
      victims_.emplace_back(&state->local_queue_, state->index_);
    }
  }

  template <class Allocator>
  void basic_static_thread_pool<Allocator>::thread_state::set_stealing() {
    pool_.numThiefs_.fetch_add(1, std::memory_order_relaxed);
  }

  template <class Allocator>
  void basic_static_thread_pool<Allocator>::thread_state::clear_stealing() noexcept {
    if (pool_.numThiefs_.fetch_sub(1, std::memory_order_relaxed) == 1) {
      notify_one_sleeping();
    }
  }

  template <class Allocator>
  basic_static_thread_pool<Allocator>::thread_state::pop_result
    basic_static_thread_pool<Allocator>::thread_state::try_steal_from_victims() {
    if (victims_.empty()) {
      return {nullptr, index_};
    }
    std::uniform_int_distribution<std::uint32_t> dist(0, victims_.size() - 1);
    std::uint32_t victimIndex = dist(rng_);
    auto&& v = victims_[victimIndex];
    return {v.try_steal(), v.index()};
  }

  template <class Allocator>
  basic_static_thread_pool<Allocator>::thread_state::pop_result
    basic_static_thread_pool<Allocator>::thread_state::explore_other_thread_states() {
    pop_result result{};
    set_stealing();
    scope_guard clear_stealing_guard{[this]() noexcept {
      clear_stealing();
    }};
    for (std::size_t i = 0; i < pool_.maxSteals_; ++i) {
      result = try_steal_from_victims();
      if (result.task) {
        return result;
      }
    }
    std::this_thread::yield();
    return result;
  }

  template <class Allocator>
  void basic_static_thread_pool<Allocator>::thread_state::notify_one_sleeping() {
    std::uniform_int_distribution<std::uint32_t> dist(0, pool_.threadCount_ - 1);
    std::uint32_t startIndex = dist(rng_);
    for (std::uint32_t i = 0; i < pool_.threadCount_; ++i) {
      std::uint32_t index = (startIndex + i) % pool_.threadCount_;
      if (index == index_) {
        continue;
      }
      if (pool_.threadStates_[index]->notify()) {
        return;
      }
    }
  }

  template <class Allocator>
  basic_static_thread_pool<Allocator>::thread_state::pop_result
    basic_static_thread_pool<Allocator>::thread_state::pop() {
    pop_result result = try_pop();
    while (!result.task) {
      result = explore_other_thread_states();
      if (result.task) {
        return result;
      }
      std::unique_lock lock{mut_};
      if (stopRequested_) {
        return result;
      }
      state expected = state::running;
      if (state_.compare_exchange_weak(expected, state::sleeping)) {
        cv_.wait(lock);
      }
      state_.store(state::running);
      lock.unlock();
      result = try_pop();
    }
    return result;
  }

  template <class Allocator>
  bool basic_static_thread_pool<Allocator>::thread_state::notify() {
    if (state_.exchange(state::notified) == state::sleeping) {
      {
        std::lock_guard lock{mut_};
      }
      cv_.notify_one();
      return true;
    }
    return false;
  }

  template <class Allocator>
  void basic_static_thread_pool<Allocator>::thread_state::request_stop() {
    {
      std::lock_guard lock{mut_};
      stopRequested_ = true;
    }
    cv_.notify_one();
  }

  template <class Allocator>
  template <typename ReceiverId, class... Condition>
  class basic_static_thread_pool<Allocator>::operation : public task_base {
    using Receiver = stdexec::__t<ReceiverId>;
    friend basic_static_thread_pool<Allocator>::scheduler<Condition...>::sender;

    remote_queue_ref<basic_static_thread_pool> queue_ref_;
    std::tuple<Condition...> condition_;
    Receiver receiver_;

    explicit operation(
      remote_queue_ref<basic_static_thread_pool> queue,
      std::tuple<Condition...> condition,
      Receiver&& r)
      : queue_ref_(queue)
      , condition_(std::move(condition))
      , receiver_((Receiver&&) r) {
      this->__execute = [](task_base* t, const std::uint32_t /* tid */) noexcept {
        auto& op = *static_cast<operation*>(t);
        auto stoken = stdexec::get_stop_token(stdexec::get_env(op.receiver_));
        if constexpr (std::unstoppable_token<decltype(stoken)>) {
          stdexec::set_value((Receiver&&) op.receiver_);
        } else if (stoken.stop_requested()) {
          stdexec::set_stopped((Receiver&&) op.receiver_);
        } else {
          stdexec::set_value((Receiver&&) op.receiver_);
        }
      };
    }

    void enqueue_(task_base* op) const {
      if constexpr (sizeof...(Condition) == 0) {
        queue_ref_.enqueue(op);
      } else {
        queue_ref_.enqueue(op, std::get<0>(condition_));
      }
    }

    friend void tag_invoke(stdexec::start_t, operation& op) noexcept {
      op.enqueue_(&op);
    }
  };

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // What follows is the implementation for parallel bulk execution on static_thread_pool.
  template <class Allocator>
  template <class SenderId, std::integral Shape, class FunId>
  struct basic_static_thread_pool<Allocator>::bulk_sender {
    using Sender = stdexec::__t<SenderId>;
    using Fun = stdexec::__t<FunId>;
    using is_sender = void;

    static_thread_pool& pool_;
    Sender sndr_;
    Shape shape_;
    Fun fun_;

    template <class Fun, class Sender, class Env>
    using with_error_invoke_t = //
      stdexec::__if_c<
        stdexec::__v<stdexec::__value_types_of_t<
          Sender,
          Env,
          stdexec::__mbind_front_q<bulk_non_throwing, Fun, Shape>,
          stdexec::__q<stdexec::__mand>>>,
        stdexec::completion_signatures<>,
        stdexec::__with_exception_ptr>;

    template <class... Tys>
    using set_value_t =
      stdexec::completion_signatures< stdexec::set_value_t(stdexec::__decay_t<Tys>...)>;

    template <class Self, class Env>
    using completion_signatures = //
      stdexec::__try_make_completion_signatures<
        stdexec::__copy_cvref_t<Self, Sender>,
        Env,
        with_error_invoke_t<Fun, stdexec::__copy_cvref_t<Self, Sender>, Env>,
        stdexec::__q<set_value_t>>;

    template <class Self, class Receiver>
    using bulk_op_state_t = //
      bulk_op_state<
        stdexec::__x<stdexec::__copy_cvref_t<Self, Sender>>,
        stdexec::__x<stdexec::__decay_t<Receiver>>,
        Shape,
        Fun>;

    template <stdexec::__decays_to<bulk_sender> Self, stdexec::receiver Receiver>
      requires stdexec::
        receiver_of<Receiver, completion_signatures<Self, stdexec::env_of_t<Receiver>>>
      friend bulk_op_state_t<Self, Receiver>                       //
      tag_invoke(stdexec::connect_t, Self&& self, Receiver&& rcvr) //
      noexcept(stdexec::__nothrow_constructible_from<
               bulk_op_state_t<Self, Receiver>,
               static_thread_pool&,
               Shape,
               Fun,
               Sender,
               Receiver>) {
      return bulk_op_state_t<Self, Receiver>{
        self.pool_, self.shape_, self.fun_, ((Self&&) self).sndr_, (Receiver&&) rcvr};
    }

    template <stdexec::__decays_to<bulk_sender> Self, class Env>
    friend auto tag_invoke(stdexec::get_completion_signatures_t, Self&&, Env&&)
      -> completion_signatures<Self, Env> {
      return {};
    }

    friend auto tag_invoke(stdexec::get_env_t, const bulk_sender& self) noexcept
      -> stdexec::env_of_t<const Sender&> {
      return stdexec::get_env(self.sndr_);
    }
  };

  template <class Allocator>
  template <class SenderId, class ReceiverId, class Shape, class Fun, bool MayThrow>
  struct basic_static_thread_pool<Allocator>::bulk_shared_state {
    using Sender = stdexec::__t<SenderId>;
    using Receiver = stdexec::__t<ReceiverId>;

    struct bulk_task : task_base {
      bulk_shared_state* sh_state_;

      bulk_task(bulk_shared_state* sh_state)
        : sh_state_(sh_state) {
        this->__execute = [](task_base* t, const std::uint32_t tid) noexcept {
          auto& sh_state = *static_cast<bulk_task*>(t)->sh_state_;
          auto total_threads = sh_state.num_agents_required();

          auto computation = [&](auto&... args) {
            auto [begin, end] = even_share(sh_state.shape_, tid, total_threads);
            for (Shape i = begin; i < end; ++i) {
              sh_state.fn_(i, args...);
            }
          };

          auto completion = [&](auto&... args) {
            stdexec::set_value((Receiver&&) sh_state.receiver_, std::move(args)...);
          };

          if constexpr (MayThrow) {
            try {
              sh_state.apply(computation);
            } catch (...) {
              std::uint32_t expected = total_threads;

              if (sh_state.thread_with_exception_.compare_exchange_strong(
                    expected, tid, std::memory_order_relaxed, std::memory_order_relaxed)) {
                sh_state.exception_ = std::current_exception();
              }
            }

            const bool is_last_thread = sh_state.finished_threads_.fetch_add(1)
                                     == (total_threads - 1);

            if (is_last_thread) {
              if (sh_state.exception_) {
                stdexec::set_error((Receiver&&) sh_state.receiver_, std::move(sh_state.exception_));
              } else {
                sh_state.apply(completion);
              }
            }
          } else {
            sh_state.apply(computation);

            const bool is_last_thread = sh_state.finished_threads_.fetch_add(1)
                                     == (total_threads - 1);

            if (is_last_thread) {
              sh_state.apply(completion);
            }
          }
        };
      }
    };

    using variant_t = //
      stdexec::__value_types_of_t<
        Sender,
        stdexec::env_of_t<Receiver>,
        stdexec::__q<stdexec::__decayed_tuple>,
        stdexec::__q<stdexec::__variant>>;

    variant_t data_;
    static_thread_pool& pool_;
    Receiver receiver_;
    Shape shape_;
    Fun fn_;

    std::atomic<std::uint32_t> finished_threads_{0};
    std::atomic<std::uint32_t> thread_with_exception_{0};
    std::exception_ptr exception_;
    std::vector<bulk_task> tasks_;

    std::uint32_t num_agents_required() const {
      return std::min(shape_, static_cast<Shape>(pool_.available_parallelism()));
    }

    template <class F>
    void apply(F f) {
      std::visit(
        [&](auto& tupl) -> void { std::apply([&](auto&... args) -> void { f(args...); }, tupl); },
        data_);
    }

    bulk_shared_state(static_thread_pool& pool, Receiver receiver, Shape shape, Fun fn)
      : pool_{pool}
      , receiver_{(Receiver&&) receiver}
      , shape_{shape}
      , fn_{fn}
      , thread_with_exception_{num_agents_required()}
      , tasks_{num_agents_required(), {this}} {
    }
  };

  template <class Allocator>
  template <class SenderId, class ReceiverId, class Shape, class Fn, bool MayThrow>
  struct basic_static_thread_pool<Allocator>::bulk_receiver {
    using is_receiver = void;
    using Sender = stdexec::__t<SenderId>;
    using Receiver = stdexec::__t<ReceiverId>;

    using shared_state = bulk_shared_state<SenderId, ReceiverId, Shape, Fn, MayThrow>;

    shared_state& shared_state_;

    void enqueue() noexcept {
      shared_state_.pool_.bulk_enqueue(
        shared_state_.tasks_.data(), shared_state_.num_agents_required());
    }

    template <class... As>
    friend void tag_invoke(
      stdexec::same_as<stdexec::set_value_t> auto,
      bulk_receiver&& self,
      As&&... as) noexcept {
      using tuple_t = stdexec::__decayed_tuple<As...>;

      shared_state& state = self.shared_state_;

      if constexpr (MayThrow) {
        try {
          state.data_.template emplace<tuple_t>((As&&) as...);
        } catch (...) {
          stdexec::set_error(std::move(state.receiver_), std::current_exception());
        }
      } else {
        state.data_.template emplace<tuple_t>((As&&) as...);
      }

      if (state.shape_) {
        self.enqueue();
      } else {
        state.apply([&](auto&... args) {
          stdexec::set_value(std::move(state.receiver_), std::move(args)...);
        });
      }
    }

    template <stdexec::__one_of<stdexec::set_error_t, stdexec::set_stopped_t> Tag, class... As>
    friend void tag_invoke(Tag tag, bulk_receiver&& self, As&&... as) noexcept {
      shared_state& state = self.shared_state_;
      tag((Receiver&&) state.receiver_, (As&&) as...);
    }

    friend auto tag_invoke(stdexec::get_env_t, const bulk_receiver& self) noexcept
      -> stdexec::env_of_t<Receiver> {
      return stdexec::get_env(self.shared_state_.receiver_);
    }
  };

  template <class Allocator>
  template <class SenderId, class ReceiverId, std::integral Shape, class Fun>
  struct basic_static_thread_pool<Allocator>::bulk_op_state {
    using Sender = stdexec::__t<SenderId>;
    using Receiver = stdexec::__t<ReceiverId>;

    static constexpr bool may_throw = //
      !stdexec::__v<stdexec::__value_types_of_t<
        Sender,
        stdexec::env_of_t<Receiver>,
        stdexec::__mbind_front_q<bulk_non_throwing, Fun, Shape>,
        stdexec::__q<stdexec::__mand>>>;

    using bulk_rcvr = bulk_receiver<SenderId, ReceiverId, Shape, Fun, may_throw>;
    using shared_state = bulk_shared_state<SenderId, ReceiverId, Shape, Fun, may_throw>;
    using inner_op_state = stdexec::connect_result_t<Sender, bulk_rcvr>;

    shared_state shared_state_;

    inner_op_state inner_op_;

    friend void tag_invoke(stdexec::start_t, bulk_op_state& op) noexcept {
      stdexec::start(op.inner_op_);
    }

    bulk_op_state(static_thread_pool& pool, Shape shape, Fun fn, Sender&& sender, Receiver receiver)
      : shared_state_(pool, (Receiver&&) receiver, shape, fn)
      , inner_op_{stdexec::connect((Sender&&) sender, bulk_rcvr{shared_state_})} {
    }
  };

#if STDEXEC_HAS_STD_RANGES()
  namespace schedule_all_ {
    template <class Rcvr>
    auto get_allocator(const Rcvr& rcvr) {
      if constexpr (stdexec::__callable<stdexec::get_allocator_t, stdexec::env_of_t<Rcvr>>) {
        return stdexec::get_allocator(stdexec::get_env(rcvr));
      } else {
        return std::allocator<char>{};
      }
    }

    template <class Receiver>
    using allocator_of_t = decltype(get_allocator(stdexec::__declval<Receiver>()));

    template <class Pool, class Range>
    struct operation_base {
      Range range_;
      Pool& pool_;
      std::mutex start_mutex_{};
      bool has_started_{false};
      __intrusive_queue<&task_base::next> tasks_{};
      std::size_t tasks_size_{};
      std::atomic<std::size_t> countdown_{std::ranges::size(range_)};
    };

    template <class Pool, class Range, class ItemReceiverId>
    struct item_operation {
      class __t : private task_base {
        using ItemReceiver = stdexec::__t<ItemReceiverId>;

        static void execute_(task_base* base, std::uint32_t /* tid */) noexcept {
          auto op = static_cast<__t*>(base);
          stdexec::set_value(static_cast<ItemReceiver&&>(op->item_receiver_), *op->it_);
        }

        ItemReceiver item_receiver_;
        std::ranges::iterator_t<Range> it_;
        operation_base<Pool, Range>* parent_;
        remote_queue_ref<Pool> remote_queue_;

        friend void tag_invoke(stdexec::start_t, __t& op) noexcept {
          // std::unique_lock lock{op.parent_->start_mutex_};
          if (!op.parent_->has_started_) {
            op.parent_->tasks_.push_back(static_cast<task_base*>(&op));
            op.parent_->tasks_size_ += 1;
          } else {
            // lock.unlock();
            op.remote_queue_.enqueue(static_cast<task_base*>(&op));
          }
        }

       public:
        using __id = item_operation;

        __t(
          ItemReceiver&& item_receiver,
          std::ranges::iterator_t<Range> it,
          operation_base<Pool, Range>* parent,
          remote_queue_ref<Pool> remote_queue)
          : task_base{.__execute = execute_}
          , item_receiver_(static_cast<ItemReceiver&&>(item_receiver))
          , it_(it)
          , parent_(parent)
          , remote_queue_(remote_queue) {
        }
      };
    };

    template <class Pool, class Range>
    struct item_sender {
      struct __t {
        using __id = item_sender;
        using is_sender = void;
        using completion_signatures = stdexec::completion_signatures<stdexec::set_value_t(
          std::ranges::range_reference_t<Range>)>;

        operation_base<Pool, Range>* op_;
        std::ranges::iterator_t<Range> it_;
        remote_queue_ref<Pool> remote_queue_;

        struct env {
          Pool* pool_;

          template <
            stdexec::same_as<stdexec::get_completion_scheduler_t<stdexec::set_value_t>> Query>
          friend auto tag_invoke(Query, const env& e) noexcept ->
            typename Pool::template scheduler<> {
            return e.pool_->get_scheduler();
          }
        };

        template <stdexec::same_as<stdexec::get_env_t> GetEnv, stdexec::__decays_to<__t> Self>
        friend auto tag_invoke(GetEnv, Self&& self) noexcept -> env {
          return {self.op_->pool_};
        }

        template <stdexec::__decays_to<__t> Self, stdexec::receiver ItemReceiver>
          requires stdexec::receiver_of<ItemReceiver, completion_signatures>
        friend auto tag_invoke(stdexec::connect_t, Self&& self, ItemReceiver rcvr) noexcept
          -> stdexec::__t<item_operation<Pool, Range, stdexec::__id<ItemReceiver>>> {
          return {static_cast<ItemReceiver&&>(rcvr), self.it_, self.op_, self.remote_queue_};
        }
      };
    };

    template <class Pool, class Range, class Receiver>
    struct operation_base_with_receiver : operation_base<Pool, Range> {
      Receiver receiver_;

      operation_base_with_receiver(Range range, Pool& pool, Receiver&& receiver)
        : operation_base<Pool, Range>{range, pool}
        , receiver_(static_cast<Receiver&&>(receiver)) {
      }
    };

    template <class Pool, class Range, class Receiver>
    struct next_receiver {
      struct __t {
        using is_receiver = void;
        operation_base_with_receiver<Pool, Range, Receiver>* op_;

        template <stdexec::same_as<stdexec::set_value_t> SetValue, stdexec::same_as<__t> Self>
        friend void tag_invoke(SetValue, Self&& self) noexcept {
          std::size_t countdown = self.op_->countdown_.fetch_sub(1, std::memory_order_relaxed);
          if (countdown == 1) {
            stdexec::set_value((Receiver&&) self.op_->receiver_);
          }
        }

        template <stdexec::same_as<stdexec::set_stopped_t> SetStopped, stdexec::same_as<__t> Self>
        friend void tag_invoke(SetStopped, Self&& self) noexcept {
          std::size_t countdown = self.op_->countdown_.fetch_sub(1, std::memory_order_relaxed);
          if (countdown == 1) {
            stdexec::set_value((Receiver&&) self.op_->receiver_);
          }
        }

        template <stdexec::same_as<stdexec::get_env_t> GetEnv, stdexec::__decays_to<__t> Self>
        friend auto tag_invoke(GetEnv, Self&& self) noexcept -> stdexec::env_of_t<Receiver> {
          return stdexec::get_env(self.op_->receiver_);
        }
      };
    };

    template <class Pool, class Range, class Receiver>
    struct operation {
      class __t : operation_base_with_receiver<Pool, Range, Receiver> {
        using Allocator = allocator_of_t<const Receiver&>;
        using ItemSender = stdexec::__t<item_sender<Pool, Range>>;
        using NextSender = next_sender_of_t<Receiver, ItemSender>;
        using NextReceiver = stdexec::__t<next_receiver<Pool, Range, Receiver>>;
        using ItemOperation = stdexec::connect_result_t<NextSender, NextReceiver>;

        using ItemAllocator =
          std::allocator_traits<Allocator>::template rebind_alloc<__manual_lifetime<ItemOperation>>;

        std::vector<__manual_lifetime<ItemOperation>, ItemAllocator> items_;

        template <stdexec::same_as<__t> Self>
        friend void tag_invoke(stdexec::start_t, Self& op) noexcept {
          std::size_t size = op.items_.size();
          std::size_t nthreads = op.pool_.available_parallelism();
          bwos_params params = op.pool_.params();
          std::size_t localSize = params.blockSize * params.numBlocks;
          std::size_t chunkSize = std::min<std::size_t>(size / nthreads, localSize * nthreads);
          auto remote_queue = op.pool_.get_remote_queue();
          std::ranges::iterator_t<Range> it = std::ranges::begin(op.range_);
          std::size_t i0 = 0;
          while (i0 + chunkSize < size) {
            for (std::size_t i = i0; i < i0 + chunkSize; ++i) {
              auto& item_op = op.items_[i].__construct_with([&] {
                return stdexec::connect(
                  set_next(op.receiver_, ItemSender{&op, it + i, remote_queue}), NextReceiver{&op});
              });
              stdexec::start(item_op);
            }
            // std::unique_lock lock{op.start_mutex_};
            remote_queue.bulk_enqueue(std::move(op.tasks_), std::exchange(op.tasks_size_, 0));
            // lock.unlock();
            i0 += chunkSize;
          }
          for (std::size_t i = i0; i < size; ++i) {
            auto& item_op = op.items_[i].__construct_with([&] {
              return stdexec::connect(
                set_next(op.receiver_, ItemSender{&op, it + i, remote_queue}), NextReceiver{&op});
            });
            stdexec::start(item_op);
          }
          // std::unique_lock lock{op.start_mutex_};
          op.has_started_ = true;
          remote_queue.bulk_enqueue(std::move(op.tasks_), std::exchange(op.tasks_size_, 0));
        }

       public:
        using __id = operation;

        __t(Range range, static_thread_pool& pool, Receiver&& receiver)
          : operation_base_with_receiver<
            Pool,
            Range,
            Receiver>{std::move(range), pool, static_cast<Receiver&&>(receiver)}
          , items_(std::ranges::size(this->range_), ItemAllocator(get_allocator(this->receiver_))) {
        }

        ~__t() {
          if (this->has_started_) {
            for (auto& item: items_) {
              item.__destroy();
            }
          }
        }
      };
    };

    template <class Pool, class Range>
    class sequence<Pool, Range>::__t {
      using item_sender_t = stdexec::__t<item_sender<Pool, Range>>;

      Range range_;
      Pool* pool_;

     public:
      using __id = sequence;

      using is_sender = sequence_tag;

      using completion_signatures = stdexec::completion_signatures<
        stdexec::set_value_t(),
        stdexec::set_error_t(std::exception_ptr),
        stdexec::set_stopped_t()>;

      using item_types = exec::item_types<stdexec::__t<item_sender<Pool, Range>>>;

      __t(Range range, Pool& pool)
        : range_(static_cast<Range&&>(range))
        , pool_(&pool) {
      }

     private:
      template <stdexec::__decays_to<__t> Self, exec::sequence_receiver_of<item_types> Receiver>
      friend auto tag_invoke(exec::subscribe_t, Self&& self, Receiver rcvr) noexcept
        -> stdexec::__t<operation<Pool, Range, Receiver>> {
        return {static_cast<Range&&>(self.range_), *self.pool_, static_cast<Receiver&&>(rcvr)};
      }
    };
  }

  struct schedule_all_t {
    template <class Pool, class Range>
    stdexec::__t<schedule_all_::sequence<Pool, stdexec::__decay_t<Range>>>
      operator()(Pool& pool, Range&& range) const {
      return {static_cast<Range&&>(range), pool};
    }
  };

  inline constexpr schedule_all_t schedule_all{};
#endif

} // namespace exec
