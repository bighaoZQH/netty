/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util.concurrent;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base class for {@link EventExecutorGroup} implementations that handles their tasks with multiple threads at
 * the same time.
 */
public abstract class MultithreadEventExecutorGroup extends AbstractEventExecutorGroup {

    // 线程池，数组形式可知为固定线程池
    private final EventExecutor[] children;
    // 线程索引，用于线程选择
    private final Set<EventExecutor> readonlyChildren;
    // 终止的线程个数
    private final AtomicInteger terminatedChildren = new AtomicInteger();
    // 线程池终止时的异步结果
    private final Promise<?> terminationFuture = new DefaultPromise(GlobalEventExecutor.INSTANCE);
    // 线程选择器
    private final EventExecutorChooserFactory.EventExecutorChooser chooser;

    /**
     * Create a new instance.
     *
     * @param nThreads          the number of threads that will be used by this instance.
     * @param threadFactory     the ThreadFactory to use, or {@code null} if the default should be used.
     * @param args              arguments which will passed to each {@link #newChild(Executor, Object...)} call
     */
    protected MultithreadEventExecutorGroup(int nThreads, ThreadFactory threadFactory, Object... args) {
        this(nThreads, threadFactory == null ? null : new ThreadPerTaskExecutor(threadFactory), args);
    }

    /**
     * Create a new instance.
     *
     * @param nThreads          the number of threads that will be used by this instance.
     * @param executor          the Executor to use, or {@code null} if the default should be used.
     * @param args              arguments which will passed to each {@link #newChild(Executor, Object...)} call
     */
    protected MultithreadEventExecutorGroup(int nThreads, Executor executor, Object... args) {
        this(nThreads, executor, DefaultEventExecutorChooserFactory.INSTANCE, args);
    }

    /**
     * Create a new instance.
     *
     * @param nThreads          the number of threads that will be used by this instance.
     * @param executor          the Executor to use, or {@code null} if the default should be used.
     * @param chooserFactory    the {@link EventExecutorChooserFactory} to use.
     * @param args              arguments which will passed to each {@link #newChild(Executor, Object...)} call
     *
     * nThreads - 线程数
     * executor - 执行器：如果传入null，则采用netty默认的线程工厂和默认的执行器ThreadPerTaskExecutor
     * chooserFactory - 默认创建单例new DefaultEventExecutorChooserFactory()
     * args - 在创建执行器的时候传入的固定参数
     */
    protected MultithreadEventExecutorGroup(int nThreads, Executor executor,
                                            EventExecutorChooserFactory chooserFactory, Object... args) {
        if (nThreads <= 0) {
            throw new IllegalArgumentException(String.format("nThreads: %d (expected: > 0)", nThreads));
        }

        if (executor == null) {
            // 如果传入null，则采用netty默认的线程工厂和默认的执行器ThreadPerTaskExecutor
            // newDefaultThreadFactory()构建出来一个线程工厂，用来生产具体线程实例的工厂
            // 创建出来的线程实例名称为 className + poolId + 线程id，线程类型为FastThreadLocal
            executor = new ThreadPerTaskExecutor(newDefaultThreadFactory());
        }

        // 创建指定线程数的执行器数组，该数组的每一个元素是用于存储NioEventLoop的
        children = new EventExecutor[nThreads];

        // 初始化线程
        for (int i = 0; i < nThreads; i ++) {
            boolean success = false;
            try {
                // 创建线程 类型为NioEventLoop
                // executor-—>ThreadPerTaskExecutor实例，这个对象里包含了一个ThreadFactory，通过该工厂来创建线程
                children[i] = newChild(executor, args);
                success = true;
            } catch (Exception e) {
                // TODO: Think about if this is a good exception type
                throw new IllegalStateException("failed to create a child event loop", e);
            } finally {
                if (!success) {
                    // 如果有一个线程没有创建成功,则把整个线程池里已实例化的的线程优雅关闭
                    for (int j = 0; j < i; j ++) {
                        children[j].shutdownGracefully();
                    }

                    // 优雅关闭后，不代表线程任务已经结束，因此需要确认线程任务结束
                    for (int j = 0; j < i; j ++) {
                        EventExecutor e = children[j];
                        try {
                            // 如果线程关闭后任务没有返回，则等待任务返回，如果超时，抛出异常
                            while (!e.isTerminated()) {
                                e.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
                            }
                        } catch (InterruptedException interrupted) {
                            // Let the caller handle the interruption.
                            // 这里捕获了awaitTermination()抛出的异常，终止线程
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
        }

        /**
         * 返回一个PowerOfTwoEventExecutorChooser或者GenericEventExecutorChooser的实例对象，
         * 根据NioEventLoopGroup中NioEventLoop的个数而定(即，children数组的个数).
         * 如果该NioEventLoopGroup的NioEventLoop个数为2的幂次方个，则chooser是一个PowerOfTwoEventExecutorChooser实例；
         * 否则，chooser是一个GenericEventExecutorChooser实例.
         * PowerOfTwoEventExecutorChooser和GenericEventExecutorChooser都是通过简单轮询的方式选择下一个EventExecutor
         *
         * 后面外部资源获取 或者 注册到nioEventLoop，都是通过chooser来分配NioEventLoop的
         */
        chooser = chooserFactory.newChooser(children);

        // 创建线程关闭监听 线程池中的线程每终止一个增加记录数，直到全部终止,设置线程池异步终止结果为成功
        final FutureListener<Object> terminationListener = new FutureListener<Object>() {
            @Override
            public void operationComplete(Future<Object> future) throws Exception {
                // 当children都关闭后，设置线程池异步终止结果为成功
                if (terminatedChildren.incrementAndGet() == children.length) {
                    terminationFuture.setSuccess(null);
                }
            }
        };

        // 给每一个单例线程池添加一个关闭监听器
        for (EventExecutor e: children) {
            e.terminationFuture().addListener(terminationListener);
        }

        // 创建readonlyChildren unmodifiableSet返回指定set的不可修改视图
        Set<EventExecutor> childrenSet = new LinkedHashSet<EventExecutor>(children.length);
        Collections.addAll(childrenSet, children);
        readonlyChildren = Collections.unmodifiableSet(childrenSet);
    }

    protected ThreadFactory newDefaultThreadFactory() {
        return new DefaultThreadFactory(getClass());
    }

    @Override
    public EventExecutor next() {
        return chooser.next();
    }

    @Override
    public Iterator<EventExecutor> iterator() {
        return readonlyChildren.iterator();
    }

    /**
     * Return the number of {@link EventExecutor} this implementation uses. This number is the maps
     * 1:1 to the threads it use.
     */
    public final int executorCount() {
        return children.length;
    }

    /**
     * Create a new EventExecutor which will later then accessible via the {@link #next()}  method. This method will be
     * called for each thread that will serve this {@link MultithreadEventExecutorGroup}.
     *
     */
    protected abstract EventExecutor newChild(Executor executor, Object... args) throws Exception;

    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
        for (EventExecutor l: children) {
            l.shutdownGracefully(quietPeriod, timeout, unit);
        }
        return terminationFuture();
    }

    @Override
    public Future<?> terminationFuture() {
        return terminationFuture;
    }

    @Override
    @Deprecated
    public void shutdown() {
        for (EventExecutor l: children) {
            l.shutdown();
        }
    }

    @Override
    public boolean isShuttingDown() {
        for (EventExecutor l: children) {
            if (!l.isShuttingDown()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isShutdown() {
        for (EventExecutor l: children) {
            if (!l.isShutdown()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isTerminated() {
        for (EventExecutor l: children) {
            if (!l.isTerminated()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        loop: for (EventExecutor l: children) {
            for (;;) {
                long timeLeft = deadline - System.nanoTime();
                if (timeLeft <= 0) {
                    break loop;
                }
                if (l.awaitTermination(timeLeft, TimeUnit.NANOSECONDS)) {
                    break;
                }
            }
        }
        return isTerminated();
    }
}
