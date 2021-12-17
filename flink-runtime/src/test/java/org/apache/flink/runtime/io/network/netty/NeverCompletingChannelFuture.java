/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.Future;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SuppressWarnings({"InfiniteLoopStatement", "BusyWait"})
class NeverCompletingChannelFuture implements ChannelFuture {

    @Override
    public Channel channel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSuccess() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancellable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Throwable cause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture addListener(
            GenericFutureListener<? extends Future<? super Void>> genericFutureListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture addListeners(
            GenericFutureListener<? extends Future<? super Void>>... genericFutureListeners) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture removeListener(
            GenericFutureListener<? extends Future<? super Void>> genericFutureListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture removeListeners(
            GenericFutureListener<? extends Future<? super Void>>... genericFutureListeners) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture sync() throws InterruptedException {
        while (true) {
            Thread.sleep(50);
        }
    }

    @Override
    public ChannelFuture syncUninterruptibly() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture await() throws InterruptedException {
        while (true) {
            Thread.sleep(50);
        }
    }

    @Override
    public ChannelFuture awaitUninterruptibly() {
        while (true) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    @Override
    public boolean await(long l, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean await(long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean awaitUninterruptibly(long l, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean awaitUninterruptibly(long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Void getNow() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean cancel(boolean b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDone() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Void get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isVoid() {
        throw new UnsupportedOperationException();
    }
}
