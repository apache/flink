/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundInvoker;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelProgressivePromise;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.EventExecutorGroup;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Testing implementation of {@link ChannelPipeline}. */
class TestingChannelPipeline implements ChannelPipeline {

    @Override
    public ChannelPipeline addFirst(String s, ChannelHandler channelHandler) {
        return null;
    }

    @Override
    public ChannelPipeline addFirst(
            EventExecutorGroup eventExecutorGroup, String s, ChannelHandler channelHandler) {
        return null;
    }

    @Override
    public ChannelPipeline addLast(String s, ChannelHandler channelHandler) {
        return null;
    }

    @Override
    public ChannelPipeline addLast(
            EventExecutorGroup eventExecutorGroup, String s, ChannelHandler channelHandler) {
        return null;
    }

    @Override
    public ChannelPipeline addBefore(String s, String s1, ChannelHandler channelHandler) {
        return null;
    }

    @Override
    public ChannelPipeline addBefore(
            EventExecutorGroup eventExecutorGroup,
            String s,
            String s1,
            ChannelHandler channelHandler) {
        return null;
    }

    @Override
    public ChannelPipeline addAfter(String s, String s1, ChannelHandler channelHandler) {
        return null;
    }

    @Override
    public ChannelPipeline addAfter(
            EventExecutorGroup eventExecutorGroup,
            String s,
            String s1,
            ChannelHandler channelHandler) {
        return null;
    }

    @Override
    public ChannelPipeline addFirst(ChannelHandler... channelHandlers) {
        return null;
    }

    @Override
    public ChannelPipeline addFirst(
            EventExecutorGroup eventExecutorGroup, ChannelHandler... channelHandlers) {
        return null;
    }

    @Override
    public ChannelPipeline addLast(ChannelHandler... channelHandlers) {
        return null;
    }

    @Override
    public ChannelPipeline addLast(
            EventExecutorGroup eventExecutorGroup, ChannelHandler... channelHandlers) {
        return null;
    }

    @Override
    public ChannelPipeline remove(ChannelHandler channelHandler) {
        return null;
    }

    @Override
    public ChannelHandler remove(String s) {
        return null;
    }

    @Override
    public <T extends ChannelHandler> T remove(Class<T> aClass) {
        return null;
    }

    @Override
    public ChannelHandler removeFirst() {
        return null;
    }

    @Override
    public ChannelHandler removeLast() {
        return null;
    }

    @Override
    public ChannelPipeline replace(
            ChannelHandler channelHandler, String s, ChannelHandler channelHandler1) {
        return null;
    }

    @Override
    public ChannelHandler replace(String s, String s1, ChannelHandler channelHandler) {
        return null;
    }

    @Override
    public <T extends ChannelHandler> T replace(
            Class<T> aClass, String s, ChannelHandler channelHandler) {
        return null;
    }

    @Override
    public ChannelHandler first() {
        return null;
    }

    @Override
    public ChannelHandlerContext firstContext() {
        return null;
    }

    @Override
    public ChannelHandler last() {
        return null;
    }

    @Override
    public ChannelHandlerContext lastContext() {
        return null;
    }

    @Override
    public ChannelHandler get(String s) {
        return null;
    }

    @Override
    public <T extends ChannelHandler> T get(Class<T> aClass) {
        return null;
    }

    @Override
    public ChannelHandlerContext context(ChannelHandler channelHandler) {
        return null;
    }

    @Override
    public ChannelHandlerContext context(String s) {
        return null;
    }

    @Override
    public ChannelHandlerContext context(Class<? extends ChannelHandler> aClass) {
        return null;
    }

    @Override
    public Channel channel() {
        return null;
    }

    @Override
    public List<String> names() {
        return null;
    }

    @Override
    public Map<String, ChannelHandler> toMap() {
        return null;
    }

    @Override
    public ChannelPipeline fireChannelRegistered() {
        return null;
    }

    @Override
    public ChannelPipeline fireChannelUnregistered() {
        return null;
    }

    @Override
    public ChannelPipeline fireChannelActive() {
        return null;
    }

    @Override
    public ChannelPipeline fireChannelInactive() {
        return null;
    }

    @Override
    public ChannelPipeline fireExceptionCaught(Throwable throwable) {
        return null;
    }

    @Override
    public ChannelPipeline fireUserEventTriggered(Object o) {
        return null;
    }

    @Override
    public ChannelPipeline fireChannelRead(Object o) {
        return null;
    }

    @Override
    public ChannelPipeline fireChannelReadComplete() {
        return null;
    }

    @Override
    public ChannelPipeline fireChannelWritabilityChanged() {
        return null;
    }

    @Override
    public ChannelFuture bind(SocketAddress socketAddress) {
        return null;
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress) {
        return null;
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress, SocketAddress socketAddress1) {
        return null;
    }

    @Override
    public ChannelFuture disconnect() {
        return null;
    }

    @Override
    public ChannelFuture close() {
        return null;
    }

    @Override
    public ChannelFuture deregister() {
        return null;
    }

    @Override
    public ChannelFuture bind(SocketAddress socketAddress, ChannelPromise channelPromise) {
        return null;
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress, ChannelPromise channelPromise) {
        return null;
    }

    @Override
    public ChannelFuture connect(
            SocketAddress socketAddress,
            SocketAddress socketAddress1,
            ChannelPromise channelPromise) {
        return null;
    }

    @Override
    public ChannelFuture disconnect(ChannelPromise channelPromise) {
        return null;
    }

    @Override
    public ChannelFuture close(ChannelPromise channelPromise) {
        return null;
    }

    @Override
    public ChannelFuture deregister(ChannelPromise channelPromise) {
        return null;
    }

    @Override
    public ChannelOutboundInvoker read() {
        return null;
    }

    @Override
    public ChannelFuture write(Object o) {
        return null;
    }

    @Override
    public ChannelFuture write(Object o, ChannelPromise channelPromise) {
        return null;
    }

    @Override
    public ChannelPipeline flush() {
        return null;
    }

    @Override
    public ChannelFuture writeAndFlush(Object o, ChannelPromise channelPromise) {
        return null;
    }

    @Override
    public ChannelFuture writeAndFlush(Object o) {
        return null;
    }

    @Override
    public ChannelPromise newPromise() {
        return null;
    }

    @Override
    public ChannelProgressivePromise newProgressivePromise() {
        return null;
    }

    @Override
    public ChannelFuture newSucceededFuture() {
        return null;
    }

    @Override
    public ChannelFuture newFailedFuture(Throwable throwable) {
        return null;
    }

    @Override
    public ChannelPromise voidPromise() {
        return null;
    }

    @Override
    public Iterator<Map.Entry<String, ChannelHandler>> iterator() {
        return null;
    }
}
