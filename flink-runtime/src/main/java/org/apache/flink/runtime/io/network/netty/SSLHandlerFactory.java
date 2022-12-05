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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.shaded.guava30.com.google.common.base.Suppliers;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/** Creates and configures {@link SslHandler} instances. */
public class SSLHandlerFactory {
    private final Supplier<SslContext> sslContextSupplier;

    private final int handshakeTimeoutMs;

    private final int closeNotifyFlushTimeoutMs;

    /**
     * Create a new {@link SslHandler} factory.
     *
     * @param handshakeTimeoutMs SSL session timeout during handshakes (-1 = use system default)
     * @param closeNotifyFlushTimeoutMs SSL session timeout after flushing the <tt>close_notify</tt>
     *     message (-1 = use system default)
     */
    public SSLHandlerFactory(
            final SslContext sslContext,
            final int handshakeTimeoutMs,
            final int closeNotifyFlushTimeoutMs) {

        this(
                Suppliers.ofInstance(requireNonNull(sslContext)),
                handshakeTimeoutMs,
                closeNotifyFlushTimeoutMs);
    }

    public SSLHandlerFactory(
            final Supplier<SslContext> sslContextSupplier,
            final int handshakeTimeoutMs,
            final int closeNotifyFlushTimeoutMs) {
        this.sslContextSupplier = sslContextSupplier;
        this.handshakeTimeoutMs = handshakeTimeoutMs;
        this.closeNotifyFlushTimeoutMs = closeNotifyFlushTimeoutMs;
    }

    public SslHandler createNettySSLHandler(ByteBufAllocator allocator) {
        return createNettySSLHandler(context().newEngine(allocator));
    }

    public SslHandler createNettySSLHandler(ByteBufAllocator allocator, String hostname, int port) {
        return createNettySSLHandler(context().newEngine(allocator, hostname, port));
    }

    private SslContext context() {
        return sslContextSupplier.get();
    }

    private SslHandler createNettySSLHandler(SSLEngine sslEngine) {
        SslHandler sslHandler = new SslHandler(sslEngine);
        if (handshakeTimeoutMs >= 0) {
            sslHandler.setHandshakeTimeoutMillis(handshakeTimeoutMs);
        }
        if (closeNotifyFlushTimeoutMs >= 0) {
            sslHandler.setCloseNotifyFlushTimeoutMillis(closeNotifyFlushTimeoutMs);
        }

        return sslHandler;
    }
}
