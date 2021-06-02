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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelDuplexHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;

import java.util.Base64;

/**
 * Netty handler for basic authentication on the Client side. Based on
 * https://github.com/vzhn/netty-http-authenticator/blob/master/src/main/java/me/vzhilin/auth/netty/BasicNettyHttpAuthenticator.java
 * (MIT License).
 */
@ChannelHandler.Sharable
public final class ClientBasicHttpAuthenticator extends ChannelDuplexHandler {
    private final String basicAuthHeader;

    public ClientBasicHttpAuthenticator(String credentials) {
        this.basicAuthHeader =
                "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes());
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
        if (msg instanceof HttpRequest) {
            ((HttpRequest) msg).headers().set(HttpHeaderNames.AUTHORIZATION, basicAuthHeader);
        }
        super.write(ctx, msg, promise);
    }
}
