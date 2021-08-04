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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.ConfigurationException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import java.util.Map;
import java.util.Optional;

/** Test inbound channel handler factory. */
public class Prio0InboundChannelHandlerFactory implements InboundChannelHandlerFactory {
    public static final ConfigOption<String> REDIRECT_FROM_URL =
            ConfigOptions.key("test.in.redirect.from.url").stringType().defaultValue("");
    public static final ConfigOption<String> REDIRECT_TO_URL =
            ConfigOptions.key("test.in.redirect.to.url").stringType().defaultValue("");

    @Override
    public int priority() {
        return 0;
    }

    @Override
    public Optional<ChannelHandler> createHandler(
            Configuration configuration, Map<String, String> responseHeaders)
            throws ConfigurationException {
        String redirectFromUrl = configuration.getString(REDIRECT_FROM_URL);
        String redirectToUrl = configuration.getString(REDIRECT_TO_URL);
        if (!redirectFromUrl.isEmpty() && !redirectToUrl.isEmpty()) {
            return Optional.of(
                    new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof HttpRequest) {
                                HttpRequest httpRequest = (HttpRequest) msg;
                                if (httpRequest.uri().equals(redirectFromUrl)) {
                                    httpRequest.setUri(redirectToUrl);
                                }
                            }
                            ctx.fireChannelRead(ReferenceCountUtil.retain(msg));
                        }
                    });
        }
        return Optional.empty();
    }
}
