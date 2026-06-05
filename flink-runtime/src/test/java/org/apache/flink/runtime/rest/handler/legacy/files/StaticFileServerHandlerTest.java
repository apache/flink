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

package org.apache.flink.runtime.rest.handler.legacy.files;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StaticFileServerHandlerTest {

    @Test
    void testSendNotModifiedIncludesConnectionCloseHeader() {
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                StaticFileServerHandler.sendNotModified(ctx);
                            }
                        });

        FullHttpResponse response = channel.readOutbound();

        assertThat(response).as("No response received!").isNotNull();

        try {
            assertThat(response.status()).isEqualTo(HttpResponseStatus.NOT_MODIFIED);

            assertThat(response.headers().contains(HttpHeaderNames.CONNECTION))
                    .as("The 'Connection' header is missing!")
                    .isTrue();

            assertThat(response.headers().get(HttpHeaderNames.CONNECTION))
                    .as("The value of 'Connection' header is not 'close'!")
                    .isEqualTo(HttpHeaderValues.CLOSE.toString());
        } finally {
            response.release();
        }
    }
}
