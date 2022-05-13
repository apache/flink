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

package org.apache.flink.runtime.rest.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.messages.MessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Interface to expose the supported {@link MessageHeaders} of a {@link RestServerEndpoint}. */
public interface DocumentingRestEndpoint {
    List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(
            final CompletableFuture<String> localAddressFuture);

    default List<MessageHeaders<?, ?, ?>> getSpecs() {
        final Comparator<String> comparator =
                new RestServerEndpoint.RestHandlerUrlComparator.CaseInsensitiveOrderComparator();

        return initializeHandlers(CompletableFuture.completedFuture(null)).stream()
                .map(tuple -> tuple.f0)
                .filter(spec -> spec instanceof MessageHeaders)
                .map(spec -> (MessageHeaders<?, ?, ?>) spec)
                .sorted(
                        (spec1, spec2) ->
                                comparator.compare(
                                        spec1.getTargetRestEndpointURL(),
                                        spec2.getTargetRestEndpointURL()))
                .collect(Collectors.toList());
    }
}
