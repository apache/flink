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

package org.apache.flink.connector.base.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;

/**
 * A sink that dynamically routes elements to different underlying sinks based on a routing
 * function.
 *
 * <p>The {@code DemultiplexingSink} allows elements to be routed to different sink instances at
 * runtime based on the content of each element. This is useful for scenarios such as:
 *
 * <ul>
 *   <li>Routing messages to different Kafka topics based on message type
 *   <li>Writing to different databases based on tenant ID
 *   <li>Sending data to different Elasticsearch clusters based on data characteristics
 * </ul>
 *
 * <p>The sink maintains an internal cache of sink instances, creating new sinks on-demand when
 * previously unseen routes are encountered. This provides good performance while supporting dynamic
 * routing scenarios.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Route messages to different Kafka topics
 * SinkRouter<MyMessage, String> router = new SinkRouter<MyMessage, String>() {
 *     @Override
 *     public String getRoute(MyMessage element) {
 *         return element.getTopicName();
 *     }
 *
 *     @Override
 *     public Sink<MyMessage> createSink(String topicName, MyMessage element) {
 *         return KafkaSink.<MyMessage>builder()
 *             .setBootstrapServers("localhost:9092")
 *             .setRecordSerializer(...)
 *             .setTopics(topicName)
 *             .build();
 *     }
 * };
 *
 * DemultiplexingSink<MyMessage, String> demuxSink =
 *     new DemultiplexingSink<>(router);
 *
 * dataStream.sinkTo(demuxSink);
 * }</pre>
 *
 * <p>The sink supports checkpointing and recovery through the {@link SupportsWriterState}
 * interface. State from all underlying sink writers is collected and restored appropriately during
 * recovery.
 *
 * @param <InputT> The type of input elements
 * @param <RouteT> The type of route keys used for sink selection
 */
@PublicEvolving
public class DemultiplexingSink<InputT, RouteT>
        implements Sink<InputT>, SupportsWriterState<InputT, DemultiplexingSinkState<RouteT>> {

    private static final long serialVersionUID = 1L;

    /** The router that determines how elements are routed to sinks. */
    private final SinkRouter<InputT, RouteT> sinkRouter;

    /**
     * Creates a new demultiplexing sink with the given router.
     *
     * @param sinkRouter The router that determines how elements are routed to different sinks
     */
    public DemultiplexingSink(SinkRouter<InputT, RouteT> sinkRouter) {
        this.sinkRouter = Preconditions.checkNotNull(sinkRouter, "sinkRouter must not be null");
    }

    @Override
    public SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException {
        return new DemultiplexingSinkWriter<>(sinkRouter, context);
    }

    @Override
    public StatefulSinkWriter<InputT, DemultiplexingSinkState<RouteT>> restoreWriter(
            WriterInitContext context, Collection<DemultiplexingSinkState<RouteT>> recoveredState)
            throws IOException {

        return new DemultiplexingSinkWriter<>(sinkRouter, context, recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<DemultiplexingSinkState<RouteT>> getWriterStateSerializer() {
        return new DemultiplexingSinkStateSerializer<>();
    }

    /**
     * Gets the sink router used by this demultiplexing sink.
     *
     * @return The sink router
     */
    public SinkRouter<InputT, RouteT> getSinkRouter() {
        return sinkRouter;
    }
}
