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

package org.apache.flink.connector.pulsar.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.committer.PulsarCommittable;
import org.apache.flink.connector.pulsar.sink.committer.PulsarCommittableSerializer;
import org.apache.flink.connector.pulsar.sink.committer.PulsarCommitter;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.PulsarWriter;
import org.apache.flink.connector.pulsar.sink.writer.delayer.MessageDelayer;
import org.apache.flink.connector.pulsar.sink.writer.router.KeyHashTopicRouter;
import org.apache.flink.connector.pulsar.sink.writer.router.RoundRobinTopicRouter;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicMetadataListener;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Sink implementation of Pulsar. Please use a {@link PulsarSinkBuilder} to construct a {@link
 * PulsarSink}. The following example shows how to create a PulsarSink receiving records of {@code
 * String} type.
 *
 * <pre>{@code
 * PulsarSink<String> sink = PulsarSink.builder()
 *      .setServiceUrl(operator().serviceUrl())
 *      .setAdminUrl(operator().adminUrl())
 *      .setTopic(topic)
 *      .setSerializationSchema(PulsarSerializationSchema.pulsarSchema(Schema.STRING))
 *      .build();
 * }</pre>
 *
 * <p>The sink supports all delivery guarantees described by {@link DeliveryGuarantee}.
 *
 * <ul>
 *   <li>{@link DeliveryGuarantee#NONE} does not provide any guarantees: messages may be lost in
 *       case of issues on the Pulsar broker and messages may be duplicated in case of a Flink
 *       failure.
 *   <li>{@link DeliveryGuarantee#AT_LEAST_ONCE} the sink will wait for all outstanding records in
 *       the Pulsar buffers to be acknowledged by the Pulsar producer on a checkpoint. No messages
 *       will be lost in case of any issue with the Pulsar brokers but messages may be duplicated
 *       when Flink restarts.
 *   <li>{@link DeliveryGuarantee#EXACTLY_ONCE}: In this mode the PulsarSink will write all messages
 *       in a Pulsar transaction that will be committed to Pulsar on a checkpoint. Thus, no
 *       duplicates will be seen in case of a Flink restart. However, this delays record writing
 *       effectively until a checkpoint is written, so adjust the checkpoint duration accordingly.
 *       Additionally, it is highly recommended to tweak Pulsar transaction timeout (link) >>
 *       maximum checkpoint duration + maximum restart duration or data loss may happen when Pulsar
 *       expires an uncommitted transaction.
 * </ul>
 *
 * <p>See {@link PulsarSinkBuilder} for more details.
 *
 * @param <IN> The input type of the sink.
 */
@PublicEvolving
public class PulsarSink<IN> implements TwoPhaseCommittingSink<IN, PulsarCommittable> {
    private static final long serialVersionUID = 4416714587951282119L;

    private final SinkConfiguration sinkConfiguration;
    private final PulsarSerializationSchema<IN> serializationSchema;
    private final TopicMetadataListener metadataListener;
    private final MessageDelayer<IN> messageDelayer;
    private final TopicRouter<IN> topicRouter;

    PulsarSink(
            SinkConfiguration sinkConfiguration,
            PulsarSerializationSchema<IN> serializationSchema,
            TopicMetadataListener metadataListener,
            TopicRoutingMode topicRoutingMode,
            TopicRouter<IN> topicRouter,
            MessageDelayer<IN> messageDelayer) {
        this.sinkConfiguration = checkNotNull(sinkConfiguration);
        this.serializationSchema = checkNotNull(serializationSchema);
        this.metadataListener = checkNotNull(metadataListener);
        this.messageDelayer = checkNotNull(messageDelayer);
        checkNotNull(topicRoutingMode);

        // Create topic router supplier.
        if (topicRoutingMode == TopicRoutingMode.CUSTOM) {
            this.topicRouter = checkNotNull(topicRouter);
        } else if (topicRoutingMode == TopicRoutingMode.ROUND_ROBIN) {
            this.topicRouter = new RoundRobinTopicRouter<>(sinkConfiguration);
        } else {
            this.topicRouter = new KeyHashTopicRouter<>(sinkConfiguration);
        }
    }

    /**
     * Create a {@link PulsarSinkBuilder} to construct a new {@link PulsarSink}.
     *
     * @param <IN> Type of incoming records.
     * @return A Pulsar sink builder.
     */
    public static <IN> PulsarSinkBuilder<IN> builder() {
        return new PulsarSinkBuilder<>();
    }

    @Internal
    @Override
    public PrecommittingSinkWriter<IN, PulsarCommittable> createWriter(InitContext initContext) {
        return new PulsarWriter<>(
                sinkConfiguration,
                serializationSchema,
                metadataListener,
                topicRouter,
                messageDelayer,
                initContext);
    }

    @Internal
    @Override
    public Committer<PulsarCommittable> createCommitter() {
        return new PulsarCommitter(sinkConfiguration);
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<PulsarCommittable> getCommittableSerializer() {
        return new PulsarCommittableSerializer();
    }
}
