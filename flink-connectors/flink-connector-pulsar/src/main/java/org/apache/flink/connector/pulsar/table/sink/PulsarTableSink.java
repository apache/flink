/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.table.sink;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.PulsarSinkBuilder;
import org.apache.flink.connector.pulsar.sink.writer.delayer.FixedMessageDelayer;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Pulsar SQL Connector sink. It supports {@link SupportsWritingMetadata}. */
public class PulsarTableSink implements DynamicTableSink, SupportsWritingMetadata {

    private final PulsarTableSerializationSchemaFactory serializationSchemaFactory;

    private final ChangelogMode changelogMode;

    private final List<String> topics;

    private final Properties properties;

    private final DeliveryGuarantee deliveryGuarantee;

    @Nullable private final TopicRouter<RowData> topicRouter;

    private final TopicRoutingMode topicRoutingMode;

    private final long messageDelayMillis;

    public PulsarTableSink(
            PulsarTableSerializationSchemaFactory serializationSchemaFactory,
            ChangelogMode changelogMode,
            List<String> topics,
            Properties properties,
            DeliveryGuarantee deliveryGuarantee,
            @Nullable TopicRouter<RowData> topicRouter,
            TopicRoutingMode topicRoutingMode,
            long messageDelayMillis) {
        this.serializationSchemaFactory = checkNotNull(serializationSchemaFactory);
        this.changelogMode = checkNotNull(changelogMode);
        this.topics = checkNotNull(topics);
        // Mutable attributes
        this.properties = checkNotNull(properties);
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee);
        this.topicRouter = topicRouter;
        this.topicRoutingMode = checkNotNull(topicRoutingMode);
        this.messageDelayMillis = messageDelayMillis;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return this.changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

        final PulsarSerializationSchema<RowData> pulsarSerializationSchema =
                serializationSchemaFactory.createPulsarSerializationSchema(context);

        final PulsarSinkBuilder<RowData> pulsarSinkBuilder =
                PulsarSink.builder()
                        .setSerializationSchema(pulsarSerializationSchema)
                        .setProperties(properties)
                        .setDeliveryGuarantee(deliveryGuarantee)
                        .setTopics(topics)
                        .setTopicRoutingMode(topicRoutingMode)
                        .delaySendingMessage(new FixedMessageDelayer<>(messageDelayMillis));

        if (topicRouter != null) {
            pulsarSinkBuilder.setTopicRouter(topicRouter);
        }
        return SinkV2Provider.of(pulsarSinkBuilder.build());
    }

    @Override
    public String asSummaryString() {
        return "Pulsar dynamic table sink";
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(PulsarWritableMetadata.WritableMetadata.values())
                .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
        return metadataMap;
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.serializationSchemaFactory.setWritableMetadataKeys(metadataKeys);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PulsarTableSink that = (PulsarTableSink) o;
        return Objects.equals(serializationSchemaFactory, that.serializationSchemaFactory)
                && Objects.equals(changelogMode, that.changelogMode)
                && Objects.equals(topics, that.topics)
                && Objects.equals(properties, that.properties)
                && deliveryGuarantee == that.deliveryGuarantee
                && Objects.equals(topicRouter, that.topicRouter)
                && topicRoutingMode == that.topicRoutingMode
                && messageDelayMillis == that.messageDelayMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                serializationSchemaFactory,
                changelogMode,
                topics,
                properties,
                deliveryGuarantee,
                topicRouter,
                topicRoutingMode,
                messageDelayMillis);
    }

    @Override
    public DynamicTableSink copy() {
        final PulsarTableSink copy =
                new PulsarTableSink(
                        serializationSchemaFactory,
                        changelogMode,
                        topics,
                        properties,
                        deliveryGuarantee,
                        topicRouter,
                        topicRoutingMode,
                        messageDelayMillis);
        return copy;
    }
}
