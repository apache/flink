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

package org.apache.flink.connector.hbase.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.source.enumerator.HBaseSourceEnumeratorCheckpointSerializer;
import org.apache.flink.connector.hbase.source.enumerator.HBaseSplitEnumerator;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceDeserializer;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceReader;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplit;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplitSerializer;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * A source connector for HBase. Please use an {@link HBaseSourceBuilder} to construct an {@link
 * HBaseSource}. The following example shows how to create a HBaseSource that reads String values
 * from each cell.
 *
 * <pre>{@code
 * HBaseSource<String> source =
 *     HBaseSource.builder()
 *         .setSourceDeserializer(new HBaseStringDeserializer())
 *         .setTableName("test-table")
 *         .setHBaseConfiguration(new HBaseTestClusterUtil().getConfig())
 *         .build();
 *
 * static class HBaseStringDeserializer implements HBaseSourceDeserializer<String> {
 *     @Override
 *     public String deserialize(HBaseSourceEvent event) {
 *         return new String(event.getPayload(), HBaseEvent.DEFAULT_CHARSET);
 *     }
 * }
 * }</pre>
 *
 * @see HBaseSourceBuilder HBaseSourceBuilder for more details.
 */
public class HBaseSource<T> implements Source<T, HBaseSourceSplit, Collection<HBaseSourceSplit>> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSource.class);

    private static final long serialVersionUID = 1L;

    private final HBaseSourceDeserializer<T> sourceDeserializer;
    private final byte[] serializedHBaseConfig;
    private final Configuration sourceConfiguration;

    HBaseSource(
            HBaseSourceDeserializer<T> sourceDeserializer,
            org.apache.hadoop.conf.Configuration hbaseConfiguration,
            Configuration sourceConfiguration) {
        this.serializedHBaseConfig =
                HBaseConfigurationUtil.serializeConfiguration(hbaseConfiguration);
        this.sourceDeserializer = sourceDeserializer;
        this.sourceConfiguration = sourceConfiguration;
    }

    public static <IN> HBaseSourceBuilder<IN> builder() {
        return new HBaseSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, HBaseSourceSplit> createReader(SourceReaderContext readerContext) {
        LOG.debug("creating reader");
        return new HBaseSourceReader<>(
                serializedHBaseConfig, sourceDeserializer, sourceConfiguration, readerContext);
    }

    @Override
    public SplitEnumerator<HBaseSourceSplit, Collection<HBaseSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<HBaseSourceSplit> enumContext,
            Collection<HBaseSourceSplit> checkpoint) {
        LOG.debug("restoring enumerator with {} splits", checkpoint.size());

        HBaseSplitEnumerator enumerator =
                new HBaseSplitEnumerator(enumContext, serializedHBaseConfig, sourceConfiguration);
        enumerator.addSplits(checkpoint);
        return enumerator;
    }

    @Override
    public SplitEnumerator<HBaseSourceSplit, Collection<HBaseSourceSplit>> createEnumerator(
            SplitEnumeratorContext<HBaseSourceSplit> enumContext) {
        LOG.debug("creating enumerator");
        return new HBaseSplitEnumerator(enumContext, serializedHBaseConfig, sourceConfiguration);
    }

    @Override
    public SimpleVersionedSerializer<HBaseSourceSplit> getSplitSerializer() {
        LOG.debug("getSplitSerializer");
        return new HBaseSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<HBaseSourceSplit>>
            getEnumeratorCheckpointSerializer() {
        LOG.debug("getEnumeratorCheckpointSerializer");
        return new HBaseSourceEnumeratorCheckpointSerializer();
    }
}
