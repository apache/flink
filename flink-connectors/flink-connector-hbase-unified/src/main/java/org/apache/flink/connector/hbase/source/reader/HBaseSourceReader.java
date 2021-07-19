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

package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplit;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplitState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** The source reader for Hbase. */
@Internal
public class HBaseSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                HBaseSourceEvent, T, HBaseSourceSplit, HBaseSourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSourceReader.class);

    public HBaseSourceReader(
            byte[] serializedHBaseConfig,
            HBaseSourceDeserializer<T> sourceDeserializer,
            Configuration sourceConfiguration,
            SourceReaderContext context) {
        super(
                () -> new HBaseSourceSplitReader(serializedHBaseConfig, sourceConfiguration),
                new HBaseRecordEmitter<>(sourceDeserializer),
                sourceConfiguration,
                context);
        LOG.debug("constructing Source Reader with config: {}", sourceConfiguration.toString());
    }

    @Override
    protected void onSplitFinished(Map<String, HBaseSourceSplitState> finishedSplitIds) {
        LOG.debug("finished {} split(s)", finishedSplitIds.entrySet().size());
        context.sendSplitRequest();
    }

    @Override
    protected HBaseSourceSplitState initializedState(HBaseSourceSplit split) {
        LOG.debug("initialized state for split {}", split.splitId());
        return new HBaseSourceSplitState(split);
    }

    @Override
    protected HBaseSourceSplit toSplitType(String splitId, HBaseSourceSplitState splitState) {
        return splitState.toSplit();
    }
}
