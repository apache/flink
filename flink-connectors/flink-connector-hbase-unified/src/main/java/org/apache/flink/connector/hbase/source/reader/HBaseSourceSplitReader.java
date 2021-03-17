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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.hbase.source.hbaseendpoint.HBaseEndpoint;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplit;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/** A {@link SplitReader} implementation for HBase. */
@Internal
public class HBaseSourceSplitReader implements SplitReader<HBaseSourceEvent, HBaseSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSourceSplitReader.class);

    private final Queue<HBaseSourceSplit> splits;
    private final HBaseEndpoint hbaseEndpoint;

    @Nullable private String currentSplitId;

    public HBaseSourceSplitReader(byte[] serializedConfig, Configuration sourceConfiguration) {
        LOG.debug("constructing Split Reader");
        try {
            this.hbaseEndpoint =
                    new HBaseEndpoint(
                            HBaseConfigurationUtil.deserializeConfiguration(serializedConfig, null),
                            sourceConfiguration);
        } catch (Exception e) {
            throw new RuntimeException("failed HBase consumer", e);
        }
        this.splits = new ArrayDeque<>();
    }

    @Override
    public RecordsWithSplitIds<HBaseSourceEvent> fetch() throws IOException {
        if (currentSplitId == null) {
            HBaseSourceSplit nextSplit = splits.poll();
            if (nextSplit != null) {
                currentSplitId = nextSplit.splitId();
            } else {
                throw new IOException("No split remaining");
            }
        }
        List<HBaseSourceEvent> records = hbaseEndpoint.getAll();
        LOG.debug("{} records in the queue", records.size());
        return new HBaseSplitRecords<>(currentSplitId, records.iterator());
    }

    @Override
    public void handleSplitsChanges(SplitsChange<HBaseSourceSplit> splitsChanges) {
        LOG.debug("handle splits change {}", splitsChanges);
        if (splitsChanges instanceof SplitsAddition) {
            HBaseSourceSplit split = splitsChanges.splits().get(0);
            try {
                this.hbaseEndpoint.startReplication(split.getColumnFamilies());
            } catch (Exception e) {
                throw new RuntimeException("failed HBase consumer", e);
            }
            splits.addAll(splitsChanges.splits());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported splits change type "
                            + splitsChanges.getClass().getSimpleName()
                            + " in "
                            + this.getClass().getSimpleName());
        }
    }

    @Override
    public void wakeUp() {
        LOG.debug("waking up HBaseEndpoint");
        hbaseEndpoint.wakeup();
    }

    @Override
    public void close() throws Exception {
        hbaseEndpoint.close();
    }

    private static class HBaseSplitRecords<T> implements RecordsWithSplitIds<T> {
        private Iterator<T> recordsForSplit;
        private String splitId;

        HBaseSplitRecords(String splitId, Iterator<T> recordsForSplit) {
            this.splitId = splitId;
            this.recordsForSplit = recordsForSplit;
        }

        @Nullable
        @Override
        public String nextSplit() {
            final String nextSplit = this.splitId;
            this.splitId = null;
            this.recordsForSplit = nextSplit != null ? this.recordsForSplit : null;

            return nextSplit;
        }

        @Nullable
        @Override
        public T nextRecordFromSplit() {
            if (recordsForSplit != null && recordsForSplit.hasNext()) {
                return recordsForSplit.next();
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return Collections.emptySet();
        }
    }
}
