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

package org.apache.flink.connector.testframe.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.testframe.source.split.FromElementsSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.core.io.InputStatus.MORE_AVAILABLE;

/**
 * A {@link SourceReader} implementation that reads data from a list. If limitedNum is set, the
 * reader will stop reading at the limitedNum position until the checkpoint or savepoint triggered.
 */
public class FromElementsSourceReader<T> implements SourceReader<T, FromElementsSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(FromElementsSourceReader.class);

    private volatile int emittedNum;
    private volatile boolean isRunning = true;

    /** The context of this source reader. */
    private SourceReaderContext context;

    private Integer limitedNum;
    private Boundedness boundedness;
    private volatile boolean checkpointAtLimitedNum = false;
    private List<T> elements;
    private Counter numRecordInCounter;

    public FromElementsSourceReader(
            Integer limitedNum,
            List<T> elements,
            Boundedness boundedness,
            SourceReaderContext context) {
        this.context = context;
        this.emittedNum = 0;
        this.elements = elements;
        this.limitedNum = limitedNum;
        this.boundedness = boundedness;
        this.numRecordInCounter = context.metricGroup().getIOMetricGroup().getNumRecordsInCounter();
    }

    @Override
    public void start() {}

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        if (isRunning && emittedNum < elements.size()) {
            /*
             * The reader will stop reading when it has emitted `successNum` records.
             * If and only if a checkpoint whose `numElementsEmitted` is equal to `successNum`
             * is completed, the reader will continue reading.
             *
             * When we disable the checkpointing and stop with a savepoint after
             * receiving `successNum` records, the job starting with the savepoint
             * will continue to read the records after the `successNum` records.
             */
            if (limitedNum == null
                    || (limitedNum != null
                            && (emittedNum < limitedNum || checkpointAtLimitedNum))) {
                output.collect(elements.get(emittedNum));
                emittedNum++;
                numRecordInCounter.inc();
            }
            return MORE_AVAILABLE;
        }

        if (Boundedness.CONTINUOUS_UNBOUNDED.equals(boundedness)) {
            return MORE_AVAILABLE;
        } else {
            return InputStatus.END_OF_INPUT;
        }
    }

    @Override
    public List<FromElementsSplit> snapshotState(long checkpointId) {
        if (limitedNum != null && !checkpointAtLimitedNum && emittedNum == limitedNum) {
            checkpointAtLimitedNum = true;
            LOG.info("checkpoint {} is the target checkpoint to be used.", checkpointId);
        }
        return Arrays.asList(new FromElementsSplit(emittedNum));
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<FromElementsSplit> splits) {
        emittedNum = splits.get(0).getEmitNum();
        LOG.info("FromElementsSourceReader restores from {}.", emittedNum);
    }

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public void close() throws Exception {
        isRunning = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.info("checkpoint {} finished.", checkpointId);
    }
}
