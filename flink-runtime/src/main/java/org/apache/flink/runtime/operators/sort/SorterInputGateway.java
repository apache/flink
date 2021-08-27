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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.runtime.operators.sort.StageRunner.SortStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A gateway for writing records into the sort/merge process. */
final class SorterInputGateway<E> {
    /** Logging. */
    private static final Logger LOG = LoggerFactory.getLogger(SorterInputGateway.class);

    private final LargeRecordHandler<E> largeRecords;
    /** The object into which the thread reads the data from the input. */
    private final StageRunner.StageMessageDispatcher<E> dispatcher;

    private long bytesUntilSpilling;
    private CircularElement<E> currentBuffer;

    /**
     * Creates a new gateway for pushing records into the sorter.
     *
     * @param dispatcher The queues used to pass buffers between the threads.
     */
    SorterInputGateway(
            StageRunner.StageMessageDispatcher<E> dispatcher,
            @Nullable LargeRecordHandler<E> largeRecordsHandler,
            long startSpillingBytes) {

        // members
        this.bytesUntilSpilling = startSpillingBytes;
        this.largeRecords = largeRecordsHandler;
        this.dispatcher = checkNotNull(dispatcher);

        if (bytesUntilSpilling < 1) {
            this.dispatcher.send(SortStage.SORT, CircularElement.spillingMarker());
        }
    }

    /** Writes the given record for sorting. */
    public void writeRecord(E record) throws IOException, InterruptedException {

        if (currentBuffer == null) {
            this.currentBuffer = this.dispatcher.take(SortStage.READ);
            if (!currentBuffer.getBuffer().isEmpty()) {
                throw new IOException("New buffer is not empty.");
            }
        }

        InMemorySorter<E> sorter = currentBuffer.getBuffer();

        long occupancyPreWrite = sorter.getOccupancy();
        if (!sorter.write(record)) {
            long recordSize = sorter.getCapacity() - occupancyPreWrite;
            signalSpillingIfNecessary(recordSize);
            boolean isLarge = occupancyPreWrite == 0;
            if (isLarge) {
                // did not fit in a fresh buffer, must be large...
                writeLarge(record, sorter);
                this.currentBuffer.getBuffer().reset();
            } else {
                this.dispatcher.send(SortStage.SORT, currentBuffer);
                this.currentBuffer = null;
                writeRecord(record);
            }
        } else {
            long recordSize = sorter.getOccupancy() - occupancyPreWrite;
            signalSpillingIfNecessary(recordSize);
        }
    }

    /** Signals the end of input. Will flush all buffers and notify later stages. */
    public void finishReading() {

        if (currentBuffer != null && !currentBuffer.getBuffer().isEmpty()) {
            this.dispatcher.send(SortStage.SORT, currentBuffer);
        }

        // add the sentinel to notify the receivers that the work is done
        // send the EOF marker
        final CircularElement<E> EOF_MARKER = CircularElement.endMarker();
        this.dispatcher.send(SortStage.SORT, EOF_MARKER);
        LOG.debug("Reading thread done.");
    }

    private void writeLarge(E record, InMemorySorter<E> sorter) throws IOException {
        if (this.largeRecords != null) {
            LOG.debug(
                    "Large record did not fit into a fresh sort buffer. Putting into large record store.");
            this.largeRecords.addRecord(record);
        } else {
            throw new IOException(
                    "The record exceeds the maximum size of a sort buffer (current maximum: "
                            + sorter.getCapacity()
                            + " bytes).");
        }
    }

    private void signalSpillingIfNecessary(long writtenSize) {
        if (bytesUntilSpilling <= 0) {
            return;
        }

        bytesUntilSpilling -= writtenSize;
        if (bytesUntilSpilling < 1) {
            // add the spilling marker
            this.dispatcher.send(SortStage.SORT, CircularElement.spillingMarker());
            bytesUntilSpilling = 0;
        }
    }
}
