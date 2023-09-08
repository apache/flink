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

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.util.NonReusingKeyGroupedIterator;
import org.apache.flink.runtime.util.ReusingKeyGroupedIterator;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A {@link SpillingThread.SpillingBehaviour} which spills & merges results of applying a {@link
 * GroupCombineFunction}.
 */
final class CombiningSpillingBehaviour<R> implements SpillingThread.SpillingBehaviour<R> {
    private static final Logger LOG = LoggerFactory.getLogger(CombiningSpillingBehaviour.class);
    private final GroupCombineFunction<R, R> combineFunction;
    private final TypeSerializer<R> serializer;
    private final TypeComparator<R> comparator;
    private final boolean objectReuseEnabled;
    private final Configuration udfConfig;

    CombiningSpillingBehaviour(
            GroupCombineFunction<R, R> combineFunction,
            TypeSerializer<R> serializer,
            TypeComparator<R> comparator,
            boolean objectReuseEnabled,
            Configuration udfConfig) {
        this.combineFunction = combineFunction;
        this.serializer = serializer;
        this.objectReuseEnabled = objectReuseEnabled;
        this.udfConfig = udfConfig;
        this.comparator = comparator;
    }

    @Override
    public void open() {
        try {
            FunctionUtils.openFunction(combineFunction, DefaultOpenContext.INSTANCE);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(
                    "The user-defined combiner failed in its 'open()' method.", t);
        }
    }

    @Override
    public void close() {
        try {
            FunctionUtils.closeFunction(combineFunction);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(
                    "The user-defined combiner failed in its 'close()' method.", t);
        }
    }

    @Override
    public void spillBuffer(
            CircularElement<R> element,
            ChannelWriterOutputView output,
            LargeRecordHandler<R> largeRecordHandler)
            throws IOException {
        // write sort-buffer to channel
        LOG.debug("Combining buffer {}.", element.getId());

        // set up the combining helpers
        final InMemorySorter<R> buffer = element.getBuffer();
        final CombineValueIterator<R> iter =
                new CombineValueIterator<>(
                        buffer, this.serializer.createInstance(), this.objectReuseEnabled);
        final WriterCollector<R> collector = new WriterCollector<>(output, this.serializer);

        int i = 0;
        int stop = buffer.size() - 1;

        try {
            while (i < stop) {
                int seqStart = i;
                while (i < stop && 0 == buffer.compare(i, i + 1)) {
                    i++;
                }

                if (i == seqStart) {
                    // no duplicate key, no need to combine. simply copy
                    buffer.writeToOutput(output, seqStart, 1);
                } else {
                    // get the iterator over the values
                    iter.set(seqStart, i);
                    // call the combiner to combine
                    combineFunction.combine(iter, collector);
                }
                i++;
            }
        } catch (Exception ex) {
            throw new IOException("An error occurred in the combiner user code.", ex);
        }

        // write the last pair, if it has not yet been included in the last iteration
        if (i == stop) {
            buffer.writeToOutput(output, stop, 1);
        }

        // done combining and writing out
        LOG.debug("Combined and spilled buffer {}.", element.getId());
    }

    @Override
    public void mergeRecords(MergeIterator<R> mergeIterator, ChannelWriterOutputView output)
            throws IOException {
        final WriterCollector<R> collector = new WriterCollector<>(output, this.serializer);

        // combine and write to disk
        try {
            if (objectReuseEnabled) {
                final ReusingKeyGroupedIterator<R> groupedIter =
                        new ReusingKeyGroupedIterator<>(
                                mergeIterator, this.serializer, this.comparator);
                while (groupedIter.nextKey()) {
                    combineFunction.combine(groupedIter.getValues(), collector);
                }
            } else {
                final NonReusingKeyGroupedIterator<R> groupedIter =
                        new NonReusingKeyGroupedIterator<>(mergeIterator, this.comparator);
                while (groupedIter.nextKey()) {
                    combineFunction.combine(groupedIter.getValues(), collector);
                }
            }
        } catch (Exception e) {
            throw new IOException("An error occurred in the combiner user code.");
        }
    }
}
