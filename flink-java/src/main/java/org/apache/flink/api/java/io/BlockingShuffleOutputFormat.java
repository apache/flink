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

package org.apache.flink.api.java.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * This is the inner OutputFormat used for specify the BLOCKING_PERSISTENT result partition type of
 * coming edge.
 *
 * @param <T>
 */
@Internal
public final class BlockingShuffleOutputFormat<T> implements OutputFormat<T> {

    private static final long serialVersionUID = 1L;

    private final AbstractID intermediateDataSetId;

    private BlockingShuffleOutputFormat(AbstractID intermediateDataSetId) {
        this.intermediateDataSetId = intermediateDataSetId;
    }

    public static <T> BlockingShuffleOutputFormat<T> createOutputFormat(
            AbstractID intermediateDataSetId) {
        return new BlockingShuffleOutputFormat<>(
                Preconditions.checkNotNull(intermediateDataSetId, "intermediateDataSetId is null"));
    }

    @Override
    public void configure(Configuration parameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeRecord(T record) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException();
    }

    public AbstractID getIntermediateDataSetId() {
        return intermediateDataSetId;
    }
}
