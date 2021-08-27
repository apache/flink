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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.AbstractEvent;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The specific delegate implementation for the single output case. */
public class SingleRecordWriter<T extends IOReadableWritable> implements RecordWriterDelegate<T> {

    private final RecordWriter<T> recordWriter;

    public SingleRecordWriter(RecordWriter<T> recordWriter) {
        this.recordWriter = checkNotNull(recordWriter);
    }

    @Override
    public void broadcastEvent(AbstractEvent event) throws IOException {
        recordWriter.broadcastEvent(event);
    }

    @Override
    public RecordWriter<T> getRecordWriter(int outputIndex) {
        checkArgument(
                outputIndex == 0,
                "The index should always be 0 for the single record writer delegate.");

        return recordWriter;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return recordWriter.getAvailableFuture();
    }

    @Override
    public boolean isAvailable() {
        return recordWriter.isAvailable();
    }

    @Override
    public void close() {
        recordWriter.close();
    }
}
