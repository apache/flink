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

package org.apache.flink.table.connector.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

/** Managed {@link Source} for testing. */
public class TestManagedSource implements Source<RowData, SourceSplit, Void> {
    private static final long serialVersionUID = 1L;

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, SourceSplit> createReader(SourceReaderContext readerContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SplitEnumerator<SourceSplit, Void> createEnumerator(
            SplitEnumeratorContext<SourceSplit> enumContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SplitEnumerator<SourceSplit, Void> restoreEnumerator(
            SplitEnumeratorContext<SourceSplit> enumContext, Void checkpoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SimpleVersionedSerializer<SourceSplit> getSplitSerializer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        // we don't need checkpoint under batch mode
        return null;
    }
}
