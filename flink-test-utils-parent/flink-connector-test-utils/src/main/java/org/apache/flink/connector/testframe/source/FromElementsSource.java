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
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testframe.source.enumerator.NoOpEnumState;
import org.apache.flink.connector.testframe.source.enumerator.NoOpEnumStateSerializer;
import org.apache.flink.connector.testframe.source.enumerator.NoOpEnumerator;
import org.apache.flink.connector.testframe.source.split.FromElementsSplit;
import org.apache.flink.connector.testframe.source.split.FromElementsSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * A {@link Source} implementation that reads data from a list and stops reading at the fixed
 * position. The source will wait until the checkpoint or savepoint triggered, the source is useful
 * for connector tests.
 *
 * <p>Note: This parallelism of source must be 1.
 */
public class FromElementsSource<OUT> implements Source<OUT, FromElementsSplit, NoOpEnumState> {
    private Boundedness boundedness;

    private List<OUT> elements;

    private Integer emittedElementsNum;

    public FromElementsSource(List<OUT> elements) {
        this.elements = elements;
    }

    public FromElementsSource(
            Boundedness boundedness, List<OUT> elements, Integer emittedElementsNum) {
        this(elements);
        if (emittedElementsNum != null) {
            Preconditions.checkState(
                    emittedElementsNum <= elements.size(),
                    String.format(
                            "The emittedElementsNum must not be larger than the elements list %d, but actual emittedElementsNum is %d",
                            elements.size(), emittedElementsNum));
        }
        this.boundedness = boundedness;
        this.emittedElementsNum = emittedElementsNum;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness == null ? Boundedness.BOUNDED : boundedness;
    }

    @Override
    public SourceReader<OUT, FromElementsSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new FromElementsSourceReader<>(
                emittedElementsNum, elements, boundedness, readerContext);
    }

    @Override
    public SplitEnumerator<FromElementsSplit, NoOpEnumState> createEnumerator(
            SplitEnumeratorContext<FromElementsSplit> enumContext) throws Exception {
        return new NoOpEnumerator();
    }

    @Override
    public SplitEnumerator<FromElementsSplit, NoOpEnumState> restoreEnumerator(
            SplitEnumeratorContext<FromElementsSplit> enumContext, NoOpEnumState checkpoint)
            throws Exception {
        return new NoOpEnumerator();
    }

    @Override
    public SimpleVersionedSerializer<FromElementsSplit> getSplitSerializer() {
        return new FromElementsSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<NoOpEnumState> getEnumeratorCheckpointSerializer() {
        return new NoOpEnumStateSerializer();
    }
}
