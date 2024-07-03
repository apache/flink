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

package org.apache.flink.connector.source;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.source.enumerator.NoOpEnumState;
import org.apache.flink.connector.source.enumerator.NoOpEnumStateSerializer;
import org.apache.flink.connector.source.enumerator.ValuesSourceEnumerator;
import org.apache.flink.connector.source.split.ValuesSourceSplit;
import org.apache.flink.connector.source.split.ValuesSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A {@link Source} implementation that reads data from a list.
 *
 * <p>The source is useful for FLIP-27 source tests.
 *
 * <p>{@code FromElementsSource} requires the elements must be serializable, and the parallelism
 * must be 1. RowData is not serializable and the parallelism of table source may not be 1, so we
 * introduce a new source for testing in table module.
 */
public class ValuesSource implements Source<RowData, ValuesSourceSplit, NoOpEnumState> {
    private final TypeSerializer<RowData> serializer;

    private final List<byte[]> serializedElements;

    private final TerminatingLogic terminatingLogic;
    private final Boundedness boundedness;

    public ValuesSource(
            TerminatingLogic terminatingLogic,
            Boundedness boundedness,
            Collection<RowData> elements,
            TypeSerializer<RowData> serializer) {
        Preconditions.checkState(serializer != null, "serializer not set");
        this.serializedElements = serializeElements(elements, serializer);
        this.serializer = serializer;
        this.terminatingLogic = terminatingLogic;
        this.boundedness = boundedness;
    }

    private List<byte[]> serializeElements(
            Collection<RowData> elements, TypeSerializer<RowData> serializer) {
        List<byte[]> serializeElements = new ArrayList<>();

        for (RowData element : elements) {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos)) {
                serializer.serialize(element, wrapper);
                serializeElements.add(baos.toByteArray());
            } catch (Exception e) {
                throw new TableException(
                        "Serializing the source elements failed: " + e.getMessage(), e);
            }
        }
        return serializeElements;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<RowData, ValuesSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new ValuesSourceReader(serializedElements, serializer, readerContext);
    }

    @Override
    public SplitEnumerator<ValuesSourceSplit, NoOpEnumState> createEnumerator(
            SplitEnumeratorContext<ValuesSourceSplit> enumContext) {
        List<ValuesSourceSplit> splits =
                IntStream.range(0, serializedElements.size())
                        .mapToObj(ValuesSourceSplit::new)
                        .collect(Collectors.toList());
        return new ValuesSourceEnumerator(enumContext, splits, terminatingLogic);
    }

    @Override
    public SplitEnumerator<ValuesSourceSplit, NoOpEnumState> restoreEnumerator(
            SplitEnumeratorContext<ValuesSourceSplit> enumContext, NoOpEnumState checkpoint)
            throws Exception {
        return createEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<ValuesSourceSplit> getSplitSerializer() {
        return new ValuesSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<NoOpEnumState> getEnumeratorCheckpointSerializer() {
        return new NoOpEnumStateSerializer();
    }
}
