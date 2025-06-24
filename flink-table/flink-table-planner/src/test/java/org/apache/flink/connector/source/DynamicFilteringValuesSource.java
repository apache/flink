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
import org.apache.flink.connector.source.enumerator.DynamicFilteringValuesSourceEnumerator;
import org.apache.flink.connector.source.enumerator.NoOpEnumState;
import org.apache.flink.connector.source.enumerator.NoOpEnumStateSerializer;
import org.apache.flink.connector.source.split.ValuesSourcePartitionSplit;
import org.apache.flink.connector.source.split.ValuesSourcePartitionSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link Source} implementation that reads data from a partitioned list.
 *
 * <p>This source is useful for dynamic filtering testing.
 */
public class DynamicFilteringValuesSource
        implements Source<RowData, ValuesSourcePartitionSplit, NoOpEnumState> {

    private final TypeSerializer<RowData> serializer;
    private final TerminatingLogic terminatingLogic;
    private final Boundedness boundedness;
    private Map<Map<String, String>, byte[]> serializedElements;
    private Map<Map<String, String>, Integer> counts;
    private final List<String> dynamicFilteringFields;

    public DynamicFilteringValuesSource(
            TerminatingLogic terminatingLogic,
            Boundedness boundedness,
            Map<Map<String, String>, Collection<RowData>> elements,
            TypeSerializer<RowData> serializer,
            List<String> dynamicFilteringFields) {
        this.serializer = serializer;
        this.dynamicFilteringFields = dynamicFilteringFields;
        this.terminatingLogic = terminatingLogic;
        this.boundedness = boundedness;
        serializeElements(serializer, elements);
    }

    private void serializeElements(
            TypeSerializer<RowData> serializer,
            Map<Map<String, String>, Collection<RowData>> elements) {
        Preconditions.checkState(serializer != null, "serializer not set");

        serializedElements = new HashMap<>();
        counts = new HashMap<>();
        for (Map<String, String> partition : elements.keySet()) {
            Collection<RowData> collection = elements.get(partition);
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos)) {
                for (RowData e : collection) {
                    serializer.serialize(e, wrapper);
                }
                byte[] value = baos.toByteArray();
                serializedElements.put(partition, value);
            } catch (Exception e) {
                throw new TableException(
                        "Serializing the source elements failed: " + e.getMessage(), e);
            }
            counts.put(partition, collection.size());
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<RowData, ValuesSourcePartitionSplit> createReader(
            SourceReaderContext readerContext) throws Exception {
        return new DynamicFilteringValuesSourceReader(
                serializedElements, counts, serializer, readerContext);
    }

    @Override
    public SplitEnumerator<ValuesSourcePartitionSplit, NoOpEnumState> createEnumerator(
            SplitEnumeratorContext<ValuesSourcePartitionSplit> context) throws Exception {
        List<ValuesSourcePartitionSplit> splits =
                serializedElements.keySet().stream()
                        .map(ValuesSourcePartitionSplit::new)
                        .collect(Collectors.toList());
        return new DynamicFilteringValuesSourceEnumerator(
                context, terminatingLogic, splits, dynamicFilteringFields);
    }

    @Override
    public SplitEnumerator<ValuesSourcePartitionSplit, NoOpEnumState> restoreEnumerator(
            SplitEnumeratorContext<ValuesSourcePartitionSplit> context, NoOpEnumState checkpoint)
            throws Exception {
        return createEnumerator(context);
    }

    @Override
    public SimpleVersionedSerializer<ValuesSourcePartitionSplit> getSplitSerializer() {
        return new ValuesSourcePartitionSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<NoOpEnumState> getEnumeratorCheckpointSerializer() {
        return new NoOpEnumStateSerializer();
    }
}
