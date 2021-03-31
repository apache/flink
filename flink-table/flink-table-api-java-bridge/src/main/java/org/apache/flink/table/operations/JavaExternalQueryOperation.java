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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes a relational operation that reads from a {@link DataStream}.
 *
 * <p>It contains all information necessary to perform a stream-to-table conversion.
 *
 * <p>This class needs to be kept in sync with {@code ScalaExternalQueryOperation} in the Scala
 * bridging module.
 *
 * @param <E> External type of data stream
 */
@Internal
public final class JavaExternalQueryOperation<E> implements QueryOperation {

    private final ObjectIdentifier identifier;

    private final DataStream<E> dataStream;

    private final DataType physicalDataType;

    private final boolean isTopLevelRecord;

    private final ChangelogMode changelogMode;

    private final ResolvedSchema resolvedSchema;

    public JavaExternalQueryOperation(
            ObjectIdentifier identifier,
            DataStream<E> dataStream,
            DataType physicalDataType,
            boolean isTopLevelRecord,
            ChangelogMode changelogMode,
            ResolvedSchema resolvedSchema) {
        this.identifier = identifier;
        this.dataStream = dataStream;
        this.physicalDataType = physicalDataType;
        this.isTopLevelRecord = isTopLevelRecord;
        this.changelogMode = changelogMode;
        this.resolvedSchema = resolvedSchema;
    }

    public ObjectIdentifier getIdentifier() {
        return identifier;
    }

    public DataStream<E> getDataStream() {
        return dataStream;
    }

    public DataType getPhysicalDataType() {
        return physicalDataType;
    }

    public boolean isTopLevelRecord() {
        return isTopLevelRecord;
    }

    public ChangelogMode getChangelogMode() {
        return changelogMode;
    }

    @Override
    public String asSummaryString() {
        final Map<String, Object> args = new LinkedHashMap<>();
        args.put("identifier", identifier);
        args.put("stream", dataStream.getId());
        args.put("type", physicalDataType);
        args.put("isTopLevelRecord", isTopLevelRecord);
        args.put("changelogMode", changelogMode);
        args.put("fields", resolvedSchema.getColumnNames());

        return OperationUtils.formatWithChildren(
                "DataStreamInput", args, getChildren(), Operation::asSummaryString);
    }

    @Override
    public List<QueryOperation> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public <T> T accept(QueryOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
