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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** Internal operation used to convert a {@link Table} into a DataStream. */
@Internal
public final class ExternalModifyOperation implements ModifyOperation {

    private final ContextResolvedTable contextResolvedTable;
    private final QueryOperation child;

    /** Null if changelog mode is derived from input. */
    private final @Nullable ChangelogMode changelogMode;

    private final DataType physicalDataType;

    public ExternalModifyOperation(
            ContextResolvedTable contextResolvedTable,
            QueryOperation child,
            @Nullable ChangelogMode changelogMode,
            DataType physicalDataType) {
        this.contextResolvedTable = contextResolvedTable;
        this.child = child;
        this.changelogMode = changelogMode;
        this.physicalDataType = physicalDataType;
    }

    public ContextResolvedTable getContextResolvedTable() {
        return contextResolvedTable;
    }

    @Override
    public QueryOperation getChild() {
        return child;
    }

    public DataType getPhysicalDataType() {
        return physicalDataType;
    }

    public Optional<ChangelogMode> getChangelogMode() {
        return Optional.ofNullable(changelogMode);
    }

    @Override
    public <T> T accept(ModifyOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String asSummaryString() {
        final Map<String, Object> args = new LinkedHashMap<>();
        args.put("identifier", getContextResolvedTable().getIdentifier().asSummaryString());
        args.put("changelogMode", changelogMode);
        args.put("type", physicalDataType);

        return OperationUtils.formatWithChildren(
                "DataStreamOutput",
                args,
                Collections.singletonList(child),
                Operation::asSummaryString);
    }
}
