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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Internal operation used to convert a {@link org.apache.flink.table.api.Table} into a {@link
 * org.apache.flink.streaming.api.datastream.DataStream}.
 */
@Internal
public class ExternalModifyOperation implements ModifyOperation {

    private final QueryOperation child;
    private final DataType type;
    private final ChangelogMode changelogMode;

    public ExternalModifyOperation(
            QueryOperation child, DataType type, ChangelogMode changelogMode) {
        this.child = child;
        this.type = type;
        this.changelogMode = changelogMode;
    }

    public DataType getType() {
        return type;
    }

    public ChangelogMode getChangelogMode() {
        return changelogMode;
    }

    @Override
    public QueryOperation getChild() {
        return child;
    }

    @Override
    public <R> R accept(ModifyOperationVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("type", type);

        return OperationUtils.formatWithChildren(
                "Output", params, Collections.singletonList(child), Operation::asSummaryString);
    }
}
