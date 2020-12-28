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
import org.apache.flink.table.api.internal.SelectResultProvider;

import java.util.Collections;
import java.util.HashMap;

/**
 * Special, internal kind of {@link ModifyOperation} that collects the content of {@link
 * QueryOperation} to local.
 */
@Internal
public class SelectSinkOperation implements ModifyOperation {

    private final QueryOperation child;
    // help the client to get the execute result from a specific sink.
    private SelectResultProvider resultProvider;

    public SelectSinkOperation(QueryOperation child) {
        this.child = child;
    }

    public void setSelectResultProvider(SelectResultProvider resultProvider) {
        this.resultProvider = resultProvider;
    }

    public SelectResultProvider getSelectResultProvider() {
        return resultProvider;
    }

    @Override
    public QueryOperation getChild() {
        return child;
    }

    @Override
    public <T> T accept(ModifyOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String asSummaryString() {
        return OperationUtils.formatWithChildren(
                "SelectSink",
                new HashMap<>(),
                Collections.singletonList(child),
                Operation::asSummaryString);
    }
}
