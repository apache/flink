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
import org.apache.flink.table.api.internal.CollectResultProvider;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Special, internal kind of {@link ModifyOperation} that collects the content of {@link
 * QueryOperation} to local.
 */
@Internal
public final class CollectModifyOperation implements ModifyOperation {

    private static final AtomicInteger uniqueId = new AtomicInteger(0);

    private final ObjectIdentifier tableIdentifier;

    private final QueryOperation child;

    // help the client to get the execute result from a specific sink.
    private CollectResultProvider resultProvider;

    public CollectModifyOperation(ObjectIdentifier tableIdentifier, QueryOperation child) {
        this.tableIdentifier = tableIdentifier;
        this.child = child;
    }

    public static int getUniqueId() {
        return uniqueId.incrementAndGet();
    }

    public ObjectIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public void setSelectResultProvider(CollectResultProvider resultProvider) {
        this.resultProvider = resultProvider;
    }

    public CollectResultProvider getSelectResultProvider() {
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
                "CollectSink",
                Collections.singletonMap("identifier", tableIdentifier),
                Collections.singletonList(child),
                Operation::asSummaryString);
    }
}
