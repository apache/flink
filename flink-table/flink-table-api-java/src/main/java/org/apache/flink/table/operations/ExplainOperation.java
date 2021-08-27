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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/** Operation to describe an EXPLAIN statement. */
public class ExplainOperation implements Operation {
    private final Operation child;
    private final Set<String> explainDetails;

    public ExplainOperation(Operation child) {
        this(child, new HashSet<>());
    }

    public ExplainOperation(Operation child, Set<String> explainDetails) {
        this.child = child;
        this.explainDetails = explainDetails;
    }

    public Operation getChild() {
        return child;
    }

    @Override
    public String asSummaryString() {
        String operationName = "EXPLAIN";
        if (!explainDetails.isEmpty()) {
            operationName =
                    String.format(
                            "EXPLAIN %s",
                            explainDetails.stream()
                                    .map(String::toUpperCase)
                                    .collect(Collectors.joining(", ")));
        }
        return OperationUtils.formatWithChildren(
                operationName,
                Collections.emptyMap(),
                Collections.singletonList(child),
                Operation::asSummaryString);
    }

    public Set<String> getExplainDetails() {
        return explainDetails;
    }
}
