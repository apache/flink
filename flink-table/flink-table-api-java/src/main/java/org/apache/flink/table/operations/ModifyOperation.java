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
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.delegation.Planner;

import java.util.List;

/**
 * A {@link Operation} that describes the DML queries such as e.g. INSERT or conversion to a
 * DataStream.
 *
 * <p>A tree of {@link QueryOperation} with a {@link ModifyOperation} on top represents a runnable
 * query that can be transformed into a graph of {@link Transformation} via {@link
 * Planner#translate(List)}
 *
 * @see QueryOperation
 */
@Internal
public interface ModifyOperation extends Operation {
    QueryOperation getChild();

    <T> T accept(ModifyOperationVisitor<T> visitor);
}
