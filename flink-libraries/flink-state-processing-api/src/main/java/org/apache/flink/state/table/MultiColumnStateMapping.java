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

package org.apache.flink.state.table;

import org.apache.flink.annotation.Internal;

import java.util.List;

/**
 * Mixed into mapping classes that back a table with an arbitrary number of value columns, each
 * registered as its own state, driven from {@link #getAllValueColumns()}. Implemented by {@link
 * StateTableMapping} and {@link WindowStateTableMapping}.
 */
@Internal
interface MultiColumnStateMapping extends SavepointStateMapping {

    /**
     * Full original value columns required for state-descriptor registration / key(-and-namespace)
     * enumeration, preserved across projections. Never empty when the source table has at least one
     * state column.
     */
    List<StateValueColumnConfiguration> getAllValueColumns();

    /**
     * Creates a new mapping of the same concrete type with column indices remapped to the projected
     * output. Declared here (rather than per concrete class) so that {@link
     * SavepointDynamicTableSource} can apply projection push-down generically.
     */
    MultiColumnStateMapping project(int[][] projectedFields);
}
