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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Arrays;

/**
 * Spec describes how the right table of search functions ser/des.
 *
 * <p>This class corresponds to {@link RelOptTable} rel node.
 */
public class VectorSearchTableSourceSpec {

    private final DynamicTableSourceSpec tableSourceSpec;
    private final RelDataType outputType;
    private final TableSourceTable searchTable;

    public VectorSearchTableSourceSpec(TableSourceTable searchTable) {
        this.searchTable = searchTable;
        this.outputType = searchTable.getRowType();
        this.tableSourceSpec =
                new DynamicTableSourceSpec(
                        searchTable.contextResolvedTable(),
                        Arrays.asList(searchTable.abilitySpecs()));
    }

    public TableSourceTable getSearchTable() {
        return searchTable;
    }

    public DynamicTableSourceSpec getTableSourceSpec() {
        return tableSourceSpec;
    }

    public RelDataType getOutputType() {
        return outputType;
    }
}
