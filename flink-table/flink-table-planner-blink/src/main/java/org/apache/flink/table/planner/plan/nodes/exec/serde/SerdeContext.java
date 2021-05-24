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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlOperatorTable;

/**
 * A context to allow the store user-defined data within ExecNode serialization and deserialization.
 */
public class SerdeContext {
    private final ClassLoader classLoader;
    private final FlinkContext flinkContext;
    private final FlinkTypeFactory typeFactory;
    private final SqlOperatorTable operatorTable;
    private final RexBuilder rexBuilder;

    public SerdeContext(
            FlinkContext flinkContext,
            ClassLoader classLoader,
            FlinkTypeFactory typeFactory,
            SqlOperatorTable operatorTable) {
        this.classLoader = classLoader;
        this.flinkContext = flinkContext;
        this.typeFactory = typeFactory;
        this.operatorTable = operatorTable;
        this.rexBuilder = new RexBuilder(typeFactory);
    }

    public Configuration getConfiguration() {
        return flinkContext.getTableConfig().getConfiguration();
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public FlinkContext getFlinkContext() {
        return flinkContext;
    }

    public FlinkTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public SqlOperatorTable getOperatorTable() {
        return operatorTable;
    }

    public RexBuilder getRexBuilder() {
        return rexBuilder;
    }
}
