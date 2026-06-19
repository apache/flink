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

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Table schema creation context. */
public class TableSchemaContext {
    public List<SqlNode> columnList = new ArrayList<>();
    public List<SqlTableConstraint> constraints = new ArrayList<>();
    @Nullable public SqlWatermark watermark;

    public boolean isColumnsIdentifiersOnly() {
        return !columnList.isEmpty() && columnList.get(0) instanceof SqlIdentifier;
    }

    /** Alter table context. */
    public static class AlterTableSchemaContext extends TableSchemaContext {
        public List<SqlNode> columnPositions = new ArrayList<>();
    }
}
