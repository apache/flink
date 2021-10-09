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

package org.apache.flink.table.api.bridge.java;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;

/**
 * A {@link StatementSet} that integrates with the Java-specific {@link DataStream} API.
 *
 * <p>It accepts pipelines defined by DML statements or {@link Table} objects. The planner can
 * optimize all added statements together and then either submit them as one job or attach them to
 * the underlying {@link StreamExecutionEnvironment}.
 *
 * <p>The added statements will be cleared when calling the {@link #execute()} or {@link
 * #attachAsDataStream()} method.
 */
@PublicEvolving
public interface StreamStatementSet extends StatementSet {

    @Override
    StreamStatementSet addInsertSql(String statement);

    @Override
    StreamStatementSet addInsert(String targetPath, Table table);

    @Override
    StreamStatementSet addInsert(String targetPath, Table table, boolean overwrite);

    @Override
    StreamStatementSet addInsert(TableDescriptor targetDescriptor, Table table);

    @Override
    StreamStatementSet addInsert(TableDescriptor targetDescriptor, Table table, boolean overwrite);

    /**
     * Optimizes all statements as one entity and adds them as transformations to the underlying
     * {@link StreamExecutionEnvironment}.
     *
     * <p>Use {@link StreamExecutionEnvironment#execute()} to execute them.
     *
     * <p>The added statements will be cleared after calling this method.
     */
    void attachAsDataStream();
}
