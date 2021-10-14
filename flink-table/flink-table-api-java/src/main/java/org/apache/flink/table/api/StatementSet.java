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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.DataType;

/**
 * A {@link StatementSet} accepts pipelines defined by DML statements or {@link Table} objects. The
 * planner can optimize all added statements together and then submit them as one job.
 *
 * <p>The added statements will be cleared when calling the {@link #execute()} method.
 */
@PublicEvolving
public interface StatementSet {

    /** Adds an {@code INSERT INTO} SQL statement. */
    StatementSet addInsertSql(String statement);

    /**
     * Adds a statement that the pipeline defined by the given {@link Table} object should be
     * written to a table (backed by a {@link DynamicTableSink}) that was registered under the
     * specified path.
     *
     * <p>See the documentation of {@link TableEnvironment#useDatabase(String)} or {@link
     * TableEnvironment#useCatalog(String)} for the rules on the path resolution.
     */
    StatementSet addInsert(String targetPath, Table table);

    /**
     * Adds a statement that the pipeline defined by the given {@link Table} object should be
     * written to a table (backed by a {@link DynamicTableSink}) that was registered under the
     * specified path.
     *
     * <p>See the documentation of {@link TableEnvironment#useDatabase(String)} or {@link
     * TableEnvironment#useCatalog(String)} for the rules on the path resolution.
     *
     * @param overwrite Indicates whether existing data should be overwritten.
     */
    StatementSet addInsert(String targetPath, Table table, boolean overwrite);

    /**
     * Adds a statement that the pipeline defined by the given {@link Table} object should be
     * written to a table (backed by a {@link DynamicTableSink}) expressed via the given {@link
     * TableDescriptor}.
     *
     * <p>The given {@link TableDescriptor descriptor} is registered as an inline (i.e. anonymous)
     * temporary catalog table (see {@link TableEnvironment#createTemporaryTable(String,
     * TableDescriptor)}. Then a statement is added to the statement set that inserts the {@link
     * Table} object's pipeline into that temporary table.
     *
     * <p>This method allows to declare a {@link Schema} for the sink descriptor. The declaration is
     * similar to a {@code CREATE TABLE} DDL in SQL and allows to:
     *
     * <ul>
     *   <li>overwrite automatically derived columns with a custom {@link DataType}
     *   <li>add metadata columns next to the physical columns
     *   <li>declare a primary key
     * </ul>
     *
     * <p>It is possible to declare a schema without physical/regular columns. In this case, those
     * columns will be automatically derived and implicitly put at the beginning of the schema
     * declaration.
     *
     * <p>Examples:
     *
     * <pre>{@code
     * StatementSet stmtSet = tEnv.createStatementSet();
     * Table sourceTable = tEnv.from("SourceTable");
     * TableDescriptor sinkDescriptor = TableDescriptor.forConnector("blackhole")
     *   .schema(Schema.newBuilder()
     *     // …
     *     .build())
     *   .build();
     *
     * stmtSet.addInsert(sinkDescriptor, sourceTable);
     * }</pre>
     */
    StatementSet addInsert(TableDescriptor targetDescriptor, Table table);

    /**
     * Adds a statement that the pipeline defined by the given {@link Table} object should be
     * written to a table (backed by a {@link DynamicTableSink}) expressed via the given {@link
     * TableDescriptor}.
     *
     * <p>The given {@link TableDescriptor descriptor} is registered as an inline (i.e. anonymous)
     * temporary catalog table (see {@link TableEnvironment#createTemporaryTable(String,
     * TableDescriptor)}. Then a statement is added to the statement set that inserts the {@link
     * Table} object's pipeline into that temporary table.
     *
     * <p>This method allows to declare a {@link Schema} for the sink descriptor. The declaration is
     * similar to a {@code CREATE TABLE} DDL in SQL and allows to:
     *
     * <ul>
     *   <li>overwrite automatically derived columns with a custom {@link DataType}
     *   <li>add metadata columns next to the physical columns
     *   <li>declare a primary key
     * </ul>
     *
     * <p>It is possible to declare a schema without physical/regular columns. In this case, those
     * columns will be automatically derived and implicitly put at the beginning of the schema
     * declaration.
     *
     * <p>Examples:
     *
     * <pre>{@code
     * StatementSet stmtSet = tEnv.createStatementSet();
     * Table sourceTable = tEnv.from("SourceTable");
     * TableDescriptor sinkDescriptor = TableDescriptor.forConnector("blackhole")
     *   .schema(Schema.newBuilder()
     *     // …
     *     .build())
     *   .build();
     *
     * stmtSet.addInsert(sinkDescriptor, sourceTable, true);
     * }</pre>
     *
     * @param overwrite Indicates whether existing data should be overwritten.
     */
    StatementSet addInsert(TableDescriptor targetDescriptor, Table table, boolean overwrite);

    /**
     * Returns the AST and the execution plan to compute the result of the all statements.
     *
     * @param extraDetails The extra explain details which the explain result should include, e.g.
     *     estimated cost, changelog mode for streaming, displaying execution plan in json format
     * @return AST and the execution plan.
     */
    String explain(ExplainDetail... extraDetails);

    /**
     * Executes all statements as one job.
     *
     * <p>The added statements will be cleared after calling this method.
     *
     * <p>By default, all DML operations are executed asynchronously. Use {@link
     * TableResult#await()} or {@link TableResult#getJobClient()} to monitor the execution. Set
     * {@link TableConfigOptions#TABLE_DML_SYNC} for always synchronous execution.
     */
    TableResult execute();
}
