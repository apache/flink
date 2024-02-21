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

package org.apache.flink.table.procedures;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;

/**
 * Base interface representing a stored procedure that can be executed by Flink. An stored procedure
 * accepts zero, one, or multiple input parameters and then return the execution result of the
 * stored procedure.
 *
 * <p>The behavior of {@link Procedure} can be defined by implements a custom call method. An call
 * method must be declared publicly, not static, and named <code>call</code>. Call methods can also
 * be overloaded by implementing multiple methods named <code>call</code>. Currently, it doesn't
 * allow users to custom their own procedure, the customer {@link Procedure} can only be provided by
 * {@link Catalog}. To provide {@link Procedure}, {@link Catalog} must implement {@link
 * Catalog#getProcedure(ObjectPath)}.
 *
 * <p>When calling a stored procedure, Flink will always pass the <code>
 * org.apache.flink.table.procedure.ProcedureContext</code> which provides
 * StreamExecutionEnvironment currently as the first parameter of the <code>call</code> method. So,
 * the custom <code>call</code> method must accept the <code>
 * org.apache.flink.table.procedure.ProcedureContext
 * </code> as the first parameter, and the other parameters of the <code>call</code> method are the
 * actual parameter of the stored procedure.
 *
 * <p>By default, input and output data types are automatically extracted using reflection. The
 * input arguments are derived from one or more {@code call()} methods. If the reflective
 * information is not sufficient, it can be supported and enriched with {@link DataTypeHint} and
 * {@link ProcedureHint}. If {@link ProcedureHint} is used to hint input arguments, it should only
 * hint the input arguments that start from the second argument since the first argument is always
 * <code>ProcedureContext</code> which doesn't need to be annotated with data type hint.
 *
 * <p>Note: The return type of the {@code call()} method should always be T[] where T can be an
 * atomic type, Row, Pojo. An atomic type will be implicitly wrapped into a row consisting of one
 * field. Also, the {@link DataTypeHint} for output data type is used to hint T.
 *
 * <p>The following examples with pseudocode show how to write a stored procedure:
 *
 * <pre>{@code
 * // a stored procedure that tries to rewrite data files for iceberg, it accept STRING
 * // and return an array of explicit ROW < STRING, STRING >.
 * class IcebergRewriteDataFilesProcedure implements Procedure {
 *   public @DataTypeHint("ROW< rewritten_data_files_count STRING, added_data_files_count STRING >")
 *          Row[] call(ProcedureContext procedureContext, String tableName) {
 *     // plan for scanning the table to do rewriting
 *     Table table = loadTable(tableName);
 *     List<CombinedScanTask> combinedScanTasks = planScanTask(table);
 *
 *     // now, rewrite the files according to the planning task
 *     StreamExecutionEnvironment env = procedureContext.getExecutionEnvironment();
 *     DataStream<CombinedScanTask> dataStream = env.fromCollection(combinedScanTasks);
 *     RowDataRewriter rowDataRewriter =
 *         new RowDataRewriter(table(), caseSensitive(), fileIO(), encryptionManager());
 *     List<DataFile> addedDataFiles；
 *     try {
 *       addedDataFiles = rowDataRewriter.rewriteDataForTasks(dataStream, parallelism);
 *     } catch (Exception e) {
 *       throw new RuntimeException("Rewrite data file error.", e);
 *     }
 *
 *     // replace the current files
 *     List<DataFile> currentDataFiles = combinedScanTasks.stream()
 *             .flatMap(tasks -> tasks.files().stream().map(FileScanTask::file))
 *             .collect(Collectors.toList());
 *     replaceDataFiles(currentDataFiles, addedDataFiles, startingSnapshotId);
 *
 *     // return the result for rewriting
 *     return new Row[] {Row.of(currentDataFiles.size(), addedDataFiles.size())};
 *   }
 * }
 *
 * // a stored procedure that accepts < STRING, LONG > and
 * // return an array of STRING without datatype hint.
 * class RollbackToSnapShotProcedure implements Procedure {
 *   public String[] call(ProcedureContext procedureContext, String tableName, Long snapshot) {
 *     Table table = loadTable(tableName);
 *     Long previousSnapShotId = table.currentSnapshot();
 *     table.manageSnapshots().rollbackTo(snapshotId).commit();
 *     return new String[] {
 *             "previous_snapshot_id: " + previousSnapShotId,
 *             "current_snapshot_id " + snapshot
 *     };
 *   }
 * }
 * }</pre>
 *
 * <p>In term of the API, a stored procedure can be used as follows:
 *
 * <pre>{@code
 * // for SQL users
 * TableEnvironment tEnv = ...
 * tEnv.executeSql("CALL rollback_to_snapshot('t', 1001)");
 * }</pre>
 */
@PublicEvolving
public interface Procedure {}
