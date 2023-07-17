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
package org.apache.flink.table.api.bridge.scala

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{ExplainDetail, StatementSet, Table, TableDescriptor, TablePipeline}

/**
 * A [[StatementSet]] that integrates with the Scala-specific [[DataStream]] API.
 *
 * It accepts pipelines defined by DML statements or [[Table]] objects. The planner can optimize all
 * added statements together and then either submit them as one job or attach them to the underlying
 * [[StreamExecutionEnvironment]].
 *
 * The added statements will be cleared when calling the `execute()` or `attachAsDataStream()`
 * method.
 *
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
@PublicEvolving
trait StreamStatementSet extends StatementSet {

  override def add(tablePipeline: TablePipeline): StreamStatementSet

  override def addInsertSql(statement: String): StreamStatementSet

  override def addInsert(targetPath: String, table: Table): StreamStatementSet

  override def addInsert(targetPath: String, table: Table, overwrite: Boolean): StreamStatementSet

  override def addInsert(targetDescriptor: TableDescriptor, table: Table): StreamStatementSet

  override def addInsert(
      targetDescriptor: TableDescriptor,
      table: Table,
      overwrite: Boolean): StreamStatementSet

  override def printExplain(extraDetails: ExplainDetail*): StreamStatementSet

  /**
   * Optimizes all statements as one entity and adds them as transformations to the underlying
   * [[StreamExecutionEnvironment]].
   *
   * Use [[StreamExecutionEnvironment.execute()]] to execute them.
   *
   * The added statements will be cleared after calling this method.
   */
  def attachAsDataStream(): Unit
}
