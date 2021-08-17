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

package org.apache.flink.table.api.bridge.scala.internal

import org.apache.flink.annotation.Internal
import org.apache.flink.table.api.bridge.scala.StreamStatementSet
import org.apache.flink.table.api.internal.StatementSetImpl
import org.apache.flink.table.api.{Table, TableDescriptor}

/** Implementation for [[StreamStatementSet]]. */
@Internal
class StreamStatementSetImpl(tableEnvironment: StreamTableEnvironmentImpl)
    extends StatementSetImpl[StreamTableEnvironmentImpl](tableEnvironment)
    with StreamStatementSet {

  override def addInsertSql(statement: String): StreamStatementSet = {
    super.addInsertSql(statement).asInstanceOf[StreamStatementSet]
  }

  override def addInsert(targetPath: String, table: Table): StreamStatementSet = {
    super.addInsert(targetPath, table).asInstanceOf[StreamStatementSet]
  }

  override def addInsert(
      targetPath: String,
      table: Table,
      overwrite: Boolean)
    : StreamStatementSet = {
    super.addInsert(targetPath, table, overwrite).asInstanceOf[StreamStatementSet]
  }

  override def addInsert(targetDescriptor: TableDescriptor, table: Table): StreamStatementSet = {
    super.addInsert(targetDescriptor, table).asInstanceOf[StreamStatementSet]
  }

  override def addInsert(
      targetDescriptor: TableDescriptor,
      table: Table,
      overwrite: Boolean)
    : StreamStatementSet = {
    super.addInsert(targetDescriptor, table, overwrite).asInstanceOf[StreamStatementSet]
  }

  override def attachAsDataStream(): Unit = {
    try {
      tableEnvironment.attachAsDataStream(operations)
    }
    finally {
      operations.clear()
    }
  }
}
