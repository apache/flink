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
package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.planner.runtime.FileSystemITCaseBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.types.Row

import org.junit.jupiter.api.BeforeEach

/** Batch [[FileSystemITCaseBase]]. */
abstract class BatchFileSystemITCaseBase extends BatchTestBase with FileSystemITCaseBase {
  @BeforeEach
  override def before(): Unit = {
    super.before()
    super.open()
  }

  override def tableEnv: TableEnvironment = {
    tEnv
  }

  override def check(sqlQuery: String, expectedResult: Seq[Row]): Unit = {
    checkResult(sqlQuery, expectedResult)
  }

  override def checkPredicate(sqlQuery: String, checkFunc: Row => Unit): Unit = {
    val table = parseQuery(sqlQuery)
    val result = executeQuery(table)

    try {
      result.foreach(checkFunc)
    } catch {
      case e: AssertionError =>
        throw new AssertionError(
          s"""
             |Results do not match for query:
             |  $sqlQuery
     """.stripMargin,
          e)
    }
  }
}
