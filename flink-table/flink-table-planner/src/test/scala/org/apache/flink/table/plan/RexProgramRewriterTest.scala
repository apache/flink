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

package org.apache.flink.table.plan

import org.apache.flink.table.plan.util.RexProgramRewriter
import org.junit.Assert.assertTrue
import org.junit.Test

import scala.collection.JavaConverters._

class RexProgramRewriterTest extends RexProgramTestBase {

  @Test
  def testRewriteRexProgram(): Unit = {
    val rexProgram = buildSimpleRexProgram()
    assertTrue(extractExprStrList(rexProgram) == wrapRefArray(Array(
      "$0",
      "$1",
      "$2",
      "$3",
      "$4",
      "*($t2, $t3)",
      "100",
      "<($t5, $t6)",
      "6",
      ">($t1, $t8)",
      "AND($t7, $t9)")))

    // use amount, id, price fields to create a new RexProgram
    val usedFields = Array(2, 3, 1)
    val types = usedFields.map(allFieldTypes.get).toList.asJava
    val names = usedFields.map(allFieldNames.get).toList.asJava
    val inputRowType = typeFactory.createStructType(types, names)
    val newRexProgram = RexProgramRewriter.rewriteWithFieldProjection(
      rexProgram, inputRowType, rexBuilder, usedFields)
    assertTrue(extractExprStrList(newRexProgram) == wrapRefArray(Array(
      "$0",
      "$1",
      "$2",
      "*($t0, $t1)",
      "100",
      "<($t3, $t4)",
      "6",
      ">($t2, $t6)",
      "AND($t5, $t7)")))
  }
}
