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

package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.apache.flink.types.Row
import org.junit.Test

class LiteralPrefixTest extends ExpressionTestBase {

  @Test
  def testFieldWithBooleanPrefix(): Unit = {

    testTableApi(
      'trUeX,
      "trUeX",
      "trUeX_value"
    )

    testTableApi(
      'FALSE_A,
      "FALSE_A",
      "FALSE_A_value"
    )

    testTableApi(
      'FALSE_AB,
      "FALSE_AB",
      "FALSE_AB_value"
    )

    testTableApi(
      true,
      "trUe",
      "true"
    )

    testTableApi(
      false,
      "FALSE",
      "false"
    )
  }

  def testData: Any = {
    val testData = new Row(3)
    testData.setField(0, "trUeX_value")
    testData.setField(1, "FALSE_A_value")
    testData.setField(2, "FALSE_AB_value")
    testData
  }

  def typeInfo: TypeInformation[Any] = {
    new RowTypeInfo(
      Array(Types.STRING, Types.STRING, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("trUeX", "FALSE_A", "FALSE_AB")
    ).asInstanceOf[TypeInformation[Any]]
  }
}
