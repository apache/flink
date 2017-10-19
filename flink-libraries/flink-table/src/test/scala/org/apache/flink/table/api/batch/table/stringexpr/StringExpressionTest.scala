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

package org.apache.flink.table.api.batch.table.stringexpr

import org.apache.flink.table.expressions.{ExpressionParser, Literal, UnresolvedFieldReference}
import org.junit.{Assert, Test}

class StringExpressionTest {

  @Test
  def testFieldWithBooleanPrefix: Unit = {

    Assert.assertEquals(
      UnresolvedFieldReference("trUex"),
      ExpressionParser.parseExpression("trUex")
    )

    Assert.assertEquals(
      UnresolvedFieldReference("FALSE_A"),
      ExpressionParser.parseExpression("FALSE_A")
    )

    Assert.assertEquals(
      Literal(true),
      ExpressionParser.parseExpression("true")
    )

    Assert.assertEquals(
      Literal(false),
      ExpressionParser.parseExpression("false")
    )

  }

}
