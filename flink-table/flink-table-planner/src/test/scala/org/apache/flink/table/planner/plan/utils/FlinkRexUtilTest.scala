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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.calcite.{FlinkRexBuilder, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.planner.codegen.ExpressionReducer

import org.apache.calcite.rex.{RexBuilder, RexLiteral, RexNode, RexUtil}
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.`type`.{BasicSqlType, SqlTypeName}
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.junit.Assert.{assertEquals, assertFalse}
import org.junit.Test

import java.math.BigDecimal
import java.util.Collections

class FlinkRexUtilTest {
  private val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem())
  private val rexBuilder = new FlinkRexBuilder(typeFactory)
  private val varcharType = typeFactory.createSqlType(VARCHAR)
  private val intType = typeFactory.createSqlType(INTEGER)

  @Test
  def testToCnf_ComplexPredicate(): Unit = {
    // From TPC-DS q41.sql
    val i_manufact = rexBuilder.makeInputRef(varcharType, 0)
    val i_category = rexBuilder.makeInputRef(varcharType, 1)
    val i_color = rexBuilder.makeInputRef(varcharType, 2)
    val i_units = rexBuilder.makeInputRef(varcharType, 3)
    val i_size = rexBuilder.makeInputRef(varcharType, 4)

    // this predicate contains 95 RexCalls. however,
    // if this predicate is converted to CNF, the result contains 2103039 RexCalls.
    val predicate = rexBuilder.makeCall(OR,
      rexBuilder.makeCall(AND,
        rexBuilder.makeCall(EQUALS, i_manufact, rexBuilder.makeLiteral("able")),
        rexBuilder.makeCall(OR,
          rexBuilder.makeCall(AND,
            rexBuilder.makeCall(EQUALS, i_category, rexBuilder.makeLiteral("Women")),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("powder")),
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("khaki"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Ounce")),
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Oz"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("medium")),
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("extra large"))
            )
          ),
          rexBuilder.makeCall(AND,
            rexBuilder.makeCall(EQUALS, i_category, rexBuilder.makeLiteral("Women")),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("brown")),
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("honeydew"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Bunch")),
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Ton"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("N/A")),
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("small"))
            )
          ),
          rexBuilder.makeCall(AND,
            rexBuilder.makeCall(EQUALS, i_category, rexBuilder.makeLiteral("Men")),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("floral")),
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("deep"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("N/A")),
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Dozen"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("petite")),
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("large"))
            )
          ),
          rexBuilder.makeCall(AND,
            rexBuilder.makeCall(EQUALS, i_category, rexBuilder.makeLiteral("Men")),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("light")),
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("cornflower"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Box")),
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Pound"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("medium")),
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("extra large"))
            )
          )
        )
      ),
      rexBuilder.makeCall(AND,
        rexBuilder.makeCall(EQUALS, i_manufact, rexBuilder.makeLiteral("able")),
        rexBuilder.makeCall(OR,
          rexBuilder.makeCall(AND,
            rexBuilder.makeCall(EQUALS, i_category, rexBuilder.makeLiteral("Women")),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("midnight")),
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("snow"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Pallet")),
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Gross"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("medium")),
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("extra large"))
            )
          ),
          rexBuilder.makeCall(AND,
            rexBuilder.makeCall(EQUALS, i_category, rexBuilder.makeLiteral("Women")),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("cyan")),
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("papaya"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Cup")),
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Dram"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("N/A")),
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("small"))
            )
          ),
          rexBuilder.makeCall(AND,
            rexBuilder.makeCall(EQUALS, i_category, rexBuilder.makeLiteral("Men")),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("orange")),
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("frosted"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Each")),
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Tbl"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("petite")),
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("large"))
            )
          ),
          rexBuilder.makeCall(AND,
            rexBuilder.makeCall(EQUALS, i_category, rexBuilder.makeLiteral("Men")),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("forest")),
              rexBuilder.makeCall(EQUALS, i_color, rexBuilder.makeLiteral("ghost"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Lb")),
              rexBuilder.makeCall(EQUALS, i_units, rexBuilder.makeLiteral("Bundle"))
            ),
            rexBuilder.makeCall(OR,
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("medium")),
              rexBuilder.makeCall(EQUALS, i_size, rexBuilder.makeLiteral("extra large"))
            )
          )
        )
      )
    )

    // the number of RexCall in the CNF result exceeds 95 * 2, so returns the original expression
    val newPredicate1 = FlinkRexUtil.toCnf(rexBuilder, -1, predicate)
    assertEquals(predicate.toString, newPredicate1.toString)

    val newPredicate2 = FlinkRexUtil.toCnf(rexBuilder, 200, predicate)
    assertEquals(predicate.toString, newPredicate2.toString)

    val newPredicate3 = FlinkRexUtil.toCnf(rexBuilder, 2103039, predicate)
    assertEquals(RexUtil.toCnf(rexBuilder, predicate).toString, newPredicate3.toString)

    val newPredicate4 = FlinkRexUtil.toCnf(rexBuilder, Int.MaxValue, predicate)
    assertFalse(RexUtil.eq(predicate, newPredicate4))
    assertEquals(RexUtil.toCnf(rexBuilder, predicate).toString, newPredicate4.toString)
  }

  @Test
  def testToCnf_SimplePredicate(): Unit = {
    // (a="1" AND b="2") OR c="3"
    val a = rexBuilder.makeInputRef(varcharType, 0)
    val b = rexBuilder.makeInputRef(varcharType, 1)
    val c = rexBuilder.makeInputRef(varcharType, 2)

    val predicate = rexBuilder.makeCall(OR,
      rexBuilder.makeCall(AND,
        rexBuilder.makeCall(EQUALS, a, rexBuilder.makeLiteral("1")),
        rexBuilder.makeCall(EQUALS, b, rexBuilder.makeLiteral("2"))
      ),
      rexBuilder.makeCall(EQUALS, c, rexBuilder.makeLiteral("3"))
    )

    // (a="1" OR c="3") OR (b="2" OR c="3")
    val expected = rexBuilder.makeCall(AND,
      rexBuilder.makeCall(OR,
        rexBuilder.makeCall(EQUALS, a, rexBuilder.makeLiteral("1")),
        rexBuilder.makeCall(EQUALS, c, rexBuilder.makeLiteral("3"))
      ),
      rexBuilder.makeCall(OR,
        rexBuilder.makeCall(EQUALS, b, rexBuilder.makeLiteral("2")),
        rexBuilder.makeCall(EQUALS, c, rexBuilder.makeLiteral("3"))
      )
    )

    val newPredicate1 = FlinkRexUtil.toCnf(rexBuilder, -1, predicate)
    assertEquals(expected.toString, newPredicate1.toString)
    assertEquals(expected.toString, RexUtil.toCnf(rexBuilder, predicate).toString)

    val newPredicate2 = FlinkRexUtil.toCnf(rexBuilder, 0, predicate)
    assertEquals(predicate.toString, newPredicate2.toString)
  }

  @Test
  def testSimplify(): Unit = {
    val a = rexBuilder.makeInputRef(varcharType, 0)
    val b = rexBuilder.makeInputRef(varcharType, 1)
    val c = rexBuilder.makeInputRef(intType, 2)
    val d = rexBuilder.makeInputRef(intType, 3)

    // a = b AND a = b
    val predicate0 = rexBuilder.makeCall(AND,
      rexBuilder.makeCall(EQUALS, a, b),
      rexBuilder.makeCall(EQUALS, a, b)
    )
    val newPredicate0 = simplify(rexBuilder, predicate0)
    assertEquals(rexBuilder.makeCall(EQUALS, a, b).toString, newPredicate0.toString)

    // a = b AND b = a
    val predicate1 = rexBuilder.makeCall(AND,
      rexBuilder.makeCall(EQUALS, a, b),
      rexBuilder.makeCall(EQUALS, b, a)
    )
    val newPredicate1 = simplify(rexBuilder, predicate1)
    assertEquals(rexBuilder.makeCall(EQUALS, a, b).toString, newPredicate1.toString)

    // a = b OR b = a
    val predicate2 = rexBuilder.makeCall(OR,
      rexBuilder.makeCall(EQUALS, a, b),
      rexBuilder.makeCall(EQUALS, b, a)
    )
    val newPredicate2 = simplify(rexBuilder, predicate2)
    assertEquals(rexBuilder.makeCall(EQUALS, a, b).toString, newPredicate2.toString)

    // a = b AND c < d AND b = a AND d > c
    val predicate3 = rexBuilder.makeCall(AND,
      rexBuilder.makeCall(EQUALS, a, b),
      rexBuilder.makeCall(LESS_THAN, c, d),
      rexBuilder.makeCall(EQUALS, b, a),
      rexBuilder.makeCall(GREATER_THAN, d, c)
    )
    val newPredicate3 = simplify(rexBuilder, predicate3)
    assertEquals(rexBuilder.makeCall(AND,
      rexBuilder.makeCall(EQUALS, a, b),
      rexBuilder.makeCall(LESS_THAN, c, d)).toString,
      newPredicate3.toString)

    // cast(a as INTEGER) >= c and c <= cast(a as INTEGER)
    val predicate4 = rexBuilder.makeCall(AND,
      rexBuilder.makeCall(GREATER_THAN_OR_EQUAL,
        rexBuilder.makeCast(typeFactory.createSqlType(INTEGER), a), c),
      rexBuilder.makeCall(LESS_THAN_OR_EQUAL,
        c, rexBuilder.makeCast(typeFactory.createSqlType(INTEGER), a))
    )
    val newPredicate4 = simplify(rexBuilder, predicate4)
    assertEquals(rexBuilder.makeCall(GREATER_THAN_OR_EQUAL,
      rexBuilder.makeCast(typeFactory.createSqlType(INTEGER), a), c).toString,
      newPredicate4.toString)

    // (substring(a, 1, 3) = b OR c <= d + 1 OR d + 1 >= c)
    // AND
    // (b = (substring(a, 1, 3) OR d + 1 >= c OR c <= d + 1)
    val aSubstring13 = rexBuilder.makeCall(SUBSTRING, a,
      rexBuilder.makeBigintLiteral(BigDecimal.ONE),
      rexBuilder.makeBigintLiteral(BigDecimal.valueOf(3)))
    val dPlus1 = rexBuilder.makeCall(PLUS, d, rexBuilder.makeBigintLiteral(BigDecimal.ONE))
    val predicate5 = rexBuilder.makeCall(AND,
      rexBuilder.makeCall(OR,
        rexBuilder.makeCall(EQUALS, aSubstring13, b),
        rexBuilder.makeCall(LESS_THAN_OR_EQUAL, c, dPlus1),
        rexBuilder.makeCall(GREATER_THAN_OR_EQUAL, dPlus1, c)),
      rexBuilder.makeCall(OR,
        rexBuilder.makeCall(EQUALS, b, aSubstring13),
        rexBuilder.makeCall(GREATER_THAN_OR_EQUAL, dPlus1, c),
        rexBuilder.makeCall(LESS_THAN_OR_EQUAL, c, dPlus1))
    )
    val newPredicate5 = simplify(rexBuilder, predicate5)
    assertEquals(rexBuilder.makeCall(OR,
      rexBuilder.makeCall(EQUALS, aSubstring13, b),
      rexBuilder.makeCall(LESS_THAN_OR_EQUAL, c, dPlus1)).toString,
      newPredicate5.toString)

    // (a = b OR c < d OR a > 'l') AND (b = a OR d > c OR b < 'k')
    val predicate6 = rexBuilder.makeCall(AND,
      rexBuilder.makeCall(OR,
        rexBuilder.makeCall(EQUALS, a, b),
        rexBuilder.makeCall(LESS_THAN, c, d),
        rexBuilder.makeCall(GREATER_THAN, a, rexBuilder.makeLiteral("l"))
      ),
      rexBuilder.makeCall(OR,
        rexBuilder.makeCall(EQUALS, b, a),
        rexBuilder.makeCall(GREATER_THAN, d, c),
        rexBuilder.makeCall(LESS_THAN, b, rexBuilder.makeLiteral("k"))
      )
    )
    val newPredicate6 = simplify(rexBuilder, predicate6)
    assertEquals(rexBuilder.makeCall(AND,
      rexBuilder.makeCall(OR,
        rexBuilder.makeCall(EQUALS, a, b),
        rexBuilder.makeCall(LESS_THAN, c, d),
        rexBuilder.makeCall(GREATER_THAN, a, rexBuilder.makeLiteral("l"))
      ),
      rexBuilder.makeCall(OR,
        rexBuilder.makeCall(EQUALS, a, b),
        rexBuilder.makeCall(LESS_THAN, c, d),
        rexBuilder.makeCall(LESS_THAN, b, rexBuilder.makeLiteral("k"))
      )
    ).toString, newPredicate6.toString)

    // (a = b AND c < d) AND b = a
    val predicate7 = rexBuilder.makeCall(AND,
      rexBuilder.makeCall(AND,
        rexBuilder.makeCall(EQUALS, a, b),
        rexBuilder.makeCall(LESS_THAN, c, d)
      ),
      rexBuilder.makeCall(EQUALS, b, a)
    )
    val newPredicate7 = simplify(rexBuilder, predicate7)
    assertEquals(rexBuilder.makeCall(AND,
      rexBuilder.makeCall(EQUALS, a, b),
      rexBuilder.makeCall(LESS_THAN, c, d)).toString,
      newPredicate7.toString)

    // b >= a OR (a <= b OR c = d)
    val predicate8 = rexBuilder.makeCall(OR,
      rexBuilder.makeCall(GREATER_THAN_OR_EQUAL, b, a),
      rexBuilder.makeCall(OR,
        rexBuilder.makeCall(LESS_THAN_OR_EQUAL, a, b),
        rexBuilder.makeCall(EQUALS, c, d)
      )
    )
    val newPredicate8 = simplify(rexBuilder, predicate8)
    assertEquals(rexBuilder.makeCall(OR,
      rexBuilder.makeCall(GREATER_THAN_OR_EQUAL, b, a),
      rexBuilder.makeCall(EQUALS, c, d)).toString,
      newPredicate8.toString)

    // true AND true
    val predicate9 = rexBuilder.makeCall(AND,
      rexBuilder.makeLiteral(true), rexBuilder.makeLiteral(true))
    val newPredicate9 = simplify(rexBuilder, predicate9)
    assertEquals(rexBuilder.makeLiteral(true).toString, newPredicate9.toString)

    // false OR false
    val predicate10 = rexBuilder.makeCall(OR,
      rexBuilder.makeLiteral(false), rexBuilder.makeLiteral(false))
    val newPredicate10 = simplify(rexBuilder, predicate10)
    assertEquals(rexBuilder.makeLiteral(false).toString, newPredicate10.toString)

    // a = a
    val predicate11 = rexBuilder.makeCall(EQUALS, a, a)
    val newPredicate11 = simplify(rexBuilder, predicate11)
    assertEquals(rexBuilder.makeLiteral(true).toString, newPredicate11.toString)

    // a >= a
    val predicate12 = rexBuilder.makeCall(GREATER_THAN_OR_EQUAL, a, a)
    val newPredicate12 = simplify(rexBuilder, predicate12)
    assertEquals(rexBuilder.makeLiteral(true).toString, newPredicate12.toString)

    // a <= a
    val predicate13 = rexBuilder.makeCall(LESS_THAN_OR_EQUAL, a, a)
    val newPredicate13 = simplify(rexBuilder, predicate13)
    assertEquals(rexBuilder.makeLiteral(true).toString, newPredicate13.toString)

    // a <> a
    val predicate14 = rexBuilder.makeCall(NOT_EQUALS, a, a)
    val newPredicate14 = simplify(rexBuilder, predicate14)
    assertEquals(rexBuilder.makeLiteral(false).toString, newPredicate14.toString)

    // a > a
    val predicate15 = rexBuilder.makeCall(GREATER_THAN, a, a)
    val newPredicate15 = simplify(rexBuilder, predicate15)
    assertEquals(rexBuilder.makeLiteral(false).toString, newPredicate15.toString)

    // a < a
    val predicate16 = rexBuilder.makeCall(LESS_THAN, a, a)
    val newPredicate16 = simplify(rexBuilder, predicate16)
    assertEquals(rexBuilder.makeLiteral(false).toString, newPredicate16.toString)

    // c = 0 AND SEARCH(c, [0, 1])
    val predicate17Equals = rexBuilder.makeCall(EQUALS, c, intLiteral(0))
    val predicate17 = rexBuilder.makeCall(
      AND,
      predicate17Equals,
      rexBuilder.makeIn(c, java.util.Arrays.asList(
        intLiteral(0),
        intLiteral(1))))
    val newPredicate17 = simplify(rexBuilder, predicate17)
    assertEquals(
      rexBuilder.makeIn(c, Collections.singletonList[RexNode](intLiteral(0))).toString,
      newPredicate17.toString)

    // c = 0 OR SEARCH(c, [0, 1])
    val predicate18Search = rexBuilder.makeIn(c, java.util.Arrays.asList(
      intLiteral(0),
      intLiteral(1)))
    val predicate18 = rexBuilder.makeCall(
      OR,
      rexBuilder.makeCall(EQUALS, c, intLiteral(0)),
      predicate18Search)
    val newPredicate18 = simplify(rexBuilder, predicate18)
    assertEquals(predicate18Search.toString, newPredicate18.toString)

    // c > 0 AND (
    //   SEARCH(c, [0, 1]) OR (
    //     SEARCH(c, [1, 2]) AND c < 2))
    val predicate19Layer2 = rexBuilder.makeCall(
      AND,
      rexBuilder.makeIn(c, java.util.Arrays.asList(
        intLiteral(1),
        intLiteral(2))),
      rexBuilder.makeCall(LESS_THAN, c, intLiteral(2)))
    val predicate19Layer1 = rexBuilder.makeCall(
      OR,
      rexBuilder.makeIn(c, java.util.Arrays.asList(
        intLiteral(0),
        intLiteral(1))),
      predicate19Layer2)
    val predicate19 = rexBuilder.makeCall(
      AND,
      rexBuilder.makeCall(GREATER_THAN, c, intLiteral(0)),
      predicate19Layer1)
    val newPredicate19 = simplify(rexBuilder, predicate19)
    assertEquals(
      rexBuilder.makeIn(c, Collections.singletonList[RexNode](intLiteral(1))).toString,
      newPredicate19.toString)

    // c >= 0 OR SEARCH(c, [0, 1])
    // TODO `c >= 0 OR SEARCH(c, [0, 1])` should be simplified to c >= 0
    val predicate20 = rexBuilder.makeCall(
      OR,
      rexBuilder.makeCall(GREATER_THAN_OR_EQUAL, c, intLiteral(0)),
      predicate18Search)
    val newPredicate20 = simplify(rexBuilder, predicate20)
    assertEquals(predicate20.toString, newPredicate20.toString)

    //CAST(1 AS BOOLEAN)
    val predicate21CastFromData = intLiteral(1)
    val predicate21Cast = makeToBooleanCast(predicate21CastFromData)
    val newPredicate21 = simplify(rexBuilder, predicate21Cast)
    assertEquals(rexBuilder.makeLiteral(true).toString, newPredicate21.toString)

    //CAST(0 AS BOOLEAN)
    val predicate22CastFromData = intLiteral(0)
    val predicate22Cast = makeToBooleanCast(predicate22CastFromData)
    val newPredicate22 = simplify(rexBuilder, predicate22Cast)
    assertEquals(rexBuilder.makeLiteral(false).toString, newPredicate22.toString)

    //CAST(-1 AS BOOLEAN)
    val predicate23CastFromData = intLiteral(-1)
    val predicate23Cast = makeToBooleanCast(predicate23CastFromData)
    val newPredicate23 = simplify(rexBuilder, predicate23Cast)
    assertEquals(rexBuilder.makeLiteral(true).toString, newPredicate23.toString)

    //CAST(1.1 AS BOOLEAN)
    val predicate24CastFromData = rexBuilder.makeExactLiteral(BigDecimal.valueOf(1.1))
    val predicate24Cast = makeToBooleanCast(predicate24CastFromData)
    val newPredicate24 = simplify(rexBuilder, predicate24Cast)
    assertEquals(rexBuilder.makeLiteral(true).toString, newPredicate24.toString)

    //CAST(0.000 AS BOOLEAN)
    val predicate25CastFromData = rexBuilder.makeExactLiteral(BigDecimal.valueOf(0.000))
    val predicate25Cast = makeToBooleanCast(predicate25CastFromData)
    val newPredicate25 = simplify(rexBuilder, predicate25Cast)
    assertEquals(rexBuilder.makeLiteral(false).toString, newPredicate25.toString)
  }

  def intLiteral(x: Int): RexLiteral = rexBuilder.makeExactLiteral(BigDecimal.valueOf(x))

  def simplify(rexBuilder: RexBuilder, expr: RexNode): RexNode ={
    val expressionReducer = new ExpressionReducer(TableConfig.getDefault, false)
    FlinkRexUtil.simplify(rexBuilder, expr, expressionReducer)
  }

  private def makeToBooleanCast(fromData: RexNode): RexNode ={
    val booleanType = new BasicSqlType(typeFactory.getTypeSystem, SqlTypeName.BOOLEAN)
    rexBuilder.makeCall(
      booleanType,
      CAST,
      Collections.singletonList(fromData.asInstanceOf[RexNode]))
  }
}
