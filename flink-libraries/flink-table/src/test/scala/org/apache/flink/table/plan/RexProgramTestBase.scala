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

import java.math.BigDecimal

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeSystem}
import org.apache.calcite.rex.{RexBuilder, RexProgram, RexProgramBuilder}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable

import scala.collection.JavaConverters._
import scala.collection.mutable

abstract class RexProgramTestBase {

  val typeFactory: JavaTypeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT)

  val allFieldNames: java.util.List[String] = List("name", "id", "amount", "price", "flag").asJava

  val allFieldTypes: java.util.List[RelDataType] =
    List(VARCHAR, BIGINT, INTEGER, DOUBLE, BOOLEAN).map(typeFactory.createSqlType).asJava

  var rexBuilder: RexBuilder = new RexBuilder(typeFactory)

  /**
    * extract all expression string list from input RexProgram expression lists
    *
    * @param rexProgram input RexProgram instance to analyze
    * @return all expression string list of input RexProgram expression lists
    */
  protected def extractExprStrList(rexProgram: RexProgram): mutable.Buffer[String] = {
    rexProgram.getExprList.asScala.map(_.toString)
  }

  // select amount, amount * price as total where amount * price < 100 and id > 6
  protected def buildSimpleRexProgram(): RexProgram = {
    val inputRowType = typeFactory.createStructType(allFieldTypes, allFieldNames)
    val builder = new RexProgramBuilder(inputRowType, rexBuilder)

    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    val t1 = rexBuilder.makeInputRef(allFieldTypes.get(1), 1)
    val t2 = rexBuilder.makeInputRef(allFieldTypes.get(3), 3)
    val t3 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, t0, t2))
    val t4 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))
    val t5 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(6L))

    // project: amount, amount * price as total
    builder.addProject(t0, "amount")
    builder.addProject(t3, "total")

    // condition: amount * price < 100 and id > 6
    val t6 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, t3, t4))
    val t7 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t1, t5))
    val t8 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.AND, List(t6, t7).asJava))
    builder.addCondition(t8)

    builder.getProgram
  }

  protected def makeTypes(fieldTypes: SqlTypeName*): java.util.List[RelDataType] = {
    fieldTypes.toList.map(typeFactory.createSqlType).asJava
  }
}
