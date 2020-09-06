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

package org.apache.flink.table.planner.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.planner.validate._

import scala.collection.mutable

/**
  * Expressions that have strict data type specification on its inputs.
  */
trait InputTypeSpec extends PlannerExpression {

  /**
    * Input type specification for each child.
    *
    * For example, [[Power]] expecting both of the children be of double type should use:
    * {{{
    *   def expectedTypes: Seq[TypeInformation[_]] = DOUBLE_TYPE_INFO :: DOUBLE_TYPE_INFO :: Nil
    * }}}
    *
    * Inputs that don't match the expected type will be safely casted to a higher type. Therefore,
    * use the decimal type with caution as all numeric types would be casted to a very
    * inefficient type.
    */
  private[flink] def expectedTypes: Seq[TypeInformation[_]]

  override private[flink] def validateInput(): ValidationResult = {
    val typeMismatches = mutable.ArrayBuffer.empty[String]

    if(expectedTypes.size != children.size){
      return ValidationFailure(
        s"""|$this fails on input type size checking: expected types size[${expectedTypes.size}].
            |Operands types size[${children.size}].
            |""".stripMargin)
    }

    children.zip(expectedTypes).zipWithIndex.foreach { case ((e, tpe), i) =>
      if (e.resultType != tpe) {
        typeMismatches += s"expecting $tpe on ${i}th input, get ${e.resultType}"
      }
    }
    if (typeMismatches.isEmpty) {
      ValidationSuccess
    } else {
      ValidationFailure(
        s"""|$this fails on input type checking: ${typeMismatches.mkString("[", ", ", "]")}.
            |Operand should be casted to proper type
            |""".stripMargin)
    }
  }
}
