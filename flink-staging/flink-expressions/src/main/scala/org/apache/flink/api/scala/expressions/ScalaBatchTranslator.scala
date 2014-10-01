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

package org.apache.flink.api.scala.expressions


import org.apache.flink.api.expressions.tree.Expression
import org.apache.flink.api.scala.wrap
import org.apache.flink.api.expressions.operations._
import org.apache.flink.api.expressions.ExpressionOperation
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet

import scala.reflect.ClassTag


/**
 * [[OperationTranslator]] for creating [[ExpressionOperation]]s from Scala [[DataSet]]s and
 * translating them back to Scala [[DataSet]]s.
 */
class ScalaBatchTranslator extends OperationTranslator {

  private val javaTranslator = new JavaBatchTranslator

  override type Representation[A] = DataSet[A]

  def createExpressionOperation[A](
      repr: DataSet[A],
      fields: Array[Expression]): ExpressionOperation[ScalaBatchTranslator] = {

    val result = javaTranslator.createExpressionOperation(repr.javaSet, fields)

    new ExpressionOperation[ScalaBatchTranslator](result.operation, this)
  }

  override def translate[O](op: Operation)(implicit tpe: TypeInformation[O]): DataSet[O] = {
    // fake it till you make it ...
    wrap(javaTranslator.translate(op))(ClassTag.AnyRef.asInstanceOf[ClassTag[O]])
  }
}
