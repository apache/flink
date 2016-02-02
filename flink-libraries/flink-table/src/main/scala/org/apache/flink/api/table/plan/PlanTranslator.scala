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
package org.apache.flink.api.table.plan

import org.apache.calcite.rel.RelNode
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.table.parser.ExpressionParser
import org.apache.flink.api.table.expressions.{Naming, Expression, UnresolvedFieldReference}
import org.apache.flink.api.table.Table

import scala.language.reflectiveCalls

/**
 * Base class for translators that transform the logical plan in a [[Table]] to an executable
 * Flink plan and also for creating a [[Table]] from a DataSet.
 */
abstract class PlanTranslator {

  type Representation[A] <: { def getType(): TypeInformation[A] }

  /**
   * Translates the given Table API back to the underlying representation, i.e, a DataSet.
   */
  def translate[A](op: RelNode)(implicit tpe: TypeInformation[A]): Representation[A]

  /**
   * Creates a [[Table]] from a DataSet (the underlying representation).
   */
  def createTable[A](
    repr: Representation[A],
    fieldIndexes: Array[Int],
    fieldNames: Array[String]): Table

  /**
   * Creates a [[Table]] from the given DataSet.
   */
  def createTable[A](repr: Representation[A]): Table = {

    val fieldNames: Array[String] = repr.getType() match {
      case t: TupleTypeInfo[A] => t.getFieldNames
      case c: CaseClassTypeInfo[A] => c.getFieldNames
      case p: PojoTypeInfo[A] => p.getFieldNames
      case tpe =>
        throw new IllegalArgumentException(
          s"Type $tpe requires explicit field naming with AS.")
    }
    val fieldIndexes = fieldNames.indices.toArray
    createTable(repr, fieldIndexes, fieldNames)
  }

  /**
   * Creates a [[Table]] from the given DataSet while only taking those
   * fields mentioned in the field expressions.
   */
  def createTable[A](repr: Representation[A], expression: String): Table = {

    val exprs = ExpressionParser
      .parseExpressionList(expression)
      .toArray

    createTable(repr, exprs)
  }

  /**
    * Creates a [[Table]] from the given DataSet while only taking those
    * fields mentioned in the field expressions.
    */
  def createTable[A](repr: Representation[A], exprs: Array[Expression]): Table = {

    val inputType = repr.getType()

    val indexedNames: Array[(Int, String)] = inputType match {
      case a: AtomicType[A] =>
        if (exprs.length != 1) {
          throw new IllegalArgumentException("Atomic type may can only have a single field.")
        }
        exprs.map {
          case UnresolvedFieldReference(name) => (0, name)
          case _ => throw new IllegalArgumentException(
            "Field reference expression expected.")
        }
      case t: TupleTypeInfo[A] =>
        exprs.zipWithIndex.map {
          case (UnresolvedFieldReference(name), idx) => (idx, name)
          case (Naming(UnresolvedFieldReference(origName), name), _) =>
            val idx = t.getFieldIndex(origName)
            if (idx < 0) {
              throw new IllegalArgumentException(s"$origName is not a field of type $t")
            }
            (idx, name)
          case _ => throw new IllegalArgumentException(
            "Field reference expression or naming expression expected.")
        }
      case c: CaseClassTypeInfo[A] =>
        exprs.zipWithIndex.map {
          case (UnresolvedFieldReference(name), idx) => (idx, name)
          case (Naming(UnresolvedFieldReference(origName), name), _) =>
            val idx = c.getFieldIndex(origName)
            if (idx < 0) {
              throw new IllegalArgumentException(s"$origName is not a field of type $c")
            }
            (idx, name)
          case _ => throw new IllegalArgumentException(
            "Field reference expression or naming expression expected.")
        }
      case p: PojoTypeInfo[A] =>
        exprs.map {
          case Naming(UnresolvedFieldReference(origName), name) =>
            val idx = p.getFieldIndex(origName)
            if (idx < 0) {
              throw new IllegalArgumentException(s"$origName is not a field of type $p")
            }
            (idx, name)
          case _ => throw new IllegalArgumentException(
            "Field naming expression expected.")
        }
      case tpe => throw new IllegalArgumentException(
        s"Type $tpe cannot be converted into Table.")
    }

    val (fieldIndexes, fieldNames) = indexedNames.unzip

    createTable(repr, fieldIndexes.toArray, fieldNames.toArray)
  }

}
