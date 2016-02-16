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

import java.lang.reflect.Modifier

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.table.parser.ExpressionParser
import org.apache.flink.api.table.expressions.{Expression, Naming, ResolvedFieldReference, UnresolvedFieldReference}
import org.apache.flink.api.table.typeinfo.RowTypeInfo
import org.apache.flink.api.table.{ExpressionException, Table}

import scala.language.reflectiveCalls

/**
 * Base class for translators that transform the logical plan in a [[Table]] to an executable
 * Flink plan and also for creating a [[Table]] from a DataSet or DataStream.
 */
abstract class PlanTranslator {

  type Representation[A] <: { def getType(): TypeInformation[A] }

  /**
   * Translates the given Table API [[PlanNode]] back to the underlying representation, i.e,
   * a DataSet or a DataStream.
   */
  def translate[A](op: PlanNode)(implicit tpe: TypeInformation[A]): Representation[A]

  /**
   * Creates a [[Table]] from a DataSet or a DataStream (the underlying representation).
   */
  def createTable[A](
      repr: Representation[A],
      inputType: CompositeType[A],
      expressions: Array[Expression],
      resultFields: Seq[(String, TypeInformation[_])]): Table

  /**
   * Creates a [[Table]] from the given DataSet or DataStream.
   */
  def createTable[A](repr: Representation[A]): Table = {

    val fields = repr.getType() match {
      case c: CompositeType[A] => c.getFieldNames.map(UnresolvedFieldReference)

      case tpe => Array() // createTable will throw an exception for this later
    }
    createTable(
      repr,
      fields.toArray.asInstanceOf[Array[Expression]],
      checkDeterministicFields = false)
  }

  /**
   * Creates a [[Table]] from the given DataSet or DataStream while only taking those
   * fields mentioned in the field expression.
   */
  def createTable[A](repr: Representation[A], expression: String): Table = {

    val fields = ExpressionParser.parseExpressionList(expression)

    createTable(repr, fields.toArray, checkDeterministicFields = true)
  }

  /**
   * Creates a [[Table]] from the given DataSet or DataStream while only taking those
   * fields mentioned in the fields parameter.
   *
   * When checkDeterministicFields is true check whether the fields of the underlying
   * [[TypeInformation]] have a deterministic ordering. This is only the case for Tuples
   * and Case classes. For a POJO, the field order is not obvious, this can lead to problems
   * when a user renames fields and assumes a certain ordering.
   */
  def createTable[A](
      repr: Representation[A],
      fields: Array[Expression],
      checkDeterministicFields: Boolean = true): Table = {

    // shortcut for DataSet[Row] or DataStream[Row]
    repr.getType() match {
      case rowTypeInfo: RowTypeInfo =>
        val expressions = rowTypeInfo.getFieldNames map {
          name => (name, rowTypeInfo.getTypeAt(name))
        }
        new Table(
          Root(repr, expressions))

      case c: CompositeType[A] => // us ok

      case tpe => throw new ExpressionException("Only DataSets or DataStreams of composite type" +
        "can be transformed to a Table. These would be tuples, case classes and " +
        "POJOs. Type is: " + tpe)

    }

    val clazz = repr.getType().getTypeClass
    if ((clazz.isMemberClass && !Modifier.isStatic(clazz.getModifiers))
        || clazz.getCanonicalName() == null) {
      throw new ExpressionException("Cannot create Table from DataSet or DataStream of type " +
        clazz.getName + ". Only top-level classes or static members classes " +
        " are supported.")
    }

    val inputType = repr.getType().asInstanceOf[CompositeType[A]]

    if (!inputType.hasDeterministicFieldOrder && checkDeterministicFields) {
      throw new ExpressionException(s"You cannot rename fields upon Table creation: " +
        s"Field order of input type $inputType is not deterministic." )
    }

    if (fields.length != inputType.getFieldNames.length) {
      throw new ExpressionException("Number of selected fields: '" + fields.mkString(",") +
        "' and number of fields in input type " + inputType + " do not match.")
    }

    val newFieldNames = fields map {
      case UnresolvedFieldReference(name) => name
      case e =>
        throw new ExpressionException("Only field references allowed in 'as' operation, " +
          " offending expression: " + e)
    }

    if (newFieldNames.toSet.size != newFieldNames.size) {
      throw new ExpressionException(s"Ambiguous field names in ${fields.mkString(", ")}")
    }

    val resultFields: Seq[(String, TypeInformation[_])] = newFieldNames.zipWithIndex map {
      case (name, index) => (name, inputType.getTypeAt(index))
    }

    val inputFields = inputType.getFieldNames
    val fieldMappings = inputFields.zip(resultFields)
    val expressions: Array[Expression] = fieldMappings map {
      case (oldName, (newName, tpe)) => Naming(ResolvedFieldReference(oldName, tpe), newName)
    }

    createTable(repr, inputType, expressions, resultFields)
  }

}
