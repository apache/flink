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

package org.apache.flink.api.table.input

import java.util.{Map, List}

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.{PrimitiveArrayTypeInfo, BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.java.operators.MapOperator
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.typeinfo.RowTypeInfo
import org.apache.flink.api.table.Row
import org.apache.flink.hcatalog.java.HCatInputFormat
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.hive.hcatalog.data.DefaultHCatRecord
import org.apache.hive.hcatalog.data.schema.{HCatSchema, HCatFieldSchema}
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type._
import scala.collection.mutable.ArraySeq
import scala.collection.mutable.Set

class HCatTableSource(private[flink] val database: String,
                      private[flink] val table: String) extends AdaptiveTableSource {

  val inputFormat = new HCatInputFormat[DefaultHCatRecord](database, table)
  val outputFields = generateFieldsFromSchema(inputFormat.getOutputSchema)
  val partitionColumns = generatePartitionColumns(inputFormat)

  var resolvedFields: Set[String] = null
  val resolvedPredicateFields: Set[String] = Set[String]()

  val predicates = Set[String]()

  override def getOutputFields(): Seq[(String, TypeInformation[_])] = {
    outputFields
  }

  override def supportsResolvedFieldPushdown(): Boolean = true

  override def supportsPredicatePushdown(): Boolean = true

  override def notifyResolvedField(field: String, filtering: Boolean) = {
    // selection: from '*' to reduced field set
    if (!filtering && resolvedFields == null) {
      resolvedFields = Set[String]()
      resolvedFields ++= resolvedPredicateFields
      resolvedFields.add(field)
    }
    // filtering: add field to separate field set in case
    // the field set will be reduced later
    else if (filtering && resolvedFields == null) {
      resolvedPredicateFields.add(field)
    }
    // selection or filtering: add field to reduced field set
    else {
      resolvedFields.add(field)
    }
  }

  def notifyPredicates(expr: Expression) = {
    // input format does not support "not" and only supports partitioned columns
    val cleanedExpr = removeNotAndUnpartitionedPredicates(expr)

    // convert non-empty predicate expressions to string and add
    if (!cleanedExpr.isInstanceOf[NopExpression]) {
      predicates.add(exprToFilterString(cleanedExpr))
    }
  }

  override def createAdaptiveDataSet(env: ExecutionEnvironment) : DataSet[Row] = {
    val adaptiveInputFormat = new HCatInputFormat[DefaultHCatRecord](database, table)

    // projection pushdown to input format
    if (resolvedFields != null) {
      val resolvedOrderedOutputNames = outputFields
        .filter(field => resolvedFields.contains(field._1))
        .map(_._1)
      adaptiveInputFormat.getFields(resolvedOrderedOutputNames:_*)
    }

    // predicate pushdown to input format
    if (predicates.size > 0) {
      adaptiveInputFormat.withFilter(predicates.mkString(" and"))
    }

    val inputDs = env.createInput(adaptiveInputFormat)
    val function = new HCatTableSource.HCatRecord2RowMapper(
      adaptiveInputFormat.getOutputSchema,
      outputFields)

    val outputNames = outputFields.map(_._1)
    val outputTypes = outputFields.map(_._2)

    val resultType = new RowTypeInfo(outputTypes, outputNames)
    new MapOperator(inputDs, resultType, function,
      s"HCatTableSource(${outputNames.mkString(",")})")
  }

  override def createAdaptiveDataStream(env: StreamExecutionEnvironment) : DataStream[Row] =
    throw new UnsupportedOperationException("HCatTableSource currently only supports " +
      "batch executions.")

  // ----------------------------------------------------------------------------------------------

  def generatePartitionColumns(inputFormat: HCatInputFormat[DefaultHCatRecord]): Seq[String] = {
    val schema = inputFormat.getPartitionColumns
    val resultFields: Array[String] = new Array(schema.size)
    for (i <- 0 until schema.size) {
      resultFields(i) = schema.get(i).getName
    }
    resultFields
  }

  def generateFieldsFromSchema(schema: HCatSchema): Seq[(String, TypeInformation[_])] = {
    val numFields = schema.getFields.size
    val resultFields: ArraySeq[(String, TypeInformation[_])] = new ArraySeq(numFields)

    for (i <- 0 until schema.size()) {
      val fieldName: String = schema.get(i).getName()

      val fieldSchema: HCatFieldSchema = schema.get(fieldName)
      val fieldPos: Int = schema.getPosition(fieldName)
      val fieldType: TypeInformation[_] = getFieldType(fieldSchema)
      resultFields.update(fieldPos, (fieldName, fieldType))
    }
    resultFields
  }

  def getFieldType(fieldSchema: HCatFieldSchema): TypeInformation[_] = {
    fieldSchema.getType match {
      case INT =>
        BasicTypeInfo.INT_TYPE_INFO
      case TINYINT =>
        BasicTypeInfo.BYTE_TYPE_INFO
      case SMALLINT =>
        BasicTypeInfo.SHORT_TYPE_INFO
      case BIGINT =>
        BasicTypeInfo.LONG_TYPE_INFO
      case BOOLEAN =>
        BasicTypeInfo.BOOLEAN_TYPE_INFO
      case FLOAT =>
        BasicTypeInfo.FLOAT_TYPE_INFO
      case DOUBLE =>
        BasicTypeInfo.DOUBLE_TYPE_INFO
      case STRING =>
        BasicTypeInfo.STRING_TYPE_INFO
      case BINARY =>
        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
      case ARRAY =>
        new GenericTypeInfo[List[_]](classOf[List[_]])
      case MAP =>
        new GenericTypeInfo[Map[_,_]](classOf[Map[_,_]])
      case STRUCT =>
        new GenericTypeInfo[List[_]](classOf[List[_]])
      case unknownType =>
        throw new IllegalArgumentException("Unknown data type \"" + unknownType
          + "\" encountered.")
    }
  }

  def removeNotAndUnpartitionedPredicates(expr: Expression): Expression = {
    expr match {
      case And(left, right) =>
        val prunedLeft = removeNotAndUnpartitionedPredicates(left)
        val prunedRight = removeNotAndUnpartitionedPredicates(right)

        if (prunedLeft.isInstanceOf[NopExpression]) {
          prunedRight
        }
        else if (prunedRight.isInstanceOf[NopExpression]) {
          prunedLeft
        }
        else {
          And(prunedLeft, prunedRight)
        }
      case Or(left, right) =>
        if (!left.isInstanceOf[Not] && !right.isInstanceOf[Not]) {
          val prunedLeft = removeNotAndUnpartitionedPredicates(left)
          val prunedRight = removeNotAndUnpartitionedPredicates(right)

          if (prunedLeft.isInstanceOf[NopExpression] || prunedRight.isInstanceOf[NopExpression]) {
            NopExpression()
          }
          else {
            Or(prunedLeft, prunedRight)
          }
        }
        else {
          NopExpression()
        }
      case Not(e) =>
          NopExpression()
      case bc: BinaryComparison if bc.right.isInstanceOf[ResolvedFieldReference] =>
        val fieldRef = bc.right.asInstanceOf[ResolvedFieldReference]
        if (partitionColumns.contains(fieldRef.name)) {
          bc
        }
        NopExpression()
      case bc: BinaryComparison if bc.left.isInstanceOf[ResolvedFieldReference] =>
        val fieldRef = bc.left.asInstanceOf[ResolvedFieldReference]
        if (partitionColumns.contains(fieldRef.name)) {
          bc
        }
        else {
          NopExpression()
        }
      case ue@(IsNotNull(_) | IsNull(_) | NumericIsNotNull(_)) =>
        val fieldRef = ue.asInstanceOf[UnaryExpression].child.asInstanceOf[ResolvedFieldReference]
        if (partitionColumns.contains(fieldRef.name)) {
          ue
        }
        else {
          NopExpression()
        }
      case e: Expression => if (!e.isInstanceOf[Not]) e else NopExpression()
      case _ => NopExpression()
    }
  }

  def exprToFilterString(expr: Expression): String = {
    expr match {
      case And(left, right) =>
        "((" + exprToFilterString(left) + ") and (" + exprToFilterString(right) + "))"
      case Or(left, right) =>
        "((" + exprToFilterString(left) + ") or (" + exprToFilterString(right) + "))"
      case bc: BinaryComparison if bc.right.isInstanceOf[ResolvedFieldReference] =>
        val fieldRef = bc.right.asInstanceOf[ResolvedFieldReference]
        val literal = bc.left.asInstanceOf[Literal]
        comparisonToString(bc, fieldRef.name, literal)
      case bc: BinaryComparison if bc.left.isInstanceOf[ResolvedFieldReference] =>
        val fieldRef = bc.left.asInstanceOf[ResolvedFieldReference]
        val literal = bc.right.asInstanceOf[Literal]
        comparisonToString(bc, fieldRef.name, literal)
      case ue@(IsNotNull(_) | IsNull(_) | NumericIsNotNull(_)) =>
        comparisonToString(ue, ue.asInstanceOf[UnaryExpression].child
          .asInstanceOf[ResolvedFieldReference].name)
      case _ => throw new IllegalArgumentException("Should not happen. This is a bug.")
    }
  }

  def comparisonToString(expr: Expression, fieldName: String, literal: Literal = null): String = {
    expr match {
      case EqualTo(_,_) => fieldName + "=" + "\"" + literal + "\""
      case GreaterThan(_,_) => fieldName + ">" + "\"" + literal + "\""
      case GreaterThanOrEqual(_,_) => fieldName + ">=" + "\"" + literal + "\""
      case LessThan(_,_) => fieldName + "<" + "\"" + literal + "\""
      case LessThanOrEqual(_,_) => fieldName + "<=" + "\"" + literal + "\""
      case NotEqualTo(_,_) => fieldName + "<>" + "\"" + literal + "\""
      case IsNotNull(_) => fieldName + "<> \"\""
      case IsNull(_) => fieldName + "= \"\""
      case NumericIsNotNull(_) => fieldName + "<> \"\""
      case _ => throw new IllegalArgumentException("Should not happen. This is a bug.")
    }
  }
}

object HCatTableSource {

  class HCatRecord2RowMapper(inputSchema: HCatSchema,
                             outputFields: Seq[(String, TypeInformation[_])])
    extends MapFunction[DefaultHCatRecord, Row] {

    override def map(input: DefaultHCatRecord): Row = {
      val row = new Row(outputFields.size)
      for (i <- 0 until input.size()){
        val field = input.get(i)
        val fieldSchema = inputSchema.get(i)
        row.setField(outputFields.indexWhere(_._1 == fieldSchema.getName),
          convertField(fieldSchema.getType, field))
      }
      row
    }

    def convertField(schemaType: HCatFieldSchema.Type, o: AnyRef): Any = {
      // partition columns are returned as String
      //   Check and convert to actual type.
      schemaType match {
        case HCatFieldSchema.Type.INT =>
          if (o.isInstanceOf[String]) {
            o.asInstanceOf[String].toInt
          }
          else {
            o.asInstanceOf[Int]
          }
        case HCatFieldSchema.Type.TINYINT =>
          if (o.isInstanceOf[String]) {
            o.asInstanceOf[String].toInt.toByte
          }
          else {
            o.asInstanceOf[Byte]
          }
        case HCatFieldSchema.Type.SMALLINT =>
          if (o.isInstanceOf[String]) {
            o.asInstanceOf[String].toInt.toShort
          }
          else {
            o.asInstanceOf[Short]
          }
        case HCatFieldSchema.Type.BIGINT =>
          if (o.isInstanceOf[String]) {
            o.asInstanceOf[String].toLong
          }
          else {
            o.asInstanceOf[Long]
          }
        case HCatFieldSchema.Type.BOOLEAN =>
          if (o.isInstanceOf[String]) {
            o.asInstanceOf[String].toBoolean
          }
          else {
            o.asInstanceOf[Boolean]
          }
        case HCatFieldSchema.Type.FLOAT =>
          if (o.isInstanceOf[String]) {
            o.asInstanceOf[String].toFloat
          }
          else {
            o.asInstanceOf[Float]
          }
        case HCatFieldSchema.Type.DOUBLE =>
          if (o.isInstanceOf[String]) {
            o.asInstanceOf[String].toDouble
          }
          else {
            o.asInstanceOf[Double]
          }
        case HCatFieldSchema.Type.STRING =>
          o
        case HCatFieldSchema.Type.BINARY =>
          if (o.isInstanceOf[String]) {
            throw new RuntimeException("Cannot handle partition keys of type BINARY.")
          }
          else {
            o.asInstanceOf[Array[Byte]]
          }
        case HCatFieldSchema.Type.ARRAY =>
          if (o.isInstanceOf[String]) {
            throw new RuntimeException("Cannot handle partition keys of type ARRAY.")
          }
          else {
            o.asInstanceOf[List[Object]]
          }
        case HCatFieldSchema.Type.MAP =>
          if (o.isInstanceOf[String]) {
            throw new RuntimeException("Cannot handle partition keys of type MAP.")
          }
          else {
            o.asInstanceOf[Map[Object, Object]]
          }
        case HCatFieldSchema.Type.STRUCT =>
          if (o.isInstanceOf[String]) {
            throw new RuntimeException("Cannot handle partition keys of type STRUCT.")
          }
          else {
            o.asInstanceOf[List[Object]]
          }
        case unknownType =>
          throw new RuntimeException("Invalid type " + unknownType +
            " encountered.")
      }
    }
  }
}
