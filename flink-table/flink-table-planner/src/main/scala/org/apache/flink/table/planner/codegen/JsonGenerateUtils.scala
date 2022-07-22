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
package org.apache.flink.table.planner.codegen

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.RawValue
import org.apache.flink.table.api.{DataTypes, JsonOnNull}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{className, newName, rowFieldReadAccess, typeTerm, ARRAY_DATA, MAP_DATA}
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.{JSON_ARRAY, JSON_OBJECT}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.runtime.functions.SqlJsonUtils
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isCharacterString
import org.apache.flink.table.types.logical._
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks

import org.apache.calcite.rex.{RexCall, RexNode}

import java.time.format.DateTimeFormatter

import scala.annotation.tailrec

/** Utility for generating JSON function calls. */
object JsonGenerateUtils {

  private def jsonUtils = className[SqlJsonUtils]

  /**
   * Returns a term which wraps the given `expression` into a [[JsonNode]]. If the operand
   * represents another JSON construction function, a raw node is used instead.
   */
  def createNodeTerm(
      ctx: CodeGeneratorContext,
      expression: GeneratedExpression,
      operand: RexNode): String = {
    if (isJsonFunctionOperand(operand)) {
      createRawNodeTerm(expression)
    } else {
      createNodeTerm(ctx, expression)
    }
  }

  /**
   * Returns a term which wraps the given `valueExpr` into a [[JsonNode]] of the appropriate type.
   *
   * Does not support nullability.
   */
  def createNodeTerm(ctx: CodeGeneratorContext, valueExpr: GeneratedExpression): String = {
    createNodeTerm(ctx, valueExpr.resultTerm, valueExpr.resultType)
  }

  @tailrec
  private def createNodeTerm(
      ctx: CodeGeneratorContext,
      valueTerm: String,
      valueType: LogicalType): String = {
    val nodeFactoryTerm = s"$jsonUtils.getNodeFactory()"

    valueType.getTypeRoot match {
      case CHAR | VARCHAR => s"$nodeFactoryTerm.textNode($valueTerm.toString())"
      case BOOLEAN => s"$nodeFactoryTerm.booleanNode($valueTerm)"
      case DECIMAL => s"$nodeFactoryTerm.numberNode($valueTerm.toBigDecimal())"
      case TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT | DOUBLE =>
        s"$nodeFactoryTerm.numberNode($valueTerm)"

      case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        val formatter = s"${typeTerm(classOf[DateTimeFormatter])}.ISO_LOCAL_DATE_TIME"
        val isoTerm = s"$valueTerm.toLocalDateTime().format($formatter)"
        valueType.getTypeRoot match {
          case TIMESTAMP_WITHOUT_TIME_ZONE => s"$nodeFactoryTerm.textNode($isoTerm)"
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE => s"""$nodeFactoryTerm.textNode($isoTerm + "Z")"""
        }

      case TIMESTAMP_WITH_TIME_ZONE =>
        throw new CodeGenException(s"'TIMESTAMP WITH TIME ZONE' is not yet supported.")

      case BINARY | VARBINARY =>
        s"$nodeFactoryTerm.binaryNode($valueTerm)"

      case ARRAY =>
        val converterName =
          generateArrayConverter(ctx, valueType.asInstanceOf[ArrayType].getElementType)
        s"$converterName($valueTerm)"

      case ROW | STRUCTURED_TYPE =>
        val converterName = generateRowConverter(ctx, valueType)
        s"$converterName($valueTerm)"

      case MAP =>
        val mapType = valueType.asInstanceOf[MapType]
        val converterName = generateMapConverter(ctx, mapType.getKeyType, mapType.getValueType)
        s"$converterName($valueTerm)"

      case MULTISET =>
        val converterName = generateMapConverter(
          ctx,
          valueType.asInstanceOf[MultisetType].getElementType,
          DataTypes.INT().getLogicalType)
        s"$converterName($valueTerm)"

      case DISTINCT_TYPE =>
        createNodeTerm(ctx, valueTerm, valueType.asInstanceOf[DistinctType].getSourceType)

      case _ =>
        throw new CodeGenException(
          s"Type '$valueType' is not scalar or cannot be converted into JSON.")
    }
  }

  private def createNullableNodeTerm(
      ctx: CodeGeneratorContext,
      containerTerm: String,
      indexTerm: String,
      fieldType: LogicalType): String = {
    val nodeFactoryTerm = s"$jsonUtils.getNodeFactory()"

    val fieldAccessTerm = rowFieldReadAccess(indexTerm, containerTerm, fieldType)

    val valueNodeTerm = createNodeTerm(ctx, fieldAccessTerm, fieldType)

    if (fieldType.isNullable) {
      s"""
         |$containerTerm.isNullAt($indexTerm) ?
         |    (${className[JsonNode]}) $nodeFactoryTerm.nullNode() :
         |    (${className[JsonNode]}) $valueNodeTerm
         |""".stripMargin
    } else {
      valueNodeTerm
    }
  }

  /**
   * Returns a term which wraps the given `valueExpr` as a raw [[JsonNode]].
   *
   * @param valueExpr
   *   Generated expression of the value which should be wrapped.
   * @return
   *   Generate code fragment creating the raw node.
   */
  private def createRawNodeTerm(valueExpr: GeneratedExpression): String = {
    s"""
       |$jsonUtils.getNodeFactory().rawValueNode(
       |    new ${typeTerm(classOf[RawValue])}(${valueExpr.resultTerm}.toString()))
       |""".stripMargin
  }

  /** Convert the operand to [[JsonOnNull]]. */
  def getOnNullBehavior(operand: GeneratedExpression): JsonOnNull = {
    operand.literalValue match {
      case Some(onNull: JsonOnNull) => onNull
      case _ =>
        throw new CodeGenException(
          s"Expected operand to be of type"
            + s"'${typeTerm(classOf[JsonOnNull])}''")
    }
  }

  /**
   * Determines whether the given operand is a call to a JSON function whose result should be
   * inserted as a raw value instead of as a character string.
   */
  def isJsonFunctionOperand(operand: RexNode): Boolean = {
    operand match {
      case rexCall: RexCall =>
        rexCall.getOperator match {
          case JSON_OBJECT | JSON_ARRAY => true
          case _ => false
        }
      case _ => false
    }
  }

  /** Generates a method to convert arrays into [[ArrayNode]]. */
  private def generateArrayConverter(
      ctx: CodeGeneratorContext,
      elementType: LogicalType): String = {
    val methodName = newName("convertArray")
    val methodCode =
      s"""
         |private ${className[ArrayNode]} $methodName($ARRAY_DATA arrData) {
         |    ${className[ArrayNode]} arrNode = $jsonUtils.getNodeFactory().arrayNode();
         |    for (int i = 0; i < arrData.size(); i++) {
         |        arrNode.add(${createNullableNodeTerm(ctx, "arrData", "i", elementType)});
         |    }
         |
         |    return arrNode;
         |}
         |""".stripMargin

    ctx.addReusableMember(methodCode)
    methodName
  }

  /** Generates a method to convert rows into [[ObjectNode]]. */
  private def generateRowConverter(ctx: CodeGeneratorContext, rowType: LogicalType): String = {
    val fieldNames = toScala(LogicalTypeChecks.getFieldNames(rowType))
    val fieldTypes = toScala(LogicalTypeChecks.getFieldTypes(rowType))

    val populateObjectCode = fieldNames.zipWithIndex.map {
      case (fieldName, idx) =>
        val fieldType = fieldTypes(idx)
        s"""
           |objNode.set(
           |    "$fieldName",
           |    ${createNullableNodeTerm(ctx, "rowData", idx.toString, fieldType)});
           |""".stripMargin
    }.mkString

    val methodName = newName("convertRow")
    val methodCode =
      s"""
         |private ${className[ObjectNode]} $methodName(${CodeGenUtils.ROW_DATA} rowData) {
         |    ${className[ObjectNode]} objNode = $jsonUtils.getNodeFactory().objectNode();
         |    $populateObjectCode
         |
         |    return objNode;
         |}
         |""".stripMargin

    ctx.addReusableMember(methodCode)
    methodName
  }

  /** Generates a method to convert maps into [[ObjectNode]]. */
  private def generateMapConverter(
      ctx: CodeGeneratorContext,
      keyType: LogicalType,
      valueType: LogicalType): String = {
    if (!isCharacterString(keyType)) {
      throw new CodeGenException(
        s"Type '$keyType' is not supported for JSON conversion. "
          + "The key type must be a character string.")
    }

    val methodName = newName("convertMap")
    val methodCode =
      s"""
         |private ${className[ObjectNode]} $methodName($MAP_DATA mapData) {
         |    ${className[ObjectNode]} objNode = $jsonUtils.getNodeFactory().objectNode();
         |    for (int i = 0; i < mapData.size(); i++) {
         |        $ARRAY_DATA keyArray = mapData.keyArray();
         |        $ARRAY_DATA valueArray = mapData.valueArray();
         |        java.lang.String key = keyArray.isNullAt(i) ? null :
         |          ${rowFieldReadAccess("i", "keyArray", keyType)}.toString();
         |        if (key == null) {
         |            throw new java.lang.IllegalArgumentException("Key at index " + i
         |                + " was null. This is not supported during conversion to JSON.");
         |        }
         |
         |        objNode.set(
         |            key,
         |            ${createNullableNodeTerm(ctx, "valueArray", "i", valueType)});
         |    }
         |
         |    return objNode;
         |}
         |""".stripMargin

    ctx.addReusableMember(methodCode)
    methodName
  }
}
