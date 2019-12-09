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

import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, PojoTypeInfo, RowTypeInfo, TupleTypeInfo, TupleTypeInfoBase, TypeExtractor}
import org.apache.flink.api.scala.createTuple2TypeInformation
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{Table, TableConfig, TableException, Types}
import org.apache.flink.table.dataformat.util.BaseRowUtil
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.planner.codegen.CodeGenUtils.genToExternal
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.generateCollect
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.areTypesCompatible
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.types.Row

object SinkCodeGenerator {

  /** Code gen a operator to convert internal type rows to external type. **/
  def generateRowConverterOperator[OUT](
      ctx: CodeGeneratorContext,
      config: TableConfig,
      inputTypeInfo: BaseRowTypeInfo,
      operatorName: String,
      rowtimeField: Option[Int],
      withChangeFlag: Boolean,
      resultType: TypeInformation[_],
      sink: TableSink[_]): (CodeGenOperatorFactory[OUT], TypeInformation[OUT]) = {

    val requestedTypeInfo = if (withChangeFlag) {
      resultType match {
        // Scala tuple
        case t: CaseClassTypeInfo[_]
          if t.getTypeClass == classOf[(_, _)] && t.getTypeAt(0) == Types.BOOLEAN =>
          t.getTypeAt[Any](1)
        // Java tuple
        case t: TupleTypeInfo[_]
          if t.getTypeClass == classOf[JTuple2[_, _]] && t.getTypeAt(0) == Types.BOOLEAN =>
          t.getTypeAt[Any](1)
        case _ => throw new TableException(
          "Don't support " + resultType + " conversion for the retract sink")
      }
    } else {
      resultType
    }

    /**
      * The tpe may been inferred by invoking [[TypeExtractor.createTypeInfo]] based the class of
      * the resulting type. For example, converts the given [[Table]] into an append [[DataStream]].
      * If the class is Row, then the return type only is [[GenericTypeInfo[Row]]. So it should
      * convert to the [[RowTypeInfo]] in order to better serialize performance.
      *
      */
    val convertOutputType = requestedTypeInfo match {
      case gt: GenericTypeInfo[Row] if gt.getTypeClass == classOf[Row] =>
        new RowTypeInfo(
          inputTypeInfo.getFieldTypes,
          inputTypeInfo.getFieldNames)
      case gt: GenericTypeInfo[BaseRow] if gt.getTypeClass == classOf[BaseRow] =>
        new BaseRowTypeInfo(
          inputTypeInfo.getLogicalTypes,
          inputTypeInfo.getFieldNames)
      case _ => requestedTypeInfo
    }

    checkRowConverterValid(inputTypeInfo, convertOutputType)

    //update out put type info
    val outputTypeInfo = if (withChangeFlag) {
      resultType match {
        // Scala tuple
        case t: CaseClassTypeInfo[_]
          if t.getTypeClass == classOf[(_, _)] && t.getTypeAt(0) == Types.BOOLEAN =>
          createTuple2TypeInformation(t.getTypeAt(0), convertOutputType)
        // Java tuple
        case t: TupleTypeInfo[_]
          if t.getTypeClass == classOf[JTuple2[_, _]] && t.getTypeAt(0) == Types.BOOLEAN =>
          new TupleTypeInfo(t.getTypeAt(0), convertOutputType)
      }
    } else {
      convertOutputType
    }

    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    var afterIndexModify = inputTerm
    val fieldIndexProcessCode =
      if (!resultType.isInstanceOf[PojoTypeInfo[_]]) {
        ""
      } else {
        // field index may change (pojo)
        val mapping = convertOutputType match {
          case ct: CompositeType[_] => ct.getFieldNames.map {
            name =>
              val index = inputTypeInfo.getFieldIndex(name)
              if (index < 0) {
                throw new TableException(
                  s"$name is not found in ${inputTypeInfo.getFieldNames.mkString(", ")}")
              }
              index
          }
          case _ => Array(0)
        }

        val resultGenerator = new ExprCodeGenerator(ctx, false).bindInput(
          fromTypeInfoToLogicalType(inputTypeInfo),
          inputTerm,
          inputFieldMapping = Option(mapping))
        val outputBaseRowType = new BaseRowTypeInfo(
            getCompositeTypes(convertOutputType).map(fromTypeInfoToLogicalType): _*)
        val conversion = resultGenerator.generateConverterResultExpression(
          outputBaseRowType.toRowType,
          classOf[GenericRow])
        afterIndexModify = CodeGenUtils.newName("afterIndexModify")
        s"""
           |${conversion.code}
           |${classOf[BaseRow].getCanonicalName} $afterIndexModify = ${conversion.resultTerm};
           |""".stripMargin
      }

    val retractProcessCode = if (!withChangeFlag) {
      generateCollect(
        genToExternal(ctx, fromLegacyInfoToDataType(outputTypeInfo), afterIndexModify))
    } else {
      val flagResultTerm =
        s"${classOf[BaseRowUtil].getCanonicalName}.isAccumulateMsg($afterIndexModify)"
      val resultTerm = CodeGenUtils.newName("result")
      val genericRowField = classOf[GenericRow].getCanonicalName
      s"""
         |$genericRowField $resultTerm = new $genericRowField(2);
         |$resultTerm.setField(0, $flagResultTerm);
         |$resultTerm.setField(1, $afterIndexModify);
         |${generateCollect(
        genToExternal(ctx, fromLegacyInfoToDataType(outputTypeInfo), resultTerm))}
          """.stripMargin
    }

    val generated = OperatorCodeGenerator.generateOneInputStreamOperator[BaseRow, OUT](
      ctx,
      operatorName,
      s"""
         |$fieldIndexProcessCode
         |$retractProcessCode
         |""".stripMargin,
      fromTypeInfoToLogicalType(inputTypeInfo))
    (new CodeGenOperatorFactory[OUT](generated), outputTypeInfo.asInstanceOf[TypeInformation[OUT]])
  }

  private def checkRowConverterValid[OUT](
      inputTypeInfo: BaseRowTypeInfo,
      requestedTypeInfo: TypeInformation[OUT]): Unit = {

    val fieldTypes = inputTypeInfo.getFieldTypes
    val fieldNames = inputTypeInfo.getFieldNames

    // check for valid type info
    if (!requestedTypeInfo.isInstanceOf[GenericTypeInfo[_]] &&
        requestedTypeInfo.getArity != fieldTypes.length) {
      throw new TableException(
        s"Arity [${fieldTypes.length}] of result [$fieldTypes] does not match " +
            s"the number[${requestedTypeInfo.getArity}] of requested type [$requestedTypeInfo].")
    }

    // check requested types

    def validateFieldType(fieldType: TypeInformation[_]): Unit = fieldType match {
      case _: TimeIndicatorTypeInfo =>
        throw new TableException("The time indicator type is an internal type only.")
      case _ => // ok
    }

    requestedTypeInfo match {
      // POJO type requested
      case pt: PojoTypeInfo[_] =>
        fieldNames.zip(fieldTypes) foreach {
          case (fName, fType) =>
            val pojoIdx = pt.getFieldIndex(fName)
            if (pojoIdx < 0) {
              throw new TableException(s"POJO does not define field name: $fName")
            }
            val requestedTypeInfo = pt.getTypeAt(pojoIdx)
            validateFieldType(requestedTypeInfo)
            if (fType != requestedTypeInfo) {
              throw new TableException(s"Result field '$fName' does not match requested type. " +
                  s"Requested: $requestedTypeInfo; Actual: $fType")
            }
        }

      // Tuple/Case class/Row type requested
      case tt: TupleTypeInfoBase[_] =>
        fieldTypes.zipWithIndex foreach {
          case (fieldTypeInfo: GenericTypeInfo[_], i) =>
            val requestedTypeInfo = tt.getTypeAt(i)
            if (!requestedTypeInfo.isInstanceOf[GenericTypeInfo[Object]]) {
              throw new TableException(
                s"Result field '${fieldNames(i)}' does not match requested type. " +
                    s"Requested: $requestedTypeInfo; Actual: $fieldTypeInfo")
            }
          case (fieldTypeInfo, i) =>
            val requestedTypeInfo = tt.getTypeAt(i)
            validateFieldType(requestedTypeInfo)
            if (!areTypesCompatible(
              fromTypeInfoToLogicalType(fieldTypeInfo),
              fromTypeInfoToLogicalType(requestedTypeInfo)) &&
              !requestedTypeInfo.isInstanceOf[GenericTypeInfo[Object]]) {
              val fieldNames = tt.getFieldNames
              throw new TableException(s"Result field '${fieldNames(i)}' does not match requested" +
                  s" type. Requested: $requestedTypeInfo; Actual: $fieldTypeInfo")
            }
        }

      // Atomic type requested
      case at: AtomicType[_] =>
        if (fieldTypes.size != 1) {
          throw new TableException(s"Requested result type is an atomic type but " +
              s"result[$fieldTypes] has more or less than a single field.")
        }
        val requestedTypeInfo = fieldTypes.head
        validateFieldType(requestedTypeInfo)
        if (requestedTypeInfo != at) {
          throw new TableException(s"Result field does not match requested type. " +
              s"Requested: $at; Actual: $requestedTypeInfo")
        }

      case _ =>
        throw new TableException(s"Unsupported result type: $requestedTypeInfo")
    }
  }

  def getCompositeTypes(t: TypeInformation[_]): Array[TypeInformation[_]] = t match {
    case ct: CompositeType[_] => (0 until ct.getArity).map(ct.getTypeAt).toArray
    case _ => Array[TypeInformation[_]](t)
  }
}
