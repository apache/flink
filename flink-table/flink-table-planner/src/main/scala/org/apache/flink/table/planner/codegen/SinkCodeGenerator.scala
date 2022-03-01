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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.createTuple2TypeInformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.util.RowDataUtil
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.planner.codegen.CodeGenUtils.genToExternalConverterWithLegacy
import org.apache.flink.table.planner.codegen.GeneratedExpression.NO_CODE
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.{generateCollect, generateCollectWithTimestamp}
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType

import scala.collection.JavaConverters._

object SinkCodeGenerator {

  def deriveSinkOutputTypeInfo[OUT](
      sink: TableSink[_],
      physicalOutputType: DataType,
      withChangeFlag: Boolean): TypeInformation[OUT] = {
    val physicalTypeInfo = fromDataTypeToTypeInfo(physicalOutputType)
    val outputTypeInfo = if (withChangeFlag) {
      val consumedClass = sink.getConsumedDataType.getConversionClass
      if (consumedClass == classOf[(_, _)]) {
        createTuple2TypeInformation(Types.BOOLEAN, physicalTypeInfo)
      } else if (consumedClass == classOf[JTuple2[_, _]]) {
        new TupleTypeInfo(Types.BOOLEAN, physicalTypeInfo)
      } else {
        throw new TableException("This should not happen.")
      }
    } else {
      physicalTypeInfo
    }
    outputTypeInfo.asInstanceOf[TypeInformation[OUT]]
  }

  /** Code gen a operator to convert internal type rows to external type. **/
  def generateRowConverterOperator[OUT](
      ctx: CodeGeneratorContext,
      inputRowType: RowType,
      sink: TableSink[_],
      physicalOutputType: DataType,
      withChangeFlag: Boolean,
      operatorName: String,
      rowtimeIndex: Int = -1): CodeGenOperatorFactory[OUT] = {
    val physicalTypeInfo = fromDataTypeToTypeInfo(physicalOutputType)
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    var afterIndexModify = inputTerm
    var modifiedRowtimeIndex = rowtimeIndex
    val fieldIndexProcessCode = physicalTypeInfo match {
      case pojo: PojoTypeInfo[_] =>
        val mapping = pojo.getFieldNames.map { name =>
          val index = inputRowType.getFieldIndex(name)
          if (index < 0) {
            throw new TableException(
              s"$name is not found in ${inputRowType.getFieldNames.asScala.mkString(", ")}")
          }
          index
        }
        val resultGenerator = new ExprCodeGenerator(ctx, false)
          .bindInput(
            inputRowType,
            inputTerm,
            inputFieldMapping = Option(mapping))
        val outputRowType = RowType.of(
          (0 until pojo.getArity)
            .map(pojo.getTypeAt)
            .map(fromTypeInfoToLogicalType): _*)
        if (rowtimeIndex >= 0) {
          modifiedRowtimeIndex = outputRowType.getFieldIndex(
            inputRowType.getFieldNames.get(rowtimeIndex))
        }
        val conversion = resultGenerator.generateConverterResultExpression(
          outputRowType,
          classOf[GenericRowData])
        afterIndexModify = CodeGenUtils.newName("afterIndexModify")
        s"""
           |${conversion.code}
           |${conversion.resultTerm}.setRowKind(${inputTerm}.getRowKind());
           |${classOf[RowData].getCanonicalName} $afterIndexModify = ${conversion.resultTerm};
           |""".stripMargin
      case _ =>
        NO_CODE
    }

    val consumedDataType = sink.getConsumedDataType
    // still uses the old conversion stack due to FLINK-18701
    val outTerm = genToExternalConverterWithLegacy(ctx, physicalOutputType, afterIndexModify)
    val retractProcessCode = if (withChangeFlag) {
      val flagResultTerm =
        s"${classOf[RowDataUtil].getCanonicalName}.isAccumulateMsg($afterIndexModify)"
      val resultTerm = CodeGenUtils.newName("result")
      if (consumedDataType.getConversionClass == classOf[JTuple2[_, _]]) {
        // Java Tuple2
        val tupleClass = consumedDataType.getConversionClass.getCanonicalName
        s"""
           |$tupleClass $resultTerm = new $tupleClass();
           |$resultTerm.setField($flagResultTerm, 0);
           |$resultTerm.setField($outTerm, 1);
           |${generateCollectCode(afterIndexModify, resultTerm, modifiedRowtimeIndex)}
         """.stripMargin
      } else {
        // Scala Case Class
        val tupleClass = consumedDataType.getConversionClass.getCanonicalName
        val scalaTupleSerializer = fromDataTypeToTypeInfo(consumedDataType)
          .createSerializer(new ExecutionConfig)
          .asInstanceOf[TupleSerializerBase[_]]
        val serializerTerm = ctx.addReusableObject(
          scalaTupleSerializer,
          "serializer",
          classOf[TupleSerializerBase[_]].getCanonicalName)
        val fieldsTerm = CodeGenUtils.newName("fields")

        s"""
           |Object[] $fieldsTerm = new Object[2];
           |$fieldsTerm[0] = $flagResultTerm;
           |$fieldsTerm[1] = $outTerm;
           |$tupleClass $resultTerm = ($tupleClass) $serializerTerm.createInstance($fieldsTerm);
           |${generateCollectCode(afterIndexModify, resultTerm, modifiedRowtimeIndex)}
         """.stripMargin
      }
    } else {
      generateCollectCode(afterIndexModify, outTerm, modifiedRowtimeIndex)
    }

    val generated = OperatorCodeGenerator.generateOneInputStreamOperator[RowData, OUT](
      ctx,
      operatorName,
      s"""
         |$fieldIndexProcessCode
         |$retractProcessCode
         |""".stripMargin,
      inputRowType)
    new CodeGenOperatorFactory[OUT](generated)
  }

  private def generateCollectCode(
      afterIndexModify: String,
      resultTerm: String,
      modifiedRowtimeIndex: Int): String = {
    if (modifiedRowtimeIndex >= 0) {
      val rowtimeTerm = CodeGenUtils.newName("rowtime")
      s"""
         | Long $rowtimeTerm =
         | $afterIndexModify.getTimestamp($modifiedRowtimeIndex, 3).getMillisecond();
         | ${generateCollectWithTimestamp(resultTerm, rowtimeTerm)}
          """.stripMargin
    } else {
      generateCollect(resultTerm)
    }
  }
}
