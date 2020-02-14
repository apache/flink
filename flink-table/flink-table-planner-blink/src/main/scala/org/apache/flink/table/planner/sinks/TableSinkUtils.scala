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

package org.apache.flink.table.planner.sinks

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.api._
import org.apache.flink.table.catalog.{CatalogTable, ObjectIdentifier}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.operations.CatalogSinkModifyOperation
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.utils.RelOptUtils
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.sinks._
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.TypeTransformations.{legacyDecimalToDefaultDecimal, toNullable}
import org.apache.flink.table.types.logical.utils.{LogicalTypeCasts, LogicalTypeChecks}
import org.apache.flink.table.types.logical.{LegacyTypeInformationType, LogicalType, RowType}
import org.apache.flink.table.types.utils.DataTypeUtils
import org.apache.flink.table.types.utils.TypeConversions.{fromLegacyInfoToDataType, fromLogicalToDataType}
import org.apache.flink.table.utils.{TableSchemaUtils, TypeMappingUtils}
import org.apache.flink.types.Row

import org.apache.calcite.rel.RelNode

import _root_.scala.collection.JavaConversions._

object TableSinkUtils {

  /**
    * Checks if the given query can be written into the given sink. It checks the field types
    * should be compatible (types should equal including precisions). If types are not compatible,
    * but can be implicitly casted, a cast projection will be applied. Otherwise, an exception will
    * be thrown.
    *
    * @param query the query to be checked
    * @param sinkSchema the schema of sink to be checked
    * @param typeFactory type factory
    * @return the query RelNode which may be applied the implicitly cast projection.
    */
  def validateSchemaAndApplyImplicitCast(
      query: RelNode,
      sinkSchema: TableSchema,
      typeFactory: FlinkTypeFactory,
      sinkIdentifier: Option[String] = None): RelNode = {

    val queryLogicalType = FlinkTypeFactory.toLogicalRowType(query.getRowType)
    val sinkLogicalType = DataTypeUtils
      // we recognize legacy decimal is the same to default decimal
      .transform(sinkSchema.toRowDataType, legacyDecimalToDefaultDecimal)
      .getLogicalType
      .asInstanceOf[RowType]
    if (LogicalTypeCasts.supportsImplicitCast(queryLogicalType, sinkLogicalType)) {
      // the query can be written into sink
      // but we may need to add a cast project if the types are not compatible
      if (LogicalTypeChecks.areTypesCompatible(
          nullableLogicalType(queryLogicalType), nullableLogicalType(sinkLogicalType))) {
        // types are compatible excepts nullable, do not need cast project
        // we ignores nullable to avoid cast project as cast non-null to nullable is redundant
        query
      } else {
        // otherwise, add a cast project
        val castedDataType = typeFactory.buildRelNodeRowType(
          sinkLogicalType.getFieldNames,
          sinkLogicalType.getFields.map(_.getType))
        RelOptUtils.createCastRel(query, castedDataType)
      }
    } else {
      // format query and sink schema strings
      val srcSchema = queryLogicalType.getFields
        .map(f => s"${f.getName}: ${f.getType}")
        .mkString("[", ", ", "]")
      val sinkSchema = sinkLogicalType.getFields
        .map(f => s"${f.getName}: ${f.getType}")
        .mkString("[", ", ", "]")

      val sinkDesc: String = sinkIdentifier.getOrElse("")

      throw new ValidationException(
        s"Field types of query result and registered TableSink $sinkDesc do not match.\n" +
          s"Query schema: $srcSchema\n" +
          s"Sink schema: $sinkSchema")
    }
  }

  /**
    * Make the logical type nullable recursively.
    */
  private def nullableLogicalType(logicalType: LogicalType): LogicalType = {
    DataTypeUtils.transform(fromLogicalToDataType(logicalType), toNullable).getLogicalType
  }

  /**
    * It checks whether the [[TableSink]] is compatible to the INSERT INTO clause, e.g.
    * whether the sink is a [[PartitionableTableSink]] and the partitions are valid.
    *
    * @param sinkOperation The sink operation with the query that is supposed to be written.
    * @param sinkIdentifier Tha path of the sink. It is needed just for logging. It does not
    *                      participate in the validation.
    * @param sink     The sink that we want to write to.
    * @param partitionKeys The partition keys of this table.
    */
  def validateTableSink(
      sinkOperation: CatalogSinkModifyOperation,
      sinkIdentifier: ObjectIdentifier,
      sink: TableSink[_],
      partitionKeys: Seq[String]): Unit = {

    // check partitions are valid
    if (partitionKeys.nonEmpty) {
      sink match {
        case _: PartitionableTableSink =>
        case _ => throw new ValidationException("We need PartitionableTableSink to write data to" +
            s" partitioned table: $sinkIdentifier")
      }
    }

    val staticPartitions = sinkOperation.getStaticPartitions
    if (staticPartitions != null && !staticPartitions.isEmpty) {
      staticPartitions.map(_._1) foreach { p =>
        if (!partitionKeys.contains(p)) {
          throw new ValidationException(s"Static partition column $p should be in the partition" +
              s" fields list $partitionKeys for Table($sinkIdentifier).")
        }
      }
    }

    sink match {
      case overwritableTableSink: OverwritableTableSink =>
        overwritableTableSink.setOverwrite(sinkOperation.isOverwrite)
      case _ =>
        assert(!sinkOperation.isOverwrite, "INSERT OVERWRITE requires " +
          s"${classOf[OverwritableTableSink].getSimpleName} but actually got " +
          sink.getClass.getName)
    }
  }

  /**
    * Inferences the physical schema of [[TableSink]], the physical schema ignores change flag
    * field and normalizes physical types (can be generic type or POJO type) into [[TableSchema]].
    * @param queryLogicalType the logical type of query, will be used to full-fill sink physical
    *                         schema if the sink physical type is not specified.
    * @param sink the instance of [[TableSink]]
    */
  def inferSinkPhysicalSchema(
      queryLogicalType: RowType,
      sink: TableSink[_]): TableSchema = {
    val withChangeFlag = sink match {
      case _: RetractStreamTableSink[_] | _: UpsertStreamTableSink[_] => true
      case _: StreamTableSink[_] => false
      case dsts: DataStreamTableSink[_] => dsts.withChangeFlag
    }
    inferSinkPhysicalSchema(sink.getConsumedDataType, queryLogicalType, withChangeFlag)
  }

  /**
    * Inferences the physical schema of [[TableSink]], the physical schema ignores change flag
    * field and normalizes physical types (can be generic type or POJO type) into [[TableSchema]].
    *
    * @param consumedDataType the consumed data type of sink
    * @param queryLogicalType the logical type of query, will be used to full-fill sink physical
    *                         schema if the sink physical type is not specified.
    * @param withChangeFlag true if the emitted records contains change flags.
    */
  def inferSinkPhysicalSchema(
      consumedDataType: DataType,
      queryLogicalType: RowType,
      withChangeFlag: Boolean): TableSchema = {
    // the requested output physical type which ignores the flag field
    val requestedOutputType = inferSinkPhysicalDataType(
      consumedDataType,
      queryLogicalType,
      withChangeFlag)
    if (LogicalTypeChecks.isCompositeType(requestedOutputType.getLogicalType)) {
      DataTypeUtils.expandCompositeTypeToSchema(requestedOutputType)
    } else {
      // atomic type
      TableSchema.builder().field("f0", requestedOutputType).build()
    }
  }

  /**
    * Inferences the physical data type of [[TableSink]], the physical data type ignores
    * the change flag field.
    *
    * @param consumedDataType the consumed data type of sink
    * @param queryLogicalType the logical type of query, will be used to full-fill sink physical
    *                         schema if the sink physical type is not specified.
    * @param withChangeFlag true if the emitted records contains change flags.
    */
  def inferSinkPhysicalDataType(
      consumedDataType: DataType,
      queryLogicalType: RowType,
      withChangeFlag: Boolean): DataType = {
    consumedDataType.getLogicalType match {
      case lt: LegacyTypeInformationType[_] =>
        val requestedTypeInfo = if (withChangeFlag) {
          lt.getTypeInformation match {
            // Scala tuple
            case t: CaseClassTypeInfo[_]
              if t.getTypeClass == classOf[(_, _)] && t.getTypeAt(0) == Types.BOOLEAN =>
              t.getTypeAt[Any](1)
            // Java tuple
            case t: TupleTypeInfo[_]
              if t.getTypeClass == classOf[JTuple2[_, _]] && t.getTypeAt(0) == Types.BOOLEAN =>
              t.getTypeAt[Any](1)
            case _ => throw new TableException(
              "Don't support " + consumedDataType + " conversion for the retract sink")
          }
        } else {
          lt.getTypeInformation
        }
        // The tpe may been inferred by invoking [[TypeExtractor.createTypeInfo]] based the
        // class of the resulting type. For example, converts the given [[Table]] into
        // an append [[DataStream]]. If the class is Row, then the return type only is
        // [[GenericTypeInfo[Row]]. So it should convert to the [[RowTypeInfo]] in order
        // to better serialize performance.
        requestedTypeInfo match {
          case gt: GenericTypeInfo[Row] if gt.getTypeClass == classOf[Row] =>
            fromLogicalToDataType(queryLogicalType).bridgedTo(classOf[Row])
          case gt: GenericTypeInfo[BaseRow] if gt.getTypeClass == classOf[BaseRow] =>
            fromLogicalToDataType(queryLogicalType).bridgedTo(classOf[BaseRow])
          case bt: BaseRowTypeInfo =>
            val fields = bt.getFieldNames.zip(bt.getLogicalTypes).map { case (n, t) =>
              DataTypes.FIELD(n, fromLogicalToDataType(t))
            }
            DataTypes.ROW(fields: _*).bridgedTo(classOf[BaseRow])
          case _ =>
            fromLegacyInfoToDataType(requestedTypeInfo)
        }

      case _ =>
        consumedDataType
    }
  }

  /**
    * Checks whether the logical schema (from DDL) and physical schema
    * (from TableSink.getConsumedDataType()) of sink are compatible.
    *
    * @param catalogTable the catalog table of sink
    * @param sink the instance of [[TableSink]]
    * @param queryLogicalType the logical type of query
    */
  def validateLogicalPhysicalTypesCompatible(
      catalogTable: CatalogTable,
      sink: TableSink[_],
      queryLogicalType: RowType): Unit = {
    // there may be generated columns in DDL, only get the physical part of DDL
    val logicalSchema = TableSchemaUtils.getPhysicalSchema(catalogTable.getSchema)
    // infer the physical schema from TableSink#getConsumedDataType
    val physicalSchema = TableSinkUtils.inferSinkPhysicalSchema(
      queryLogicalType,
      sink)
    // check for valid type info
    if (logicalSchema.getFieldCount != physicalSchema.getFieldCount) {
      throw new ValidationException("The field count of logical schema of the table does" +
        " not match with the field count of physical schema\n. " +
        s"The logical schema: [${logicalSchema.getFieldDataTypes.mkString(",")}]\n" +
        s"The physical schema: [${physicalSchema.getFieldDataTypes.mkString(",")}].")
    }

    for (i <- 0 until logicalSchema.getFieldCount) {
      val logicalFieldType = DataTypeUtils.transform(
        logicalSchema.getFieldDataTypes()(i), toNullable) // ignore nullabilities
      val logicalFieldName = logicalSchema.getFieldNames()(i)
      val physicalFieldType = DataTypeUtils.transform(
        physicalSchema.getFieldDataTypes()(i), toNullable) // ignore nullabilities
      val physicalFieldName = physicalSchema.getFieldNames()(i)
      TypeMappingUtils.checkPhysicalLogicalTypeCompatible(
        physicalFieldType.getLogicalType,
        logicalFieldType.getLogicalType,
        physicalFieldName,
        logicalFieldName,
        false)
     }
  }
}
