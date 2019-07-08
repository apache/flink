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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.api.dag.Transformation
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.codegen.SinkCodeGenerator._
import org.apache.flink.table.codegen.{CodeGenUtils, CodeGeneratorContext}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.calcite.Sink
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.nodes.resource.NodeResourceConfig
import org.apache.flink.table.planner.BatchPlanner
import org.apache.flink.table.sinks.{DataStreamTableSink, RetractStreamTableSink, StreamTableSink, TableSink, UpsertStreamTableSink}
import org.apache.flink.table.types.{ClassLogicalTypeConverter, DataType}
import org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.lang.reflect.Modifier
import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode to to write data into an external sink defined by a [[TableSink]].
  */
class BatchExecSink[T](
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sink: TableSink[T],
    sinkName: String)
  extends Sink(cluster, traitSet, inputRel, sink, sinkName)
  with BatchPhysicalRel
  with BatchExecNode[Any] {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecSink(cluster, traitSet, inputs.get(0), sink, sinkName)
  }

  //~ ExecNode methods -----------------------------------------------------------

  /**
    * For sink operator, the records will not pass through it, so it's DamBehavior is FULL_DAM.
    *
    * @return Returns [[DamBehavior]] of Sink.
    */
  override def getDamBehavior: DamBehavior = DamBehavior.FULL_DAM

  override def getInputNodes: util.List[ExecNode[BatchPlanner, _]] = {
    List(getInput.asInstanceOf[ExecNode[BatchPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[Any] = {
    val resultTransformation = sink match {
      case _: RetractStreamTableSink[T] | _: UpsertStreamTableSink[T] =>
        throw new TableException("RetractStreamTableSink and UpsertStreamTableSink is not" +
          " supported in Batch environment.")

      case streamTableSink: StreamTableSink[T] =>
        // we can insert the bounded DataStream into a StreamTableSink
        val transformation = translateToTransformation(withChangeFlag = false, planner)
        val boundedStream = new DataStream(planner.getExecEnv, transformation)
        val dsSink = streamTableSink.consumeDataStream(boundedStream)
        if (dsSink == null) {
          throw new TableException("The StreamTableSink#consumeDataStream(DataStream) must be " +
            "implemented and return the sink transformation DataStreamSink. " +
            s"However, ${sink.getClass.getCanonicalName} doesn't implement this method.")
        }
        val sinkTransformation = dsSink.getTransformation

        val configSinkParallelism = NodeResourceConfig.getSinkParallelism(
          planner.getTableConfig.getConf)

        val maxSinkParallelism = sinkTransformation.getMaxParallelism

        // only set user's parallelism when user defines a sink parallelism
        if (configSinkParallelism > 0) {
          // set the parallelism when user's parallelism is not larger than max parallelism
          // or max parallelism is not set
          if (maxSinkParallelism < 0 || configSinkParallelism <= maxSinkParallelism) {
            sinkTransformation.setParallelism(configSinkParallelism)
          }
        }

        sinkTransformation

      case dsTableSink: DataStreamTableSink[T] =>
        // In case of table to bounded stream through Batchplannerironment#toBoundedStream, we
        // insert a DataStreamTableSink then wrap it as a LogicalSink, there is no real batch table
        // sink, so we do not need to invoke TableSink#emitBoundedStream and set resource, just a
        // translation to Transformation is ok.
        translateToTransformation(withChangeFlag = dsTableSink.withChangeFlag, planner)

      case _ =>
        throw new TableException(s"Only Support StreamTableSink! " +
          s"However ${sink.getClass.getCanonicalName} is not a StreamTableSink.")
    }
    resultTransformation.asInstanceOf[Transformation[Any]]
  }

  private def translateToTransformation(
      withChangeFlag: Boolean,
      planner: BatchPlanner): Transformation[T] = {
    val config = planner.getTableConfig
    val resultDataType = sink.getConsumedDataType
    val resultType = fromDataTypeToLegacyInfo(resultDataType)
    validateType(resultDataType)
    val inputNode = getInputNodes.get(0)
    inputNode match {
      // Sink's input must be BatchExecNode[BaseRow] now.
      case node: BatchExecNode[BaseRow] =>
        val plan = node.translateToPlan(planner)
        val typeClass = extractTableSinkTypeClass(sink)
        if (CodeGenUtils.isInternalClass(typeClass, resultDataType)) {
          plan.asInstanceOf[Transformation[T]]
        } else {
          val (converterOperator, outputTypeInfo) = generateRowConverterOperator[T](
            CodeGeneratorContext(config),
            config,
            plan.getOutputType.asInstanceOf[BaseRowTypeInfo],
            "SinkConversion",
            None,
            withChangeFlag,
            resultType,
            sink
          )
          new OneInputTransformation(
            plan,
            s"SinkConversionTo${resultType.getTypeClass.getSimpleName}",
            converterOperator,
            outputTypeInfo,
            plan.getParallelism)
        }
      case _ =>
        throw new TableException("Cannot generate BoundedStream due to an invalid logical plan. " +
                                   "This is a bug and should not happen. Please file an issue.")
    }
  }


  /**
    * Validate if class represented by the typeInfo is static and globally accessible
    * @param dataType type to check
    * @throws TableException if type does not meet these criteria
    */
  private def validateType(dataType: DataType): Unit = {
    var clazz = dataType.getConversionClass
    if (clazz == null) {
      clazz = ClassLogicalTypeConverter.getDefaultExternalClassForType(dataType.getLogicalType)
    }
    if ((clazz.isMemberClass && !Modifier.isStatic(clazz.getModifiers)) ||
      !Modifier.isPublic(clazz.getModifiers) ||
      clazz.getCanonicalName == null) {
      throw new TableException(
        s"Class '$clazz' described in type information '$dataType' must be " +
          s"static and globally accessible.")
    }
  }
}
