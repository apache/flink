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
package org.apache.flink.table.planner.codegen.runtimefilter

import org.apache.flink.runtime.operators.util.BloomFilter
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, OperatorCodeGenerator, ProjectionCodeGenerator}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{newName, ROW_DATA}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.INPUT_SELECTION
import org.apache.flink.table.planner.typeutils.RowTypeUtils
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.util.StreamRecordCollector
import org.apache.flink.table.types.logical.RowType

/** Operator code generator for runtime filter operator. */
object RuntimeFilterCodeGenerator {
  def gen(
      ctx: CodeGeneratorContext,
      buildType: RowType,
      probeType: RowType,
      probeIndices: Array[Int]): CodeGenOperatorFactory[RowData] = {
    val probeGenProj = ProjectionCodeGenerator.generateProjection(
      ctx,
      "RuntimeFilterProjection",
      probeType,
      RowTypeUtils.projectRowType(probeType, probeIndices),
      probeIndices)
    ctx.addReusableInnerClass(probeGenProj.getClassName, probeGenProj.getCode)

    val probeProjection = newName("probeToBinaryRow")
    ctx.addReusableMember(s"private transient ${probeGenProj.getClassName} $probeProjection;")
    val probeProjRefs = ctx.addReusableObject(probeGenProj.getReferences, "probeProjRefs", null)
    ctx.addReusableOpenStatement(
      s"$probeProjection = new ${probeGenProj.getClassName}($probeProjRefs);")

    val buildEnd = newName("buildEnd")
    ctx.addReusableMember(s"private transient boolean $buildEnd;")
    ctx.addReusableOpenStatement(s"$buildEnd = false;")

    val filter = newName("filter")
    val filterClass = classOf[BloomFilter].getCanonicalName
    ctx.addReusableMember(s"private transient $filterClass $filter;")

    val collector = newName("collector")
    val collectorClass = classOf[StreamRecordCollector[_]].getCanonicalName
    ctx.addReusableMember(s"private transient $collectorClass<$ROW_DATA> $collector;")
    ctx.addReusableOpenStatement(s"$collector = new $collectorClass<>(output);")

    val input1Item = "input1"
    val input2Item = "input2"

    val processElement1Code =
      s"""
         |if ($buildEnd) {
         |    throw new IllegalStateException("Should not build ended.");
         |}
         |if ($filter == null && !$input1Item.isNullAt(1)) {
         |    $filter = $filterClass.fromBytes($input1Item.getBinary(1));
         |}
         |""".stripMargin

    val processElement2Code =
      s"""
         |if (!$buildEnd) {
         |    throw new IllegalStateException("Should build ended.");
         |}
         |if ($filter != null) {
         |    final int hashCode = $probeProjection.apply($input2Item).hashCode();
         |    if ($filter.testHash(hashCode)) {
         |        $collector.collect($input2Item);
         |    }
         |} else {
         |    $collector.collect($input2Item);
         |}
         |""".stripMargin

    val nextSelectionCode = s"return $buildEnd ? $INPUT_SELECTION.SECOND : $INPUT_SELECTION.FIRST;"

    val endInputCode1 =
      s"""
         |if ($buildEnd) {
         |    throw new IllegalStateException("Should not build ended.");
         |}
         |LOG.info("Finish build phase.");
         |$buildEnd = true;
         |""".stripMargin

    val endInputCode2 =
      s"""
         |if (!$buildEnd) {
         |    throw new IllegalStateException("Should build ended.");
         |}
         |LOG.info("Finish probe phase.");
         |""".stripMargin

    new CodeGenOperatorFactory[RowData](
      OperatorCodeGenerator.generateTwoInputStreamOperator(
        ctx,
        "RuntimeFilterOperator",
        processElement1Code,
        processElement2Code,
        buildType,
        probeType,
        input1Item,
        input2Item,
        nextSelectionCode = Some(nextSelectionCode),
        endInputCode1 = Some(endInputCode1),
        endInputCode2 = Some(endInputCode2)
      ))
  }
}
