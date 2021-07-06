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

package org.apache.flink.table.planner.codegen.over

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.codegen.CodeGenUtils.{ROW_DATA, newName}
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.codegen.{CodeGenUtils, CodeGeneratorContext, GenerateUtils}
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec
import org.apache.flink.table.runtime.generated.{GeneratedRecordComparator, RecordComparator}
import org.apache.flink.table.types.logical.RowType

/**
  * RANGE allow the compound ORDER BY and the random type when the bound is current row.
  */
class MultiFieldRangeBoundComparatorCodeGenerator(
    conf: TableConfig,
    inputType: RowType,
    sortSpec: SortSpec,
    isLowerBound: Boolean = true) {

  def generateBoundComparator(name: String): GeneratedRecordComparator = {
    val className = newName(name)
    val input = CodeGenUtils.DEFAULT_INPUT1_TERM
    val current = CodeGenUtils.DEFAULT_INPUT2_TERM

    // In order to avoid the loss of precision in long cast to int.
    def generateReturnCode(comp: String): String = {
      if (isLowerBound) s"return $comp >= 0 ? 1 : -1;" else s"return $comp > 0 ? 1 : -1;"
    }

    val ctx = CodeGeneratorContext(conf)
    val compareCode = GenerateUtils.generateRowCompare(ctx, inputType, sortSpec, input, current)

    val code =
      j"""
      public class $className implements ${classOf[RecordComparator].getCanonicalName} {

        private final Object[] references;
        ${ctx.reuseMemberCode()}

        public $className(Object[] references) {
          this.references = references;
          ${ctx.reuseInitCode()}
          ${ctx.reuseOpenCode()}
        }

        @Override
        public int compare($ROW_DATA $input, $ROW_DATA $current) {
          int ret = compareInternal($input, $current);
          ${generateReturnCode("ret")}
        }

        private int compareInternal($ROW_DATA $input, $ROW_DATA $current) {
          $compareCode
          return 0;
        }

      }
      """.stripMargin

    new GeneratedRecordComparator(
      className, code, ctx.references.toArray, ctx.tableConfig.getConfiguration)
  }
}

