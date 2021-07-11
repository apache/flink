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

package org.apache.flink.table.planner.codegen.sort

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.codegen.CodeGenUtils.{ROW_DATA, newName}
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GenerateUtils}
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec
import org.apache.flink.table.runtime.generated.{GeneratedRecordComparator, RecordComparator}
import org.apache.flink.table.types.logical.RowType

/**
  * A code generator for generating [[RecordComparator]].
  */
object ComparatorCodeGenerator {

  /**
    * Generates a [[RecordComparator]] that can be passed to a Java compiler.
    *
    * @param tableConfig Table config.
    * @param name        Class name of the function.
    *                    Does not need to be unique but has to be a valid Java class identifier.
    * @param inputType   input type.
    * @param sortSpec    sort specification.
    * @return A GeneratedRecordComparator
    */
  def gen(
      tableConfig: TableConfig,
      name: String,
      inputType: RowType,
      sortSpec: SortSpec): GeneratedRecordComparator = {
    val className = newName(name)
    val baseClass = classOf[RecordComparator]

    val ctx = new CodeGeneratorContext(tableConfig)
    val compareCode = GenerateUtils.generateRowCompare(ctx, inputType, sortSpec, "o1", "o2")

    val code =
      j"""
      public class $className implements ${baseClass.getCanonicalName} {

        private final Object[] references;
        ${ctx.reuseMemberCode()}

        public $className(Object[] references) {
          this.references = references;
          ${ctx.reuseInitCode()}
          ${ctx.reuseOpenCode()}
        }

        @Override
        public int compare($ROW_DATA o1, $ROW_DATA o2) {
          $compareCode
          return 0;
        }

      }
      """.stripMargin

    new GeneratedRecordComparator(className, code, ctx.references.toArray)
  }

}
