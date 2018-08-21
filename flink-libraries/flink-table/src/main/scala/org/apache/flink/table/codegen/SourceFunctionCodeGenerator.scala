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

package org.apache.flink.table.codegen

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableConfig
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.table.codegen.CodeGenUtils.newName
import org.apache.flink.util.Collector

/**
  * A code generator for generating Flink [[RichSourceFunction]].
  *
  * @param config configuration that determines runtime behavior
  */
class SourceFunctionCodeGenerator[T](
  config: TableConfig,
  outputType: TypeInformation[T])
  extends CodeGenerator(config, false, new RowTypeInfo(), None, None) {

  def generateSourceFunction(
    name: String,
    generatedFunc: GeneratedExpression)
  : GeneratedFunction[RichSourceFunction[T], T] = {

    val funcName = newName(name)
    val collectorName = newName("TableFunctionSourceCollector")
    val configName = classOf[Configuration].getCanonicalName

    val functionCode =
      s"""
         |public class $funcName extends ${classOf[RichSourceFunction[_]].getCanonicalName} {
         |
         |  ${reuseMemberCode()}
         |
         |  private ${collectorName} collectorProxy;
         |
         |  @Override
         |  public void open(${configName} parameters) throws Exception {
         |    ${reuseInitCode()}
         |    ${reuseOpenCode()}
         |    collectorProxy = new ${collectorName}();
         |  }
         |
         |  @Override
         |  public void close() throws Exception {
         |    ${reuseCloseCode()}
         |  }
         |
         |  void run(${classOf[SourceContext[_]].getCanonicalName} ctx) throws Exception {
         |    collectorProxy.setContext(ctx);
         |    ${generatedFunc.resultTerm}.setCollector(collectorProxy);
         |    ${generatedFunc.code}
         |  }
         |
         |  void cancel() {
         |
         |  }
         |
         |  class ${collectorName} implements ${classOf[Collector[_]].getCanonicalName} {
         |
         |    private ${classOf[SourceContext[_]].getCanonicalName} ctx;
         |
         |    public void setContext(${classOf[SourceContext[_]].getCanonicalName} ctx) {
         |      this.ctx = ctx;
         |    }
         |
         |    public void collect(Object record) {
         |      ctx.collect(record);
         |    }
         |
         |    public void close() {
         |
         |    }
         |  }
         |}
       """.stripMargin

    GeneratedFunction(funcName, outputType, functionCode)
  }

}
