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
package org.apache.flink.api.table.codegen

import java.io.StringReader

import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.table.TableConfig
import org.apache.flink.api.table.codegen.Indenter._
import org.apache.flink.api.table.expressions.{Expression, NopExpression}
import org.slf4j.LoggerFactory

/**
 * Code generator for assembling the result of a binary operation.
 */
class GenerateJoin[L, R, O](
    leftTypeInfo: CompositeType[L],
    rightTypeInfo: CompositeType[R],
    resultTypeInfo: CompositeType[O],
    predicate: Expression,
    outputFields: Seq[Expression],
    cl: ClassLoader,
    config: TableConfig)
  extends GenerateResultAssembler[FlatJoinFunction[L, R, O]](
    Seq(("in0", leftTypeInfo), ("in1", rightTypeInfo)),
    cl = cl,
    config) {

  val LOG = LoggerFactory.getLogger(this.getClass)


  override protected def generateInternal(): FlatJoinFunction[L, R, O] = {

    val leftTpe = typeTermForTypeInfo(leftTypeInfo)
    val rightTpe = typeTermForTypeInfo(rightTypeInfo)
    val resultTpe = typeTermForTypeInfo(resultTypeInfo)


    val resultCode = createResult(resultTypeInfo, outputFields, o => s"coll.collect($o);")

    val generatedName = freshName("GeneratedJoin")


    val code = predicate match {
      case n: NopExpression =>
        // Janino does not support generics, that's why we need
        // manual casting here
        if (nullCheck) {
          j"""
        public class $generatedName
            implements org.apache.flink.api.common.functions.FlatFlatJoinFunction {

          ${reuseCode(resultTypeInfo)}

          public org.apache.flink.api.table.TableConfig config = null;
          public $generatedName(org.apache.flink.api.table.TableConfig config) {
            this.config = config;
          }

          public void join(Object _in0, Object _in1, org.apache.flink.util.Collector coll) {
            $leftTpe in0 = ($leftTpe) _in0;
            $rightTpe in1 = ($rightTpe) _in1;

            $resultCode
          }
        }
      """
        } else {
          j"""
        public class $generatedName
            implements org.apache.flink.api.common.functions.FlatJoinFunction {

          ${reuseCode(resultTypeInfo)}

          public org.apache.flink.api.table.TableConfig config = null;
          public $generatedName(org.apache.flink.api.table.TableConfig config) {
            this.config = config;
          }

          public void join(Object _in0, Object _in1, org.apache.flink.util.Collector coll) {
            $leftTpe in0 = ($leftTpe) _in0;
            $rightTpe in1 = ($rightTpe) _in1;

            $resultCode
          }
        }
      """
        }

      case _ =>
        val pred = generateExpression(predicate)
        // Janino does not support generics, that's why we need
        // manual casting here
        if (nullCheck) {
          j"""
        public class $generatedName
            implements org.apache.flink.api.common.functions.FlatFlatJoinFunction {

          ${reuseCode(resultTypeInfo)}

          org.apache.flink.api.table.TableConfig config = null;

          public $generatedName(org.apache.flink.api.table.TableConfig config) {
            this.config = config;
            ${reuseInitCode()}
          }

          public void join(Object _in0, Object _in1, org.apache.flink.util.Collector coll) {
            $leftTpe in0 = ($leftTpe) _in0;
            $rightTpe in1 = ($rightTpe) _in1;

            ${pred.code}

            if (${pred.nullTerm} && ${pred.resultTerm}) {
              $resultCode
            }
          }
        }
      """
        } else {
          j"""
        public class $generatedName
            implements org.apache.flink.api.common.functions.FlatJoinFunction {

          ${reuseCode(resultTypeInfo)}

          org.apache.flink.api.table.TableConfig config = null;

          public $generatedName(org.apache.flink.api.table.TableConfig config) {
            this.config = config;
            ${reuseInitCode()}
          }

          public void join(Object _in0, Object _in1, org.apache.flink.util.Collector coll) {
            $leftTpe in0 = ($leftTpe) _in0;
            $rightTpe in1 = ($rightTpe) _in1;

            ${pred.code}

            if (${pred.resultTerm}) {
              $resultCode
            }
          }
        }
      """
        }
    }

    LOG.debug(s"""Generated join:\n$code""")
    compiler.cook(new StringReader(code))
    val clazz = compiler.getClassLoader().loadClass(generatedName)
    val constructor = clazz.getConstructor(classOf[TableConfig])
    constructor.newInstance(config).asInstanceOf[FlatJoinFunction[L, R, O]]
  }
}
