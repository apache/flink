/**
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


package org.apache.flink.api.scala

import scala.collection.JavaConversions.asJavaCollection

import java.util.Calendar

import org.apache.flink.api.common.Plan
import org.apache.flink.compiler.plan.OptimizedPlan
import org.apache.flink.compiler.postpass.RecordModelPostPass
import org.apache.flink.api.common.operators.Operator
import org.apache.flink.types.Record

import org.apache.flink.api.scala.analysis.GlobalSchemaGenerator
import org.apache.flink.api.scala.analysis.postPass.GlobalSchemaOptimizer


class ScalaPlan(scalaSinks: Seq[ScalaSink[_]], scalaJobName: String = "Flink Scala Job at " + Calendar.getInstance()
  .getTime()) extends Plan(asJavaCollection(ScalaPlan.setAnnotations(scalaSinks) map { _.sink }), scalaJobName) {
  val pactSinks = scalaSinks map { _.sink.asInstanceOf[Operator[Record] with ScalaOperator[_, _]] }
  new GlobalSchemaGenerator().initGlobalSchema(pactSinks)
  override def getPostPassClassName() = "org.apache.flink.api.scala.ScalaPostPass";

}

object ScalaPlan{
  def setAnnotations(sinks: Seq[ScalaSink[_]]): Seq[ScalaSink[_]] = {
    AnnotationUtil.setAnnotations(sinks)
  }
}

case class Args(argsMap: Map[String, String], defaultParallelism: Int, schemaHints: Boolean, schemaCompaction: Boolean) {
  def apply(key: String): String = argsMap.getOrElse(key, key)
  def apply(key: String, default: => String) = argsMap.getOrElse(key, default)
}

object Args {

  def parse(args: Seq[String]): Args = {

    var argsMap = Map[String, String]()
    var defaultParallelism = 1
    var schemaHints = true
    var schemaCompaction = true

    val ParamName = "-(.+)".r

    def parse(args: Seq[String]): Unit = args match {
      case Seq("-subtasks", value, rest @ _*)     => { defaultParallelism = value.toInt; parse(rest) }
      case Seq("-nohints", rest @ _*)             => { schemaHints = false; parse(rest) }
      case Seq("-nocompact", rest @ _*)           => { schemaCompaction = false; parse(rest) }
      case Seq(ParamName(name), value, rest @ _*) => { argsMap = argsMap.updated(name, value); parse(rest) }
      case Seq()                                  =>
    }

    parse(args)
    Args(argsMap, defaultParallelism, schemaHints, schemaCompaction)
  }
}

//abstract class ScalaProgram extends Program {
//  def getScalaPlan(args: Args): ScalaPlan
//  
//  override def getPlan(args: String*): Plan = {
//    val scalaArgs = Args.parse(args.toSeq)
//    
//    getScalaPlan(scalaArgs)
//  }
//}


class ScalaPostPass extends RecordModelPostPass with GlobalSchemaOptimizer {
  override def postPass(plan: OptimizedPlan): Unit = {
    optimizeSchema(plan, false)
    super.postPass(plan)
  }
}
