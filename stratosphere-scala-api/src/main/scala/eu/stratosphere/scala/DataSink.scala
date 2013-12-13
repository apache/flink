/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.scala

import java.net.URI
import eu.stratosphere.scala.analysis._
import eu.stratosphere.pact.common.contract.FileDataSink
import eu.stratosphere.pact.generic.io.OutputFormat
import eu.stratosphere.pact.generic.contract.Contract
import eu.stratosphere.nephele.configuration.Configuration
import eu.stratosphere.pact.generic.io.FileOutputFormat
import eu.stratosphere.pact.common.contract.GenericDataSink

object DataSinkOperator {

  def write[In](input: DataSet[In], url: String, format: ScalaOutputFormat[In]): ScalaSink[In] = {
    val uri = getUri(url)

    val ret = uri.getScheme match {
      case "file" | "hdfs" => new FileDataSink(format.asInstanceOf[FileOutputFormat[_]], uri.toString, input.contract)
          with OneInputScalaContract[In, Nothing] {

        def getUDF = format.getUDF
        override def persistConfiguration() = format.persistConfiguration(this.getParameters())
      }
    }
    new ScalaSink(ret)
  }

  private def getUri(url: String) = {
    val uri = new URI(url)
    if (uri.getScheme == null)
      new URI("file://" + url)
    else
      uri
  }
}

class ScalaSink[In] private[scala] (private[scala] val sink: GenericDataSink)

trait ScalaOutputFormat[In] { this: OutputFormat[_] =>
  def getUDF: UDF1[In, Nothing]
  def persistConfiguration(config: Configuration) = {}
  def configure(config: Configuration)
}