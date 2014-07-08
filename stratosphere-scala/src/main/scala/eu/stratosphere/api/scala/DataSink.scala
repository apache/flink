/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.scala

import java.net.URI
import eu.stratosphere.api.scala.analysis._
import eu.stratosphere.api.common.io.OutputFormat
import eu.stratosphere.configuration.Configuration
import eu.stratosphere.api.common.io.FileOutputFormat
import eu.stratosphere.types.Record
import eu.stratosphere.api.common.operators.base.GenericDataSinkBase
import eu.stratosphere.api.java.record.operators.FileDataSink

object DataSinkOperator {
  val DEFAULT_DATASINKOPERATOR_NAME = "<Unnamed Scala Data Sink>"

  def write[In](input: DataSet[In], url: String, format: ScalaOutputFormat[In],
                name: String = DEFAULT_DATASINKOPERATOR_NAME): ScalaSink[In]
  = {
    val uri = getUri(url)

    val ret = uri.getScheme match {
      case "file" | "hdfs" => new FileDataSink(format.asInstanceOf[FileOutputFormat[Record]], uri.toString,
        input.contract, name) with ScalaOutputOperator[In] {

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

class ScalaSink[In] private[scala] (private[scala] val sink: GenericDataSinkBase[Record])

trait ScalaOutputFormat[In] { this: OutputFormat[_] =>
  def getUDF: UDF1[In, Nothing]
  def persistConfiguration(config: Configuration) = {}
  def configure(config: Configuration)
}