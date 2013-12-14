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
import eu.stratosphere.scala.stubs._
import eu.stratosphere.types._
import eu.stratosphere.types.parser._
import eu.stratosphere.api.io.InputFormat
import eu.stratosphere.api.operators.GenericDataSource
import eu.stratosphere.api.operators.FileDataSource
import eu.stratosphere.configuration.Configuration
import eu.stratosphere.api.io.FileInputFormat
import eu.stratosphere.api.io.GenericInputFormat
import eu.stratosphere.scala.operators.TextInputFormat

object DataSource {

  def apply[Out](url: String, format: ScalaInputFormat[Out]): DataSet[Out] with OutputHintable[Out] = {
    val uri = getUri(url)
    
    val ret = uri.getScheme match {

      case "file" | "hdfs" => new FileDataSource(format.asInstanceOf[FileInputFormat[_]], uri.toString)
          with ScalaContract[Out] {

        override def getUDF = format.getUDF

        override def persistConfiguration() = format.persistConfiguration(this.getParameters())
      }

      case "ext" => new GenericDataSource[GenericInputFormat[_]](format.asInstanceOf[GenericInputFormat[_]], uri.toString)
          with ScalaContract[Out] {

        override def getUDF = format.getUDF
        override def persistConfiguration() = format.persistConfiguration(this.getParameters())
      }
    }
    
    new DataSet[Out](ret) with OutputHintable[Out] {}
  }

  private def getUri(url: String) = {
    val uri = new URI(url)
    if (uri.getScheme == null)
      new URI("file://" + url)
    else
      uri
  }
}


trait ScalaInputFormat[Out] { this: InputFormat[_, _] =>
  def getUDF: UDF0[Out]
  def persistConfiguration(config: Configuration) = {}
  def configure(config: Configuration)
}

// convenience text file to look good in word count example :D
object TextFile {
  def apply(url: String): DataSet[String] with OutputHintable[String] = DataSource(url, TextInputFormat())
}
