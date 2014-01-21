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
import eu.stratosphere.api.scala.functions._
import eu.stratosphere.types._
import eu.stratosphere.types.parser._
import eu.stratosphere.api.common.io.InputFormat
import eu.stratosphere.api.common.operators.GenericDataSource
import eu.stratosphere.api.common.operators.FileDataSource
import eu.stratosphere.api.common.operators.{CollectionDataSource => JavaCollectionDataSource}
import eu.stratosphere.configuration.Configuration
import eu.stratosphere.api.common.io.FileInputFormat
import eu.stratosphere.api.common.io.GenericInputFormat
import eu.stratosphere.api.scala.operators.TextInputFormat
import collection.JavaConversions._

object DataSource {

  def apply[Out](url: String, format: ScalaInputFormat[Out]): DataSet[Out] with OutputHintable[Out] = {
    val uri = getUri(url)
    
    val ret = uri.getScheme match {

      case "file" | "hdfs" => new FileDataSource(format.asInstanceOf[FileInputFormat[_]], uri.toString)
          with ScalaOperator[Out] {

        override def getUDF = format.getUDF

        override def persistConfiguration() = format.persistConfiguration(this.getParameters())
      }

      case "ext" => new GenericDataSource[GenericInputFormat[_]](format.asInstanceOf[GenericInputFormat[_]], uri.toString)
          with ScalaOperator[Out] {

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

object CollectionDataSource {
  /*
  constructor for collection input
   */
  def apply[Out: UDT](data: Iterable[Out]):DataSet[Out] with OutputHintable[Out] = {
    /*
    reuse the java implementation of collection data by adding scala operator
    */
    val js:java.util.Collection[Out] = data
    val ret = new JavaCollectionDataSource(js)
    	with ScalaOperator[Out]{
       
       val udf = new UDF0(implicitly[UDT[Out]])
       override def getUDF = udf

    }
    
    new DataSet[Out](ret) with OutputHintable[Out] {}
  }
  
  /*
  constructor for serializable iterator input
   */
  def apply[Out: UDT](data: Iterator[Out] with Serializable) = {

    /*
    reuse the java implementation of collection data by adding scala operator
     */
    val ret = new JavaCollectionDataSource(data)
    	with ScalaOperator[Out]{
       
       val udf = new UDF0(implicitly[UDT[Out]])
       override def getUDF = udf

    }
    
    new DataSet[Out](ret) with OutputHintable[Out] {}
  }
}



trait ScalaInputFormat[Out] { this: InputFormat[_, _] =>
  def getUDF: UDF0[Out]
  def persistConfiguration(config: Configuration) = {}
  def configure(config: Configuration)
}


object TextFile {
  def apply(url: String): DataSet[String] with OutputHintable[String] = DataSource(url, TextInputFormat())
}
