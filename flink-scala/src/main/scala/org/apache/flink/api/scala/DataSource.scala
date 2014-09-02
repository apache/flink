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

import java.net.URI
import collection.JavaConversions._
import org.apache.flink.api.scala.analysis._
import org.apache.flink.api.scala.functions._
import org.apache.flink.api.scala.analysis.UDF0
import org.apache.flink.types._
import org.apache.flink.types.parser._
import org.apache.flink.api.java.record.operators.{CollectionDataSource => JavaCollectionDataSource, FileDataSource}
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.java.record.operators.{CollectionDataSource => JavaCollectionDataSource}
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.scala.operators.TextInputFormat



object DataSource {

  def apply[Out](url: String, format: ScalaInputFormat[Out]): DataSet[Out] with OutputHintable[Out] = {
    val uri = getUri(url)
    
    val ret = uri.getScheme match {

      case "file" | "hdfs" => new FileDataSource(format.asInstanceOf[FileInputFormat[Record]], uri.toString)
          with ScalaOperator[Out, Record] {

        override def getUDF = format.getUDF

        override def persistConfiguration() = format.persistConfiguration(this.getParameters)
      }

//      case "ext" => new GenericDataSource[GenericInputFormat[_]](format.asInstanceOf[GenericInputFormat[_]], uri.toString)
//          with ScalaOperator[Out] {
//
//        override def getUDF = format.getUDF
//        override def persistConfiguration() = format.persistConfiguration(this.getParameters())
//      }
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
    	with ScalaOperator[Out, Record]{
       
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
    	with ScalaOperator[Out, Record]{
       
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
