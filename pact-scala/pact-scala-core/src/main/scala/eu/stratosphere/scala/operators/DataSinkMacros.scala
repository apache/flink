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

package eu.stratosphere.scala.operators

import language.experimental.macros
import java.io.DataOutput
import java.io.OutputStream
import scala.reflect.macros.Context
import eu.stratosphere.scala.ScalaOutputFormat

import eu.stratosphere.scala.analysis.{UDTSerializer, UDF1, UDT, InputField}
import eu.stratosphere.nephele.configuration.Configuration
import eu.stratosphere.scala.codegen.UDTDescriptors
import eu.stratosphere.scala.codegen.MacroContextHolder

import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.generic.io.{BinaryOutputFormat => JavaBinaryOutputFormat}
import eu.stratosphere.pact.generic.io.{SequentialOutputFormat => JavaSequentialOutputFormat}
import eu.stratosphere.pact.common.io.{DelimitedOutputFormat => JavaDelimitedOutputFormat}
import eu.stratosphere.pact.common.io.{RecordOutputFormat => JavaRecordOutputFormat}
import eu.stratosphere.pact.common.io.{FileOutputFormat => JavaFileOutputFormat}
import eu.stratosphere.pact.generic.io.{OutputFormat => JavaOutputFormat}


trait ScalaOutputFormatBase[In] extends ScalaOutputFormat[In] { this: JavaOutputFormat[_] =>
  protected val udt: UDT[In]
  lazy val udf: UDF1[In, Nothing] = new UDF1[In, Nothing](udt, UDT.NothingUDT)
  def getUDF: UDF1[In, Nothing] = udf
  protected var deserializer: UDTSerializer[In] = _

  abstract override def configure(config: Configuration) {
    super.configure(config)
    this.deserializer = udf.getInputDeserializer
  }
}


object RawOutputFormat {
  def apply[In](writeFunction: (In, OutputStream) => Unit): ScalaOutputFormat[In] = macro impl[In]
  
  def impl[In: c.WeakTypeTag](c: Context)(writeFunction: c.Expr[(In, OutputStream) => Unit]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    
    val pact4sFormat = reify {
      
      new JavaFileOutputFormat with ScalaOutputFormatBase[In] {
        override val udt = c.Expr(createUdtIn).splice

        override def writeRecord(record: PactRecord) = {
          val input = deserializer.deserializeRecyclingOn(record)
          writeFunction.splice.apply(input, this.stream)
        }
      }
      
    }
    
    val result = c.Expr[ScalaOutputFormat[In]](Block(List(udtIn), pact4sFormat.tree))

    return result
    
  }
}

object BinaryOutputFormat {
  
  def apply[In](writeFunction: (In, DataOutput) => Unit): ScalaOutputFormat[In] = macro implWithoutBlocksize[In]
  def apply[In](writeFunction: (In, DataOutput) => Unit, blockSize: Long): ScalaOutputFormat[In] = macro implWithBlocksize[In]
  
  def implWithoutBlocksize[In: c.WeakTypeTag](c: Context)(writeFunction: c.Expr[(In, DataOutput) => Unit]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    impl(c)(writeFunction, reify { None })
  }
  def implWithBlocksize[In: c.WeakTypeTag](c: Context)(writeFunction: c.Expr[(In, DataOutput) => Unit], blockSize: c.Expr[Long]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    impl(c)(writeFunction, reify { Some(blockSize.splice) })
  }
  
  def impl[In: c.WeakTypeTag](c: Context)(writeFunction: c.Expr[(In, DataOutput) => Unit], blockSize: c.Expr[Option[Long]]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    
    val pact4sFormat = reify {
      
      new JavaBinaryOutputFormat with ScalaOutputFormatBase[In] {
        override val udt = c.Expr(createUdtIn).splice
      
        override def persistConfiguration(config: Configuration) {
          blockSize.splice map { config.setLong(JavaBinaryOutputFormat.BLOCK_SIZE_PARAMETER_KEY, _) }
        }

        override def serialize(record: PactRecord, target: DataOutput) = {
          val input = deserializer.deserializeRecyclingOn(record)
          writeFunction.splice.apply(input, target)
        }
      }
      
    }
    
    val result = c.Expr[ScalaOutputFormat[In]](Block(List(udtIn), pact4sFormat.tree))

    return result
    
  }
}

object SequentialOutputFormat {
  
  def apply[In](): ScalaOutputFormat[In] = macro implWithoutBlocksize[In]
  def apply[In](blockSize: Long): ScalaOutputFormat[In] = macro implWithBlocksize[In]
  
  def implWithoutBlocksize[In: c.WeakTypeTag](c: Context)() : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    impl(c)(reify { None })
  }
  def implWithBlocksize[In: c.WeakTypeTag](c: Context)(blockSize: c.Expr[Long]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    impl(c)(reify { Some(blockSize.splice) })
  }
  
  def impl[In: c.WeakTypeTag](c: Context)(blockSize: c.Expr[Option[Long]]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    
    val pact4sFormat = reify {
      
      new JavaSequentialOutputFormat with ScalaOutputFormat[In] {
        override def persistConfiguration(config: Configuration) {
          blockSize.splice map { config.setLong(JavaBinaryOutputFormat.BLOCK_SIZE_PARAMETER_KEY, _) }
        }
        val udt = c.Expr[UDT[In]](createUdtIn).splice
        lazy val udf: UDF1[In, Nothing] = new UDF1[In, Nothing](udt, UDT.NothingUDT)
        override def getUDF = udf
      }
      
    }
    
    val result = c.Expr[ScalaOutputFormat[In]](Block(List(udtIn), pact4sFormat.tree))

    return result
    
  }
}

object DelimitedOutputFormat {
  
  def forString[In](formatFunction: In => String) = {

    (source: In, target: Array[Byte]) => {
      val str = formatFunction(source)
      val data = str.getBytes
      if (data.length <= target.length) {
        System.arraycopy(data, 0, target, 0, data.length);
        data.length;
      } else {
        -data.length;
      }
    }
    
  }

  def forStringBuilder[In](formatFunction: (In, StringBuilder) => Unit)  = {

    val stringBuilder = new StringBuilder

    (source: In, target: Array[Byte]) => {
      stringBuilder.clear
      formatFunction(source, stringBuilder)

      val data = stringBuilder.toString.getBytes
      if (data.length <= target.length) {
        System.arraycopy(data, 0, target, 0, data.length);
        data.length;
      } else {
        -data.length;
      }
    }
    
  }

  def maybeDelim(delim: String) = if (delim == null) None else Some(delim)
  
  def apply[In](formatFunction: In => String): ScalaOutputFormat[In] = macro writeFunctionForStringWithoutDelim[In]
  def apply[In](formatFunction: In => String, delimiter: String): ScalaOutputFormat[In] = macro writeFunctionForStringWithDelim[In]
  def apply[In](formatFunction: (In, StringBuilder) => Unit): ScalaOutputFormat[In] = macro writeFunctionForStringBuilderWithoutDelim[In]
  def apply[In](formatFunction: (In, StringBuilder) => Unit, delimiter: String): ScalaOutputFormat[In] = macro writeFunctionForStringBuilderWithDelim[In]
  
  def writeFunctionForStringWithoutDelim[In: c.WeakTypeTag](c: Context)(formatFunction: c.Expr[In => String]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    val writeFun = reify {
      forString[In](formatFunction.splice)
    }
    impl(c)(writeFun, reify { None })
  }
  
  def writeFunctionForStringWithDelim[In: c.WeakTypeTag](c: Context)(formatFunction: c.Expr[In => String], delimiter: c.Expr[String]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    val writeFun = reify {
      forString[In](formatFunction.splice)
    }
    impl(c)(writeFun, reify { Some(delimiter.splice) })
  }
  
  def writeFunctionForStringBuilderWithoutDelim[In: c.WeakTypeTag](c: Context)(formatFunction: c.Expr[(In, StringBuilder) => Unit]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    val writeFun = reify {
      forStringBuilder[In](formatFunction.splice)
    }
    impl(c)(writeFun, reify { None })
  }
  
  def writeFunctionForStringBuilderWithDelim[In: c.WeakTypeTag](c: Context)(formatFunction: c.Expr[(In, StringBuilder) => Unit], delimiter: c.Expr[String]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    val writeFun = reify {
      forStringBuilder[In](formatFunction.splice)
    }
    impl(c)(writeFun, reify { Some(delimiter.splice) })
  }
  
  def impl[In: c.WeakTypeTag](c: Context)(writeFunction: c.Expr[(In, Array[Byte]) => Int], delimiter: c.Expr[Option[String]]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    
    val pact4sFormat = reify {
      
      new JavaDelimitedOutputFormat with ScalaOutputFormatBase[In] {
        override val udt = c.Expr(createUdtIn).splice

        override def persistConfiguration(config: Configuration) {
          delimiter.splice map { config.setString(JavaDelimitedOutputFormat.RECORD_DELIMITER, _) }
        }

        override def serializeRecord(record: PactRecord, target: Array[Byte]): Int = {
          val input = deserializer.deserializeRecyclingOn(record)
          writeFunction.splice.apply(input, target)
        }
      }
      
    }
    
    val result = c.Expr[ScalaOutputFormat[In]](Block(List(udtIn), pact4sFormat.tree))

    return result
    
  }
}

object CsvOutputFormat {
  def apply[In](recordDelimiter: Option[String], fieldDelimiter: Option[String] = None, lenient: Option[Boolean]): ScalaOutputFormat[In] = macro impl[In]
  
  def apply[In](): ScalaOutputFormat[In] = macro implWithoutAll[In]
  def apply[In](recordDelimiter: String): ScalaOutputFormat[In] = macro implWithRD[In]
  def apply[In](recordDelimiter: String, fieldDelimiter: String): ScalaOutputFormat[In] = macro implWithRDandFD[In]
  def apply[In](recordDelimiter: String, fieldDelimiter: String, lenient: Boolean): ScalaOutputFormat[In] = macro implWithRDandFDandLenient[In]
  
  def implWithoutAll[In: c.WeakTypeTag](c: Context)() : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    impl(c)(reify { None }, reify { None }, reify { None })
  }
  def implWithRD[In: c.WeakTypeTag](c: Context)(recordDelimiter: c.Expr[String]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    impl(c)(reify { Some(recordDelimiter.splice) }, reify { None }, reify { None })
  }
  def implWithRDandFD[In: c.WeakTypeTag](c: Context)(recordDelimiter: c.Expr[String], fieldDelimiter: c.Expr[String]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    impl(c)(reify { Some(recordDelimiter.splice) }, reify { Some(fieldDelimiter.splice) }, reify { None })
  }
  def implWithRDandFDandLenient[In: c.WeakTypeTag](c: Context)(recordDelimiter: c.Expr[String], fieldDelimiter: c.Expr[String], lenient: c.Expr[Boolean]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    impl(c)(reify { Some(recordDelimiter.splice) }, reify { Some(fieldDelimiter.splice) }, reify { Some(lenient.splice) })
  }
  
  def impl[In: c.WeakTypeTag](c: Context)(recordDelimiter: c.Expr[Option[String]], fieldDelimiter: c.Expr[Option[String]], lenient: c.Expr[Option[Boolean]]) : c.Expr[ScalaOutputFormat[In]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    
    val pact4sFormat = reify {
      
      new JavaRecordOutputFormat with ScalaOutputFormat[In] {
        override def persistConfiguration(config: Configuration) {

          val fields = getUDF.inputFields.filter(_.isUsed)

          config.setInteger(JavaRecordOutputFormat.NUM_FIELDS_PARAMETER, fields.length)

          var index = 0
          fields foreach { field: InputField =>
            val tpe = getUDF.inputUDT.fieldTypes(field.localPos)
            config.setClass(JavaRecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + index, tpe)
            config.setInteger(JavaRecordOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + index, field.globalPos.getValue)
            index = index + 1
          }

          recordDelimiter.splice map { config.setString(JavaRecordOutputFormat.RECORD_DELIMITER_PARAMETER, _) }
          fieldDelimiter.splice map { config.setString(JavaRecordOutputFormat.FIELD_DELIMITER_PARAMETER, _) }
          lenient.splice map { config.setBoolean(JavaRecordOutputFormat.LENIENT_PARSING, _) }
        }
        
        val udt = c.Expr[UDT[In]](createUdtIn).splice
        lazy val udf: UDF1[In, Nothing] = new UDF1[In, Nothing](udt, UDT.NothingUDT)
        override def getUDF = udf
      }
      
    }
    
    val result = c.Expr[ScalaOutputFormat[In]](Block(List(udtIn), pact4sFormat.tree))

    return result
    
  }
}