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
import eu.stratosphere.scala.DataSinkFormat
import eu.stratosphere.pact.common.io.RecordOutputFormat
import eu.stratosphere.scala.analysis.UDF1
import eu.stratosphere.scala.operators.stubs.BinaryOutput4sStub
import eu.stratosphere.scala.operators.stubs.DataOutput4sStub
import eu.stratosphere.scala.operators.stubs.RawOutput4sStub
import eu.stratosphere.scala.operators.stubs.DelimitedOutput4sStub
import eu.stratosphere.pact.generic.io.SequentialOutputFormat
import eu.stratosphere.nephele.configuration.Configuration
import eu.stratosphere.scala.analysis.UDT
import eu.stratosphere.pact.generic.io.BinaryOutputFormat
import eu.stratosphere.scala.analysis.InputField
import eu.stratosphere.pact.common.io.DelimitedOutputFormat
import eu.stratosphere.scala.codegen.UDTDescriptors
import eu.stratosphere.scala.codegen.MacroContextHolder


object RawDataSinkFormat {
  def apply[In](writeFunction: (In, OutputStream) => Unit): DataSinkFormat[In] = macro impl[In]
  
  def impl[In: c.WeakTypeTag](c: Context)(writeFunction: c.Expr[(In, OutputStream) => Unit]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    
    val pact4sFormat = reify {
      
      class GeneratedOutputFormat extends RawOutput4sStub[In] {
        override val udt = c.Expr(createUdtIn).splice
        override val userFunction = writeFunction.splice
      }
      
      new DataSinkFormat[In](new GeneratedOutputFormat, c.Expr[UDT[In]](createUdtIn).splice) {
        override def getUDF = this.format.asInstanceOf[DataOutput4sStub[In]].udf
      }
      
    }
    
    val result = c.Expr[DataSinkFormat[In]](Block(List(udtIn), pact4sFormat.tree))

    return result
    
  }
}

object BinaryDataSinkFormat {
  
  def apply[In](writeFunction: (In, DataOutput) => Unit): DataSinkFormat[In] = macro implWithoutBlocksize[In]
  def apply[In](writeFunction: (In, DataOutput) => Unit, blockSize: Long): DataSinkFormat[In] = macro implWithBlocksize[In]
  
  def implWithoutBlocksize[In: c.WeakTypeTag](c: Context)(writeFunction: c.Expr[(In, DataOutput) => Unit]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    impl(c)(writeFunction, reify { None })
  }
  def implWithBlocksize[In: c.WeakTypeTag](c: Context)(writeFunction: c.Expr[(In, DataOutput) => Unit], blockSize: c.Expr[Long]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    impl(c)(writeFunction, reify { Some(blockSize.splice) })
  }
  
  def impl[In: c.WeakTypeTag](c: Context)(writeFunction: c.Expr[(In, DataOutput) => Unit], blockSize: c.Expr[Option[Long]]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    
    val pact4sFormat = reify {
      
      class GeneratedOutputFormat extends BinaryOutput4sStub[In] {
        override val udt = c.Expr(createUdtIn).splice
        override val userFunction = writeFunction.splice
      }
      
      new DataSinkFormat[In](new GeneratedOutputFormat, c.Expr[UDT[In]](createUdtIn).splice) {
        override def persistConfiguration(config: Configuration) {
          blockSize.splice map { config.setLong(BinaryOutputFormat.BLOCK_SIZE_PARAMETER_KEY, _) }
        }
        override def getUDF = this.format.asInstanceOf[DataOutput4sStub[In]].udf
      }
      
    }
    
    val result = c.Expr[DataSinkFormat[In]](Block(List(udtIn), pact4sFormat.tree))

    return result
    
  }
}

// TODO check whether this ever worked ...
object SequentialDataSinkFormat {
  
  def apply[In](): DataSinkFormat[In] = macro implWithoutBlocksize[In]
  def apply[In](blockSize: Long): DataSinkFormat[In] = macro implWithBlocksize[In]
  
  def implWithoutBlocksize[In: c.WeakTypeTag](c: Context)() : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    impl(c)(reify { None })
  }
  def implWithBlocksize[In: c.WeakTypeTag](c: Context)(blockSize: c.Expr[Long]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    impl(c)(reify { Some(blockSize.splice) })
  }
  
  def impl[In: c.WeakTypeTag](c: Context)(blockSize: c.Expr[Option[Long]]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    
    val pact4sFormat = reify {
      
      new DataSinkFormat[In](new SequentialOutputFormat, c.Expr[UDT[In]](createUdtIn).splice) {
        override def persistConfiguration(config: Configuration) {
          blockSize.splice map { config.setLong(BinaryOutputFormat.BLOCK_SIZE_PARAMETER_KEY, _) }
        }
        override val udt = c.Expr[UDT[In]](createUdtIn).splice
        lazy val udf: UDF1[In, Nothing] = new UDF1[In, Nothing](udt, UDT.NothingUDT)
        override def getUDF = udf
      }
      
    }
    
    val result = c.Expr[DataSinkFormat[In]](Block(List(udtIn), pact4sFormat.tree))

    return result
    
  }
}

object DelimitedDataSinkFormat {
  
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
  
  def apply[In](formatFunction: In => String): DataSinkFormat[In] = macro writeFunctionForStringWithoutDelim[In]
  def apply[In](formatFunction: In => String, delimiter: String): DataSinkFormat[In] = macro writeFunctionForStringWithDelim[In]
  def apply[In](formatFunction: (In, StringBuilder) => Unit): DataSinkFormat[In] = macro writeFunctionForStringBuilderWithoutDelim[In]
  def apply[In](formatFunction: (In, StringBuilder) => Unit, delimiter: String): DataSinkFormat[In] = macro writeFunctionForStringBuilderWithDelim[In]
  
  def writeFunctionForStringWithoutDelim[In: c.WeakTypeTag](c: Context)(formatFunction: c.Expr[In => String]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    val writeFun = reify {
      forString[In](formatFunction.splice)
    }
    impl(c)(writeFun, reify { None })
  }
  
  def writeFunctionForStringWithDelim[In: c.WeakTypeTag](c: Context)(formatFunction: c.Expr[In => String], delimiter: c.Expr[String]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    val writeFun = reify {
      forString[In](formatFunction.splice)
    }
    impl(c)(writeFun, reify { Some(delimiter.splice) })
  }
  
  def writeFunctionForStringBuilderWithoutDelim[In: c.WeakTypeTag](c: Context)(formatFunction: c.Expr[(In, StringBuilder) => Unit]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    val writeFun = reify {
      forStringBuilder[In](formatFunction.splice)
    }
    impl(c)(writeFun, reify { None })
  }
  
  def writeFunctionForStringBuilderWithDelim[In: c.WeakTypeTag](c: Context)(formatFunction: c.Expr[(In, StringBuilder) => Unit], delimiter: c.Expr[String]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    val writeFun = reify {
      forStringBuilder[In](formatFunction.splice)
    }
    impl(c)(writeFun, reify { Some(delimiter.splice) })
  }
  
  def impl[In: c.WeakTypeTag](c: Context)(writeFunction: c.Expr[(In, Array[Byte]) => Int], delimiter: c.Expr[Option[String]]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    
    val pact4sFormat = reify {
      
      class GeneratedOutputFormat extends DelimitedOutput4sStub[In] {
        override val udt = c.Expr(createUdtIn).splice
        override val userFunction = writeFunction.splice
      }
      
      new DataSinkFormat[In](new GeneratedOutputFormat, c.Expr[UDT[In]](createUdtIn).splice) {
        override def persistConfiguration(config: Configuration) {
          delimiter.splice map { config.setString(DelimitedOutputFormat.RECORD_DELIMITER, _) }
        }
        override def getUDF = this.format.asInstanceOf[DataOutput4sStub[In]].udf
      }
      
    }
    
    val result = c.Expr[DataSinkFormat[In]](Block(List(udtIn), pact4sFormat.tree))

    return result
    
  }
}

object RecordDataSinkFormat {
  def apply[In](recordDelimiter: Option[String], fieldDelimiter: Option[String] = None, lenient: Option[Boolean]): DataSinkFormat[In] = macro impl[In]
  
  def apply[In](): DataSinkFormat[In] = macro implWithoutAll[In]
  def apply[In](recordDelimiter: String): DataSinkFormat[In] = macro implWithRD[In]
  def apply[In](recordDelimiter: String, fieldDelimiter: String): DataSinkFormat[In] = macro implWithRDandFD[In]
  def apply[In](recordDelimiter: String, fieldDelimiter: String, lenient: Boolean): DataSinkFormat[In] = macro implWithRDandFDandLenient[In]
  
  def implWithoutAll[In: c.WeakTypeTag](c: Context)() : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    impl(c)(reify { None }, reify { None }, reify { None })
  }
  def implWithRD[In: c.WeakTypeTag](c: Context)(recordDelimiter: c.Expr[String]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    impl(c)(reify { Some(recordDelimiter.splice) }, reify { None }, reify { None })
  }
  def implWithRDandFD[In: c.WeakTypeTag](c: Context)(recordDelimiter: c.Expr[String], fieldDelimiter: c.Expr[String]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    impl(c)(reify { Some(recordDelimiter.splice) }, reify { Some(fieldDelimiter.splice) }, reify { None })
  }
  def implWithRDandFDandLenient[In: c.WeakTypeTag](c: Context)(recordDelimiter: c.Expr[String], fieldDelimiter: c.Expr[String], lenient: c.Expr[Boolean]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    impl(c)(reify { Some(recordDelimiter.splice) }, reify { Some(fieldDelimiter.splice) }, reify { Some(lenient.splice) })
  }
  
  def impl[In: c.WeakTypeTag](c: Context)(recordDelimiter: c.Expr[Option[String]], fieldDelimiter: c.Expr[Option[String]], lenient: c.Expr[Option[Boolean]]) : c.Expr[DataSinkFormat[In]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    
    val pact4sFormat = reify {
      
      new DataSinkFormat[In](new RecordOutputFormat, c.Expr[UDT[In]](createUdtIn).splice) {
        override def persistConfiguration(config: Configuration) {

          val fields = getUDF.inputFields.filter(_.isUsed)

          config.setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, fields.length)

          var index = 0
          fields foreach { field: InputField =>
            val tpe = getUDF.inputUDT.fieldTypes(field.localPos)
            config.setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + index, tpe)
            config.setInteger(RecordOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + index, field.globalPos.getValue)
            index = index + 1
          }

          recordDelimiter.splice map { config.setString(RecordOutputFormat.RECORD_DELIMITER_PARAMETER, _) }
          fieldDelimiter.splice map { config.setString(RecordOutputFormat.FIELD_DELIMITER_PARAMETER, _) }
          lenient.splice map { config.setBoolean(RecordOutputFormat.LENIENT_PARSING, _) }
        }
        
        override val udt = c.Expr[UDT[In]](createUdtIn).splice
        lazy val udf: UDF1[In, Nothing] = new UDF1[In, Nothing](udt, UDT.NothingUDT)
        override def getUDF = udf
      }
      
    }
    
    val result = c.Expr[DataSinkFormat[In]](Block(List(udtIn), pact4sFormat.tree))

    return result
    
  }
}