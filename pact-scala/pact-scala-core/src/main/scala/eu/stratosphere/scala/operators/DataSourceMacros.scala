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

import java.io.DataInput
import scala.language.experimental.macros
import scala.reflect.macros.Context
import eu.stratosphere.nephele.configuration.Configuration
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.Value
import eu.stratosphere.pact.common.`type`.base.PactDouble
import eu.stratosphere.pact.common.`type`.base.PactDouble
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.`type`.base.PactLong
import eu.stratosphere.pact.common.`type`.base.PactLong
import eu.stratosphere.pact.common.`type`.base.PactString
import eu.stratosphere.pact.common.`type`.base.PactString
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextDoubleParser
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextDoubleParser
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextIntParser
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextIntParser
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextLongParser
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextLongParser
import eu.stratosphere.pact.common.`type`.base.parser.FieldParser
import eu.stratosphere.pact.common.`type`.base.parser.VarLengthStringParser
import eu.stratosphere.pact.common.`type`.base.parser.VarLengthStringParser
import eu.stratosphere.scala.DataSourceFormat
import eu.stratosphere.scala.analysis.OutputField
import eu.stratosphere.scala.analysis.UDF0
import eu.stratosphere.scala.analysis.UDT
import eu.stratosphere.scala.operators.stubs.ScalaInputFormat
import eu.stratosphere.scala.operators.stubs.FixedLengthInputFormat
import eu.stratosphere.scala.operators.stubs.BinaryInputFormat
import eu.stratosphere.scala.operators.stubs.DelimitedInputFormat
import eu.stratosphere.scala.operators.stubs.FixedLengthInputFormat
import eu.stratosphere.pact.generic.io.{BinaryInputFormat => JavaBinaryInputFormat}
import eu.stratosphere.pact.generic.io.{SequentialInputFormat => JavaSequentialInputFormat}
import eu.stratosphere.pact.common.io.{DelimitedInputFormat => JavaDelimitedInputFormat}
import eu.stratosphere.pact.common.io.{FixedLengthInputFormat => JavaFixedLengthInputFormat}
import eu.stratosphere.pact.common.io.{RecordInputFormat => JavaRecordInputFormat}
import eu.stratosphere.pact.common.io.{TextInputFormat => JavaTextInputFormat}
import eu.stratosphere.scala.codegen.MacroContextHolder

object BinaryInputFormat {
  
  // We need to do the "optional parameters" manually here (and in all other formats) because scala macros
  // do (not yet?) support optional parameters in macros.
  
  def apply[Out](readFunction: DataInput => Out): DataSourceFormat[Out] = macro implWithoutBlocksize[Out]
  def apply[Out](readFunction: DataInput => Out, blockSize: Long): DataSourceFormat[Out] = macro implWithBlocksize[Out]
  
  def implWithoutBlocksize[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[DataInput => Out]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    impl(c)(readFunction, reify { None })
  }
  def implWithBlocksize[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[DataInput => Out], blockSize: c.Expr[Long]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    impl(c)(readFunction, reify { Some(blockSize.splice) })
  }
  
  def impl[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[DataInput => Out], blockSize: c.Expr[Option[Long]]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      
      new BinaryInputFormat[Out] with DataSourceFormat[Out] {
        val udt = c.Expr(createUdtOut).splice
        override val userFunction = readFunction.splice

        override def persistConfiguration(config: Configuration) {
          super.persistConfiguration(config)
          blockSize.splice map { config.setLong(JavaBinaryInputFormat.BLOCK_SIZE_PARAMETER_KEY, _) }
        }
        override def getUDF = this.udf
      }
    }
    
    val result = c.Expr[DataSourceFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

//    c.info(c.enclosingPosition, s"GENERATED Pact4s DataSource Format: ${show(result)}", true)

    return result
    
  }
}

object SequentialInputFormat {
  def apply[Out](): DataSourceFormat[Out] = macro implWithoutBlocksize[Out]
  def apply[Out](blockSize: Long): DataSourceFormat[Out] = macro implWithBlocksize[Out]
  
  def implWithoutBlocksize[Out: c.WeakTypeTag](c: Context)() : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    impl(c)(reify { None })
  }
  def implWithBlocksize[Out: c.WeakTypeTag](c: Context)(blockSize: c.Expr[Long]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    impl(c)(reify { Some(blockSize.splice) })
  }
  
  def impl[Out: c.WeakTypeTag](c: Context)(blockSize: c.Expr[Option[Long]]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      
      new JavaSequentialInputFormat[PactRecord] with DataSourceFormat[Out] {
        override def persistConfiguration(config: Configuration) {
          super.persistConfiguration(config)
          blockSize.splice map { config.setLong(JavaBinaryInputFormat.BLOCK_SIZE_PARAMETER_KEY, _) }
        }
        
        val udt: UDT[Out] = c.Expr[UDT[Out]](createUdtOut).splice
        lazy val udf: UDF0[Out] = new UDF0(udt)
        override def getUDF = udf
      }
      
    }
    
    val result = c.Expr[DataSourceFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

//    c.info(c.enclosingPosition, s"GENERATED Pact4s DataSource Format: ${show(result)}", true)

    return result
    
  }
}

object DelimitedInputFormat {
  def asReadFunction[Out](parseFunction: String => Out) = {
    (source: Array[Byte], offset: Int, numBytes: Int) => {
        parseFunction(new String(source, offset, numBytes))
    }
  }
  
  def apply[Out](readFunction: (Array[Byte], Int, Int) => Out, delim: Option[String]): DataSourceFormat[Out] = macro impl[Out]
  def apply[Out](parseFunction: String => Out): DataSourceFormat[Out] = macro parseFunctionImplWithoutDelim[Out]
  def apply[Out](parseFunction: String => Out, delim: String): DataSourceFormat[Out] = macro parseFunctionImplWithDelim[Out]
  
  def parseFunctionImplWithoutDelim[Out: c.WeakTypeTag](c: Context)(parseFunction: c.Expr[String => Out]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    val readFun = reify {
      asReadFunction[Out](parseFunction.splice)
    }
    impl(c)(readFun, reify { None })
  }
  def parseFunctionImplWithDelim[Out: c.WeakTypeTag](c: Context)(parseFunction: c.Expr[String => Out], delim: c.Expr[String]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    val readFun = reify {
      asReadFunction[Out](parseFunction.splice)
    }
    impl(c)(readFun, reify { Some(delim.splice) })
  }

  
  def impl[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[(Array[Byte], Int, Int) => Out], delim: c.Expr[Option[String]]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      
      new DelimitedInputFormat[Out] with DataSourceFormat[Out]{
        val udt = c.Expr(createUdtOut).splice
        
        setDelimiter((delim.splice.getOrElse("\n")));
        
        override val userFunction = readFunction.splice
        
        override def getUDF = this.udf
      }
      
    }
    
    val result = c.Expr[DataSourceFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

//    c.info(c.enclosingPosition, s"GENERATED Pact4s DataSource Format: ${show(result)}", true)

    return result
    
  }
}

object CsvInputFormat {
  
  def apply[Out](): DataSourceFormat[Out] = macro implWithoutAll[Out]
  def apply[Out](recordDelim: String): DataSourceFormat[Out] = macro implWithRD[Out]
  def apply[Out](recordDelim: String, fieldDelim: Char): DataSourceFormat[Out] = macro implWithRDandFD[Out]
  
  def implWithoutAll[Out: c.WeakTypeTag](c: Context)() : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    impl(c)(reify { None }, reify { None })
  }
  def implWithRD[Out: c.WeakTypeTag](c: Context)(recordDelim: c.Expr[String]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    impl(c)(reify { Some(recordDelim.splice) }, reify { None })
  }
  def implWithRDandFD[Out: c.WeakTypeTag](c: Context)(recordDelim: c.Expr[String], fieldDelim: c.Expr[Char]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    impl(c)(reify { Some(recordDelim.splice) }, reify { Some(fieldDelim.splice) })
  }
  
  def impl[Out: c.WeakTypeTag](c: Context)(recordDelim: c.Expr[Option[String]], fieldDelim: c.Expr[Option[Char]]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      new JavaRecordInputFormat with DataSourceFormat[Out] {
        
        val udt: UDT[Out] = c.Expr[UDT[Out]](createUdtOut).splice
        lazy val udf: UDF0[Out] = new UDF0(udt)
        override def getUDF = udf
        
                setDelimiter((recordDelim.splice.getOrElse("\n")))
        setFieldDelim(fieldDelim.splice.getOrElse(','))
        
        // there is a problem with the reification of Class[_ <: Value], so we work with Class[_] and convert it
        // in a function outside the reify block
        setFieldTypesArray(asValueClassArray(getUDF.outputFields.filter(_.isUsed).map(x => getUDF.outputUDT.fieldTypes(x.localPos))))
        
        // is this maybe more correct? Note that null entries in the types array denote fields skipped by the CSV parser
//      setFieldTypesArray(asValueClassArrayFromOption(getUDF.outputFields.map(x => if (x.isUsed) Some(getUDF.outputUDT.fieldTypes(x.localPos)) else None)))
        
      }
      
    }
    
    val result = c.Expr[DataSourceFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

//    c.info(c.enclosingPosition, s"GENERATED Pact4s DataSource Format: ${show(result)}", true)

    return result
    
  }
  
    // we need to do this conversion outside the reify block
    def asValueClassArray(types: Seq[Class[_]]) : Array[Class[_ <: Value]] = {
      
      val typed = types.foldRight(List[Class[_ <: Value]]())((x,y) => {
        val t : Class[_ <: Value] = x.asInstanceOf[Class[_ <: Value]]
        t :: y
      })
      
      Array[Class[_ <: Value]]() ++ typed
    }
    
    // we need to do this conversion outside the reify block
    def asValueClassArrayFromOption(types: Seq[Option[Class[_]]]) : Array[Class[_ <: Value]] = {
      
      val typed = types.foldRight(List[Class[_ <: Value]]())((x,y) => {
        val t : Class[_ <: Value] = if (x.isEmpty) null else x.asInstanceOf[Class[_ <: Value]]
        t :: y
      })
      
      Array[Class[_ <: Value]]() ++ typed
    }
}

object TextInputFormat {
  def apply(charSetName: Option[String] = None): DataSourceFormat[String] = {

    new JavaTextInputFormat with DataSourceFormat[String] {
      override def persistConfiguration(config: Configuration) {
        super.persistConfiguration(config)

        charSetName map { config.setString(JavaTextInputFormat.CHARSET_NAME, _) }

        config.setInteger(JavaTextInputFormat.FIELD_POS, getUDF.outputFields(0).globalPos.getValue)
      }
     // override val udt: UDT[String] = UDT.StringUDT
      lazy val udf: UDF0[String] = new UDF0(UDT.StringUDT)
      override def getUDF = udf
    }
  }
}

object FixedLengthInputFormat {
  def apply[Out](readFunction: (Array[Byte], Int) => Out, recordLength: Int): DataSourceFormat[Out] = macro impl[Out]
  
  def impl[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[(Array[Byte], Int) => Out], recordLength: c.Expr[Int]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      
      new FixedLengthInputFormat[Out] with DataSourceFormat[Out] {
        val udt = c.Expr(createUdtOut).splice
        override val userFunction = readFunction.splice
      
        override def persistConfiguration(config: Configuration) {
          super.persistConfiguration(config)
          config.setInteger(JavaFixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, (recordLength.splice))
        }
        override def getUDF = this.udf
      }
      
    }
    
    val result = c.Expr[DataSourceFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

//    c.info(c.enclosingPosition, s"GENERATED Pact4s DataSource Format: ${show(result)}", true)

    return result
    
  }
}
