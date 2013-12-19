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

package eu.stratosphere.api.scala.operators

import java.io.DataInput
import scala.language.experimental.macros
import scala.reflect.macros.Context
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.Record
import eu.stratosphere.types.Value
import eu.stratosphere.types.DoubleValue
import eu.stratosphere.types.IntValue
import eu.stratosphere.types.LongValue
import eu.stratosphere.types.StringValue
import eu.stratosphere.types.parser.DecimalTextDoubleParser
import eu.stratosphere.types.parser.DecimalTextIntParser
import eu.stratosphere.types.parser.DecimalTextLongParser
import eu.stratosphere.types.parser.FieldParser
import eu.stratosphere.types.parser.VarLengthStringParser
import eu.stratosphere.api.scala.ScalaInputFormat
import eu.stratosphere.api.scala.analysis.{UDTSerializer, OutputField, UDF0, UDT}
import eu.stratosphere.api.io.{InputFormat => JavaInputFormat}
import eu.stratosphere.api.io.{BinaryInputFormat => JavaBinaryInputFormat}
import eu.stratosphere.api.io.{SequentialInputFormat => JavaSequentialInputFormat}
import eu.stratosphere.api.record.io.{DelimitedInputFormat => JavaDelimitedInputFormat}
import eu.stratosphere.api.record.io.{FixedLengthInputFormat => JavaFixedLengthInputFormat}
import eu.stratosphere.api.record.io.{CsvInputFormat => JavaCsvInputFormat}
import eu.stratosphere.api.record.io.{TextInputFormat => JavaTextInputFormat}
import eu.stratosphere.api.scala.codegen.MacroContextHolder


trait ScalaInputFormatBase[Out] extends ScalaInputFormat[Out] { this: JavaInputFormat[_, _] =>
  protected val udt: UDT[Out]
  lazy val udf: UDF0[Out] = new UDF0(udt)
  def getUDF: UDF0[Out] = udf
  protected var serializer: UDTSerializer[Out] = _
  protected var outputLength: Int = _

  abstract override def configure(config: Configuration) {
    super.configure(config)
    this.outputLength = udf.getOutputLength
    this.serializer = udf.getOutputSerializer
  }
}

object BinaryInputFormat {
  
  // We need to do the "optional parameters" manually here (and in all other formats) because scala macros
  // do (not yet?) support optional parameters in macros.
  
  def apply[Out](readFunction: DataInput => Out): ScalaInputFormat[Out] = macro implWithoutBlocksize[Out]
  def apply[Out](readFunction: DataInput => Out, blockSize: Long): ScalaInputFormat[Out] = macro implWithBlocksize[Out]
  
  def implWithoutBlocksize[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[DataInput => Out]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    impl(c)(readFunction, reify { None })
  }
  def implWithBlocksize[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[DataInput => Out], blockSize: c.Expr[Long]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    impl(c)(readFunction, reify { Some(blockSize.splice) })
  }
  
  def impl[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[DataInput => Out], blockSize: c.Expr[Option[Long]]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      
      new JavaBinaryInputFormat[Record] with ScalaInputFormatBase[Out] {
        override val udt = c.Expr(createUdtOut).splice

        override def persistConfiguration(config: Configuration) {
          super.persistConfiguration(config)
          blockSize.splice map { config.setLong(JavaBinaryInputFormat.BLOCK_SIZE_PARAMETER_KEY, _) }
        }

        override def deserialize(record: Record, source: DataInput) = {
          val output = readFunction.splice.apply(source)
          record.setNumFields(outputLength)
          serializer.serialize(output, record)
        }
      }
    }
    
    val result = c.Expr[ScalaInputFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

//    c.info(c.enclosingPosition, s"GENERATED Pact4s DataSource Format: ${show(result)}", true)

    return result
    
  }
}

object SequentialInputFormat {
  def apply[Out](): ScalaInputFormat[Out] = macro implWithoutBlocksize[Out]
  def apply[Out](blockSize: Long): ScalaInputFormat[Out] = macro implWithBlocksize[Out]
  
  def implWithoutBlocksize[Out: c.WeakTypeTag](c: Context)() : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    impl(c)(reify { None })
  }
  def implWithBlocksize[Out: c.WeakTypeTag](c: Context)(blockSize: c.Expr[Long]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    impl(c)(reify { Some(blockSize.splice) })
  }
  
  def impl[Out: c.WeakTypeTag](c: Context)(blockSize: c.Expr[Option[Long]]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      
      new JavaSequentialInputFormat[Record] with ScalaInputFormat[Out] {
        override def persistConfiguration(config: Configuration) {
          super.persistConfiguration(config)
          blockSize.splice map { config.setLong(JavaBinaryInputFormat.BLOCK_SIZE_PARAMETER_KEY, _) }
        }
        
        val udt: UDT[Out] = c.Expr[UDT[Out]](createUdtOut).splice
        val udf: UDF0[Out] = new UDF0(udt)
        override def getUDF = udf
      }
      
    }
    
    val result = c.Expr[ScalaInputFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

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
  
  def apply[Out](readFunction: (Array[Byte], Int, Int) => Out, delim: Option[String]): ScalaInputFormat[Out] = macro impl[Out]
  def apply[Out](parseFunction: String => Out): ScalaInputFormat[Out] = macro parseFunctionImplWithoutDelim[Out]
  def apply[Out](parseFunction: String => Out, delim: String): ScalaInputFormat[Out] = macro parseFunctionImplWithDelim[Out]
  
  def parseFunctionImplWithoutDelim[Out: c.WeakTypeTag](c: Context)(parseFunction: c.Expr[String => Out]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    val readFun = reify {
      asReadFunction[Out](parseFunction.splice)
    }
    impl(c)(readFun, reify { None })
  }
  def parseFunctionImplWithDelim[Out: c.WeakTypeTag](c: Context)(parseFunction: c.Expr[String => Out], delim: c.Expr[String]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    val readFun = reify {
      asReadFunction[Out](parseFunction.splice)
    }
    impl(c)(readFun, reify { Some(delim.splice) })
  }

  
  def impl[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[(Array[Byte], Int, Int) => Out], delim: c.Expr[Option[String]]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      
      new JavaDelimitedInputFormat with ScalaInputFormatBase[Out]{
        override val udt = c.Expr(createUdtOut).splice
        
        setDelimiter((delim.splice.getOrElse("\n")));

        override def readRecord(record: Record, source: Array[Byte], offset: Int, numBytes: Int): Boolean = {
          val output = readFunction.splice.apply(source, offset, numBytes)

          if (output != null) {
            record.setNumFields(outputLength)
            serializer.serialize(output, record)
          }

          return output != null
        }
      }
      
    }
    
    val result = c.Expr[ScalaInputFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

//    c.info(c.enclosingPosition, s"GENERATED Pact4s DataSource Format: ${show(result)}", true)

    return result
    
  }
}

object CsvInputFormat {
  
  def apply[Out](): ScalaInputFormat[Out] = macro implWithoutAll[Out]
  def apply[Out](recordDelim: String): ScalaInputFormat[Out] = macro implWithRD[Out]
  def apply[Out](recordDelim: String, fieldDelim: Char): ScalaInputFormat[Out] = macro implWithRDandFD[Out]

  def apply[Out](fieldIndices: Seq[Int]): ScalaInputFormat[Out] = macro implWithoutAllWithIndices[Out]
  def apply[Out](fieldIndices: Seq[Int], recordDelim: String): ScalaInputFormat[Out] = macro implWithRDWithIndices[Out]
  def apply[Out](fieldIndices: Seq[Int], recordDelim: String, fieldDelim: Char): ScalaInputFormat[Out] = macro implWithRDandFDWithIndices[Out]


  def implWithoutAll[Out: c.WeakTypeTag](c: Context)() : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    impl(c)(reify { Seq[Int]() }, reify { None }, reify { None })
  }
  def implWithRD[Out: c.WeakTypeTag](c: Context)
                                    (recordDelim: c.Expr[String]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    impl(c)(reify { Seq[Int]() },reify { Some(recordDelim.splice) }, reify { None })
  }
  def implWithRDandFD[Out: c.WeakTypeTag](c: Context)
                                         (recordDelim: c.Expr[String], fieldDelim: c.Expr[Char]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    impl(c)(reify { Seq[Int]() },reify { Some(recordDelim.splice) }, reify { Some(fieldDelim.splice) })
  }

  def implWithoutAllWithIndices[Out: c.WeakTypeTag](c: Context)(fieldIndices: c.Expr[Seq[Int]]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    impl(c)(fieldIndices, reify { None }, reify { None })
  }
  def implWithRDWithIndices[Out: c.WeakTypeTag](c: Context)
                                    (fieldIndices: c.Expr[Seq[Int]], recordDelim: c.Expr[String]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    impl(c)(fieldIndices,reify { Some(recordDelim.splice) }, reify { None })
  }
  def implWithRDandFDWithIndices[Out: c.WeakTypeTag](c: Context)
                                         (fieldIndices: c.Expr[Seq[Int]], recordDelim: c.Expr[String], fieldDelim: c.Expr[Char]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    impl(c)(fieldIndices,reify { Some(recordDelim.splice) }, reify { Some(fieldDelim.splice) })
  }
  
  def impl[Out: c.WeakTypeTag](c: Context)
                              (fieldIndices: c.Expr[Seq[Int]], recordDelim: c.Expr[Option[String]], fieldDelim: c.Expr[Option[Char]]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      new JavaCsvInputFormat with ScalaInputFormat[Out] {
        
        val udt: UDT[Out] = c.Expr[UDT[Out]](createUdtOut).splice
        val udf: UDF0[Out] = new UDF0(udt)
        override def getUDF = udf
        
        setDelimiter((recordDelim.splice.getOrElse("\n")))
        setFieldDelim(fieldDelim.splice.getOrElse(','))
        
        // there is a problem with the reification of Class[_ <: Value], so we work with Class[_] and convert it
        // in a function outside the reify block
//        setFieldTypesArray(asValueClassArray(getUDF.outputFields.filter(_.isUsed).map(x => getUDF.outputUDT.fieldTypes(x.localPos))))
        
        // is this maybe more correct? Note that null entries in the types array denote fields skipped by the CSV parser
        val indices = fieldIndices.splice
        val fieldTypes = asValueClassArrayFromOption(getUDF.outputFields.map {
          case x if x.isUsed => Some(getUDF.outputUDT.fieldTypes(x.localPos))
          case _ => None
        })
        if (indices.isEmpty) {
          setFieldTypesArray(fieldTypes)
        } else {
          setFields(indices.toArray, fieldTypes)
        }
        
      }
      
    }
    
    val result = c.Expr[ScalaInputFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

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
        val t : Class[_ <: Value] = x match {
          case None => null
          case Some(x) => x.asInstanceOf[Class[_ <: Value]]
        }
        t :: y
      })
      
      Array[Class[_ <: Value]]() ++ typed
    }
}

object TextInputFormat {
  def apply(charSetName: Option[String] = None): ScalaInputFormat[String] = {

    new JavaTextInputFormat with ScalaInputFormat[String] {
      override def persistConfiguration(config: Configuration) {
        super.persistConfiguration(config)

        charSetName map { config.setString(JavaTextInputFormat.CHARSET_NAME, _) }

        config.setInteger(JavaTextInputFormat.FIELD_POS, getUDF.outputFields(0).globalPos.getValue)
      }
     // override val udt: UDT[String] = UDT.StringUDT
      val udf: UDF0[String] = new UDF0(UDT.StringUDT)
      override def getUDF = udf
    }
  }
}

object FixedLengthInputFormat {
  def apply[Out](readFunction: (Array[Byte], Int) => Out, recordLength: Int): ScalaInputFormat[Out] = macro impl[Out]
  
  def impl[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[(Array[Byte], Int) => Out], recordLength: c.Expr[Int]) : c.Expr[ScalaInputFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      
      new JavaFixedLengthInputFormat with ScalaInputFormatBase[Out] {
        override val udt = c.Expr(createUdtOut).splice

        override def persistConfiguration(config: Configuration) {
          super.persistConfiguration(config)
          config.setInteger(JavaFixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, (recordLength.splice))
        }

        override def readBytes(record: Record, source: Array[Byte], startPos: Int): Boolean = {
          val output = readFunction.splice.apply(source, startPos)

          if (output != null) {
            record.setNumFields(outputLength)
            serializer.serialize(output, record)
          }

          return output != null
        }
      }
      
    }
    
    val result = c.Expr[ScalaInputFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

//    c.info(c.enclosingPosition, s"GENERATED Pact4s DataSource Format: ${show(result)}", true)

    return result
    
  }
}
