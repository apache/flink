/*
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

package org.apache.flink.table.typeutils

import java.io.Serializable

import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils._
import org.apache.flink.table.api.{TableException, Types, ValidationException}
import org.apache.flink.table.descriptors.DescriptorProperties.normalizeTypeInfo
import org.apache.flink.util.InstantiationUtil

import _root_.scala.language.implicitConversions
import _root_.scala.util.parsing.combinator.{JavaTokenParsers, PackratParsers}

/**
  * Utilities to convert [[org.apache.flink.api.common.typeinfo.TypeInformation]] into a
  * string representation and back.
  */
object TypeStringUtils extends JavaTokenParsers with PackratParsers {
  case class Keyword(key: String)

  // convert the keyword into an case insensitive Parser
  implicit def keyword2Parser(kw: Keyword): Parser[String] = {
    ("""(?i)\Q""" + kw.key + """\E""").r
  }

  lazy val VARCHAR: Keyword = Keyword("VARCHAR")
  lazy val STRING: Keyword = Keyword("STRING")
  lazy val BOOLEAN: Keyword = Keyword("BOOLEAN")
  lazy val BYTE: Keyword = Keyword("BYTE")
  lazy val TINYINT: Keyword = Keyword("TINYINT")
  lazy val SHORT: Keyword = Keyword("SHORT")
  lazy val SMALLINT: Keyword = Keyword("SMALLINT")
  lazy val INT: Keyword = Keyword("INT")
  lazy val LONG: Keyword = Keyword("LONG")
  lazy val BIGINT: Keyword = Keyword("BIGINT")
  lazy val FLOAT: Keyword = Keyword("FLOAT")
  lazy val DOUBLE: Keyword = Keyword("DOUBLE")
  lazy val DECIMAL: Keyword = Keyword("DECIMAL")
  lazy val SQL_DATE: Keyword = Keyword("SQL_DATE")
  lazy val DATE: Keyword = Keyword("DATE")
  lazy val SQL_TIME: Keyword = Keyword("SQL_TIME")
  lazy val TIME: Keyword = Keyword("TIME")
  lazy val SQL_TIMESTAMP: Keyword = Keyword("SQL_TIMESTAMP")
  lazy val TIMESTAMP: Keyword = Keyword("TIMESTAMP")
  lazy val ROW: Keyword = Keyword("ROW")
  lazy val ANY: Keyword = Keyword("ANY")
  lazy val POJO: Keyword = Keyword("POJO")

  lazy val qualifiedName: Parser[String] =
    """\p{javaJavaIdentifierStart}[\p{javaJavaIdentifierPart}.]*""".r

  lazy val base64Url: Parser[String] =
    """[A-Za-z0-9_-]*""".r

  lazy val atomic: PackratParser[TypeInformation[_]] =
    (VARCHAR | STRING) ^^ { e => Types.STRING } |
    BOOLEAN ^^ { e => Types.BOOLEAN } |
    (TINYINT | BYTE) ^^ { e => Types.BYTE } |
    (SMALLINT | SHORT) ^^ { e => Types.SHORT } |
    INT ^^ { e => Types.INT } |
    (BIGINT | LONG) ^^ { e => Types.LONG } |
    FLOAT ^^ { e => Types.FLOAT } |
    DOUBLE ^^ { e => Types.DOUBLE } |
    DECIMAL ^^ { e => Types.DECIMAL } |
    (DATE | SQL_DATE) ^^ { e => Types.SQL_DATE.asInstanceOf[TypeInformation[_]] } |
    (TIMESTAMP | SQL_TIMESTAMP) ^^ { e => Types.SQL_TIMESTAMP } |
    (TIME | SQL_TIME) ^^ { e => Types.SQL_TIME }

  lazy val escapedFieldName: PackratParser[String] = "\"" ~> stringLiteral <~ "\"" ^^ { s =>
    StringEscapeUtils.unescapeJava(s)
  }

  lazy val fieldName: PackratParser[String] = escapedFieldName | stringLiteral | ident

  lazy val field: PackratParser[(String, TypeInformation[_])] =
    fieldName ~ typeInfo ^^ {
      case name ~ info => (name, info)
    }

  lazy val namedRow: PackratParser[TypeInformation[_]] =
    ROW ~ "(" ~> rep1sep(field, ",") <~ ")" ^^ {
      fields => Types.ROW(fields.map(_._1).toArray, fields.map(_._2).toArray)
    } | failure("Named row type expected.")

  lazy val unnamedRow: PackratParser[TypeInformation[_]] =
    ROW ~ "(" ~> rep1sep(typeInfo, ",") <~ ")" ^^ {
      types => Types.ROW(types: _*)
    } | failure("Unnamed row type expected.")

  lazy val generic: PackratParser[TypeInformation[_]] =
    ANY ~ "(" ~> qualifiedName <~ ")" ^^ {
      typeClass =>
        val clazz = loadClass(typeClass)
        new GenericTypeInfo[AnyRef](clazz.asInstanceOf[Class[AnyRef]])
    }

  lazy val pojo: PackratParser[TypeInformation[_]] = POJO ~ "(" ~> qualifiedName <~ ")" ^^ {
    typeClass =>
      val clazz = loadClass(typeClass)
      val info = TypeExtractor.createTypeInfo(clazz)
      if (!info.isInstanceOf[PojoTypeInfo[_]]) {
        throw new ValidationException(s"Class '$typeClass'is not a POJO type.")
      }
      info
  }

  lazy val any: PackratParser[TypeInformation[_]] =
    ANY ~ "(" ~ qualifiedName ~ "," ~ base64Url ~ ")" ^^ {
      case _ ~ _ ~ typeClass ~ _ ~ serialized ~ _=>
        val clazz = loadClass(typeClass)
        val typeInfo = deserialize(serialized)

        if (clazz != typeInfo.getTypeClass) {
          throw new ValidationException(
            s"Class '$typeClass' does no correspond to serialized data.")
        }
        typeInfo
    }

  lazy val typeInfo: PackratParser[TypeInformation[_]] =
    namedRow | unnamedRow | any | generic | pojo | atomic | failure("Invalid type.")

  def readTypeInfo(typeString: String): TypeInformation[_] = {
    parseAll(typeInfo, typeString) match {
      case Success(lst, _) => lst

      case NoSuccess(msg, next) =>
        throwError(msg, next)
    }
  }

  // ----------------------------------------------------------------------------------------------

  def writeTypeInfo(typeInfo: TypeInformation[_]): String = typeInfo match {

    case Types.STRING => VARCHAR.key
    case Types.BOOLEAN => BOOLEAN.key
    case Types.BYTE => TINYINT.key
    case Types.SHORT => SMALLINT.key
    case Types.INT => INT.key
    case Types.LONG => BIGINT.key
    case Types.FLOAT => FLOAT.key
    case Types.DOUBLE => DOUBLE.key
    case Types.DECIMAL => DECIMAL.key
    case Types.SQL_DATE => DATE.key
    case Types.SQL_TIME => TIME.key
    case Types.SQL_TIMESTAMP => TIMESTAMP.key

    case rt: RowTypeInfo =>
      val fields = rt.getFieldNames.zip(rt.getFieldTypes)
      val normalizedFields = fields.map { f =>

        // escape field name if it contains spaces
        val name = if (!f._1.matches("\\S+")) {
          "\"" + StringEscapeUtils.escapeJava(f._1) + "\""
        } else {
          f._1
        }

        s"$name ${normalizeTypeInfo(f._2)}"
      }
      s"${ROW.key}(${normalizedFields.mkString(", ")})"

    case generic: GenericTypeInfo[_] =>
      s"${ANY.key}(${generic.getTypeClass.getName})"

    case pojo: PojoTypeInfo[_] =>
      // we only support very simple POJOs that only contain extracted fields
      // (not manually specified)
      val extractedInfo = try {
        Some(TypeExtractor.createTypeInfo(pojo.getTypeClass))
      } catch {
        case _: InvalidTypesException => None
      }
      extractedInfo match {
        case Some(ei) if ei == pojo => s"${POJO.key}(${pojo.getTypeClass.getName})"
        case _ =>
          throw new TableException(
            "A string representation for custom POJO types is not supported yet.")
      }

    case _: CompositeType[_] =>
      throw new TableException("A string representation for composite types is not supported yet.")

    case _: BasicArrayTypeInfo[_, _] | _: ObjectArrayTypeInfo[_, _] |
         _: PrimitiveArrayTypeInfo[_] =>
      throw new TableException("A string representation for array types is not supported yet.")

    case _: MapTypeInfo[_, _] | _: MultisetTypeInfo[_] =>
      throw new TableException("A string representation for map types is not supported yet.")

    case any: TypeInformation[_] =>
      s"${ANY.key}(${any.getTypeClass.getName}, ${serialize(any)})"
  }

  // ----------------------------------------------------------------------------------------------

  private def serialize(obj: Serializable): String = {
    try {
      val byteArray = InstantiationUtil.serializeObject(obj)
      Base64.encodeBase64URLSafeString(byteArray)
    } catch {
      case e: Exception =>
        throw new ValidationException(s"Unable to serialize type information '$obj' with " +
          s"class '${obj.getClass.getName}'.", e)
    }
  }

  private def deserialize(data: String): TypeInformation[_] = {
    val byteData = Base64.decodeBase64(data)
    InstantiationUtil
      .deserializeObject[TypeInformation[_]](byteData, Thread.currentThread.getContextClassLoader)
  }

  private def throwError(msg: String, next: Input): Nothing = {
    val improvedMsg = msg.replace("string matching regex `\\z'", "End of type information")

    throw new ValidationException(
      s"""Could not parse type information at column ${next.pos.column}: $improvedMsg
        |${next.pos.longString}""".stripMargin)
  }

  private def loadClass(qualifiedName: String): Class[_] = try {
    Class.forName(qualifiedName, true, Thread.currentThread.getContextClassLoader)
  } catch {
    case e: ClassNotFoundException =>
      throw new ValidationException("Class '" + qualifiedName + "' could not be found. " +
        "Please note that inner classes must be globally accessible and declared static.", e)
  }
}
