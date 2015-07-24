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
package org.apache.flink.api.java.table

import java.lang.reflect.ParameterizedType

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{CsvInputFormat, CsvReader}
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.api.java.table.TableEnvironment.CsvOptions
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment, Utils}
import org.apache.flink.api.table.Table
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStream

/**
 * Environment for working with the Table API.
 *
 * This can be used to convert [[DataSet]] or [[DataStream]] to a [[Table]] and back again. You
 * can also use the provided methods to create a [[Table]] directly from a data source.
 */
class TableEnvironment {

  /**
   * Transforms the given DataSet to a [[org.apache.flink.api.table.Table]].
   * The fields of the DataSet type are renamed to the given set of fields:
   *
   * Example:
   *
   * {{{
   *   tableEnv.fromDataSet(set, "a, b")
   * }}}
   *
   * This will transform the set containing elements of two fields to a table where the fields
   * are named a and b.
   */
  def fromDataSet[T](set: DataSet[T], fields: String): Table = {
    new JavaBatchTranslator().createTable(set, fields)
  }

  /**
   * Transforms the given DataSet to a [[org.apache.flink.api.table.Table]].
   * The fields of the DataSet type are used to name the
   * [[org.apache.flink.api.table.Table]] fields.
   */
  def fromDataSet[T](set: DataSet[T]): Table = {
    new JavaBatchTranslator().createTable(set)
  }

  /**
   * Transforms the given DataStream to a [[org.apache.flink.api.table.Table]].
   * The fields of the DataStream type are renamed to the given set of fields:
   *
   * Example:
   *
   * {{{
   *   tableEnv.fromDataStream(set, "a, b")
   * }}}
   *
   * This will transform the set containing elements of two fields to a table where the fields
   * are named a and b.
   */
  def fromDataStream[T](set: DataStream[T], fields: String): Table = {
    new JavaStreamingTranslator().createTable(set, fields)
  }

  /**
   * Transforms the given DataStream to a [[org.apache.flink.api.table.Table]].
   * The fields of the DataStream type are used to name the
   * [[org.apache.flink.api.table.Table]] fields.
   */
  def fromDataStream[T](set: DataStream[T]): Table = {
    new JavaStreamingTranslator().createTable(set)
  }

  /**
   * Converts the given [[org.apache.flink.api.table.Table]] to
   * a DataSet. The given type must have exactly the same fields as the
   * [[org.apache.flink.api.table.Table]]. That is, the names of the
   * fields and the types must match.
   */
  @SuppressWarnings(Array("unchecked"))
  def toDataSet[T](table: Table, clazz: Class[T]): DataSet[T] = {
    new JavaBatchTranslator().translate[T](table.operation)(
      TypeExtractor.createTypeInfo(clazz).asInstanceOf[TypeInformation[T]])
  }

  /**
   * Converts the given [[org.apache.flink.api.table.Table]] to
   * a DataStream. The given type must have exactly the same fields as the
   * [[org.apache.flink.api.table.Table]]. That is, the names of the
   * fields and the types must match.
   */
  @SuppressWarnings(Array("unchecked"))
  def toDataStream[T](table: Table, clazz: Class[T]): DataStream[T] = {
    new JavaStreamingTranslator().translate[T](table.operation)(
      TypeExtractor.createTypeInfo(clazz).asInstanceOf[TypeInformation[T]])

  }

  /**
   * Create a Table from csv file, user needs to give an Array of Class. Support column numbers
   * up to 25. The CSV file is parsed using default line/field delimiter, and other default
   * options. This method is mainly for java api because java doesn't have default parameter
   * mechanism
   * * Example:
   *
   * {{{
   *   tableEnv.fromDataStream(path, env, Array(classOf[String, classOf[String], classOf[String]]))
   * }}}
   * @param path The path of the csv file
   * @param env the execution env
   * @param types an array of types for each column of the table
   * @return
   */
  def fromCsvFile(path: Path, env: ExecutionEnvironment,
                                  types:Array[Class[_]]): Table = {

    val tupleClazz = Tuple.getTupleClass(types.length)
    fromCsvFileInternalDefault(path, env, tupleClazz, types)
  }

  /**
   * Create a [[Table]] From csv file, user needs to given an Array of Class to match the table
   * types. Support column numbers up to 25. The csv file is parsed using options passed by the
   * user.
   * @param path The path of the csv file
   * @param env the execution env
   * @param types an array of types for each column of the table
   * @param fields string of field names
   * @param options options for parsing the csv file
   * @return
   */
  def fromCsvFile(path: Path, env: ExecutionEnvironment,
                  types: Array[Class[_]], fields: String = null,
                  options: CsvOptions): Table = {
    val tupleClazz = Tuple.getTupleClass(types.length)
    fromCsvFileInternal(path, env, tupleClazz, types, fields, options)
  }

  /**
   * helper method for creating table from  csv file
   */
  private def fromCsvFileInternalDefault[T <: JavaTuple](path: Path, env: ExecutionEnvironment,
                                                      tupleClazz: Class[T],
                                                      types: Array[Class[_]]): Table = {

    val tupleTypeInfo: TupleTypeInfo[T] = TupleTypeInfo.getBasicTupleTypeInfo(types: _*)
    val inputFormat: CsvInputFormat[T] = new CsvInputFormat[T](path, tupleTypeInfo)
    new JavaBatchTranslator().createTable(
      new DataSource[T](env, inputFormat, tupleTypeInfo, Utils.getCallLocationName))
  }


  /**
   * helper method for creating table from csv file
   */
  private def fromCsvFileInternal[T <: JavaTuple](path: Path, env: ExecutionEnvironment,
                                               tupleClazz: Class[T],
                                               types: Array[Class[_]], fields: String = null,
                                               options: CsvOptions): Table = {

    //create the TupleTypeInfo
    val tupleTypeInfo: TupleTypeInfo[T] = TupleTypeInfo.getBasicTupleTypeInfo(types: _*)

    //configure CsvInputFormat
    val format: CsvInputFormat[T] = new CsvInputFormat[T](path, tupleTypeInfo)
    format.setDelimiter(options.lineDelimiter)
    format.setFieldDelimiter(options.fieldDelimiter)
    format.setCommentPrefix(options.commentPrefix)
    format.setSkipFirstLineAsHeader(options.skipFirstLineAsHeader)
    format.setLenient(options.ignoreInvalidLines)
    if (options.parseQuotedStrings) {
      format.enableQuotedStringParsing(options.quoteCharacter)
    }

    if (options.includedMask != null) {
      format.setFields(options.includedMask, types)
    }

    //create the table
    fields match {
      case null => new JavaBatchTranslator().createTable(
        new DataSource[T](env, format, tupleTypeInfo, Utils.getCallLocationName))
      case _ => new JavaBatchTranslator().createTable(
        new DataSource[T](env, format, tupleTypeInfo, Utils.getCallLocationName), fields)
    }

  }

  /**
   * Create a Table from csv file, the columns are represented as the fileds of a pojo class
   * User have to provide the pojo class and the list of fields.
   * @param path The path of the csv file
   * @param env the execution env
   * @param clazz the pojo class to represent a row of the table
   * @param pojoFields fields of the pojo class used in the table
   * @return
   */
  def fromCsvFile[T](path: Path, env: ExecutionEnvironment, clazz: Class[T], pojoFields: String):
  Table = {

    val fields = pojoFields.split(",")
    new JavaBatchTranslator().createTable(
      new CsvReader(path, env).pojoType[T](clazz, fields: _*)
    )
  }

  type JavaTuple = org.apache.flink.api.java.tuple.Tuple

  /**
   * Create a Table from csv file, using default delimiter "," and line delimiter. Using all
   * Fields and The Rows are represented as a Parameterized Tuple Type.
   * @param path The path of the csv file
   * @param env the execution env
   * @param clazz The [[ParameterizedType]] Tuple class representing the row of the table
   * @tparam T
   * @return
   */
  def fromCsvFile[T <: JavaTuple with ParameterizedType](path: String, env: ExecutionEnvironment,
                                                         clazz: Class[T]): Table = {
    val csvReader = env.readCsvFile(path)
    new JavaBatchTranslator().createTable(csvReader.tupleType(clazz))
  }
}

object TableEnvironment {
  class CsvOptions {
    //default option
    var fieldDelimiter: String = CsvInputFormat.DEFAULT_FIELD_DELIMITER
    var lineDelimiter: String =  CsvInputFormat.DEFAULT_LINE_DELIMITER
    var includedMask: Array[Boolean] = null
    var commentPrefix: String = null
    var parseQuotedStrings: Boolean = false
    var quoteCharacter: Char = '"'
    var skipFirstLineAsHeader: Boolean = false
    var ignoreInvalidLines: Boolean = false

    //explicit setters for java api
    def setFieldDelimiter(fd:String) = {
      fieldDelimiter = fd
    }

    def setLineDelimiter(ld: String) = {
      lineDelimiter = ld
    }

    def setIncludedMask(im: Array[Boolean]) = {
      includedMask = im
    }

    def setCommentPrefix(cp: String) = {
      commentPrefix = cp
    }

    def setParseQuotedStrings(pq: Boolean) = {
      parseQuotedStrings = pq
    }

    def setQuoteCharacter(qc: Char) = {
      quoteCharacter = qc
    }

    def setSkipFirstLineAsHeader(sf: Boolean) = {
      skipFirstLineAsHeader = sf
    }

    def setIgnoreInvalidLines(ii: Boolean) = {
      ignoreInvalidLines = ii
    }
  }
}
