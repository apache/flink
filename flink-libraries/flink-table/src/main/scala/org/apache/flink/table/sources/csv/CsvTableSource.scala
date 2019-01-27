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

package org.apache.flink.table.sources.csv

import java.util.{Set => JSet}
import java.util.TimeZone
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.functions.{AsyncTableFunction, TableFunction}
import org.apache.flink.table.api.types.{DataType, DataTypes, InternalType, RowType, TypeConverters}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.sources._
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeUtils}
import org.apache.flink.table.util.Logging

import _root_.scala.collection.mutable
import _root_.scala.collection.JavaConversions._

/**
  * A [[BatchTableSource]] and [[StreamTableSource]] for simple CSV files with a
  * (logically) unlimited number of fields.
  *
  * @param path            The path to the CSV file.
  * @param fieldNames      The names of the table fields.
  * @param fieldTypes      The types of the table fields.
  * @param fieldDelim      The field delimiter, "," by default.
  * @param rowDelim        The row delimiter, "\n" by default.
  * @param quoteCharacter  An optional quote character for String values, null by default.
  * @param ignoreFirstLine Flag to ignore the first line, false by default.
  * @param ignoreComments  An optional prefix to indicate comments, null by default.
  * @param lenient         Flag to skip records with parse error instead to fail, false by default.
  * @param charset         The charset of the CSV file.
  */
class CsvTableSource(
    private val path: String,
    private val fieldNames: Array[String],
    private val fieldTypes: Array[InternalType],
    private val fieldNullables: Array[Boolean],
    private val fieldDelim: String,
    private val rowDelim: String,
    private val quoteCharacter: Character,
    private val ignoreFirstLine: Boolean,
    private val ignoreComments: String,
    private val lenient: Boolean,
    private val charset: String,
    private val emptyColumnAsNull: Boolean,
    private var isLimitPushdown: Boolean = false,
    private var limit: Long = Long.MaxValue,
    private var uniqueKeySet: JSet[JSet[String]] = null,
    private var indexKeySet: JSet[JSet[String]] = null,
    private var timezone: TimeZone = null,
    private var nestedEnumerate: Boolean = true)
  extends BatchTableSource[BaseRow]
  with StreamTableSource[BaseRow]
  with LookupableTableSource[BaseRow]
  with LimitableTableSource
  with ProjectableTableSource
  with Logging {

  if (fieldNames.length != fieldTypes.length) {
    throw new TableException("Number of field names and field types must be equal.")
  }

  if (fieldNames.length != fieldNullables.length) {
    throw new TableException("Number of field names and field nullables must be equal.")
  }

  private val returnType = new RowType(
    fieldTypes.toArray[DataType], fieldNames)
  private val returnTypeInfo = TypeConverters.toBaseRowTypeInfo(returnType)

  private var selectedFields: Array[Int] = fieldTypes.indices.toArray

  private var cachedStats: Option[TableStats] = None

  private lazy val inputFormat = createCsvInput()

  /** Returns the [[DataType]] for the return type of the [[CsvTableSource]]. */
  override def getReturnType: DataType = returnType

  /**
    * Returns the data of the table as a [[DataStream]] of [[BaseRow]].
    *
    * NOTE: This method is for internal use only for defining a [[TableSource]].
    * Do not use it in Table API programs.
    */
  override def getDataStream(streamExecEnv: StreamExecutionEnvironment): DataStream[BaseRow] = {
    streamExecEnv.createInputV2(createCsvInput(), returnTypeInfo)
  }

  /**
    * Returns the data of the table as a [[DataStream]].
    *
    * NOTE: This method is for internal use only for defining a [[TableSource]].
    * Do not use it in Table API programs.
    */
  override def getBoundedStream(streamExecEnv: StreamExecutionEnvironment):
    DataStream[BaseRow] = {
    streamExecEnv.createInputV2(createCsvInput(), returnTypeInfo, s"csv source: $path")
  }

  /** Returns a copy of [[TableSource]] with ability to project fields */
  override def projectFields(fields: Array[Int]): CsvTableSource = {

    val (newFields, newFieldNames, newFieldTypes, newFieldNullables) = if (fields.nonEmpty) {
      (fields, fields.map(fieldNames(_)), fields.map(fieldTypes(_)), fields.map(fieldNullables(_)))
    } else {
      // reporting number of records only, we must read some columns to get row count.
      // (e.g. SQL: select count(1) from csv_table)
      // We choose the first column here.
      (
          Array(0),
          Array(fieldNames.head),
          Array[InternalType](fieldTypes.head),
          Array[Boolean](fieldNullables.head))
    }

    val source = new CsvTableSource(path,
      newFieldNames,
      newFieldTypes,
      newFieldNullables,
      fieldDelim,
      rowDelim,
      quoteCharacter,
      ignoreFirstLine,
      ignoreComments,
      lenient,
      charset,
      emptyColumnAsNull,
      isLimitPushdown,
      limit,
      null,
      null,
      timezone,
      nestedEnumerate
    )
    source.selectedFields = newFields
    source
  }


  /**
   * Check and push down the limit to the table source.
   *
   * @param applylimit the value which limit the number of records.
   * @return A new cloned instance of [[TableSource]]
   */
  override def applyLimit(applylimit: Long): TableSource = {
    this.isLimitPushdown = true
    val csvTableSource = new CsvTableSource(
      path,
      fieldNames,
      fieldTypes,
      fieldNullables,
      fieldDelim,
      rowDelim,
      quoteCharacter,
      ignoreFirstLine,
      ignoreComments,
      lenient,
      charset,
      emptyColumnAsNull,
      true,
      applylimit,
      uniqueKeySet,
      indexKeySet,
      timezone,
      nestedEnumerate)
    csvTableSource.selectedFields = selectedFields
    csvTableSource
  }

  /**
   * Return the flag to indicate whether limit push down has been tried. Must return true on
   * the returned instance of [[applyLimit]].
   */
  override def isLimitPushedDown: Boolean = isLimitPushdown

  private def createCsvInput(): BaseRowCsvInputFormat = {
    val inputFormat = new BaseRowCsvInputFormat(
      new Path(path),
      fieldTypes,
      rowDelim,
      fieldDelim,
      selectedFields,
      emptyColumnAsNull,
      limit)

    inputFormat.setSkipFirstLineAsHeader(ignoreFirstLine)
    inputFormat.setLenient(lenient)
    if (charset != null) {
      inputFormat.setCharset(charset)
    }
    if (quoteCharacter != null) {
      inputFormat.enableQuotedStringParsing(quoteCharacter)
    }
    if (ignoreComments != null) {
      inputFormat.setCommentPrefix(ignoreComments)
    }
    val tz = if (timezone == null) {
      TimeZone.getTimeZone("UTC")
    }
    else {
      timezone
    }

    inputFormat.setTimezone(tz)

    inputFormat.setNestedFileEnumeration(nestedEnumerate)

    inputFormat
  }

  override def equals(other: Any): Boolean = other match {
    case that: CsvTableSource => returnType == that.returnType &&
        path == that.path &&
        fieldDelim == that.fieldDelim &&
        rowDelim == that.rowDelim &&
        quoteCharacter == that.quoteCharacter &&
        ignoreFirstLine == that.ignoreFirstLine &&
        ignoreComments == that.ignoreComments &&
        lenient == that.lenient
    case _ => false
  }

  override def hashCode(): Int = {
    returnType.hashCode()
  }

  override def getTableStats: TableStats = {
    if (cachedStats.isEmpty) {
      val statistics = inputFormat.getStatistics(null)

      // Using Some(null) instead of None because getTableStats method will be executed
      // many times, while we want that getStatistics method is executed only once.
      cachedStats = if (statistics == null) {
        LOG.warn("no statistics provided")
        Some(null)
      }
      // getNumberOfRecords may be return -1 when file is empty
      else if (statistics.getNumberOfRecords < 0) {
        LOG.warn(s"Illegal number of records: ${statistics.getNumberOfRecords}")
        Some(null)
      }
      else {
        Some(TableStats(statistics.getNumberOfRecords))
      }
    }
    cachedStats.get
  }

  override def explainSource(): String = {
    val limitStr = if (isLimitPushedDown && limit < Long.MaxValue) s", limit=$limit" else ""
    s"selectedFields=[${fieldNames.mkString(", ")}]" + limitStr
  }

  override def getTableSchema: TableSchema = {
    val builder = TableSchema.builder()
    fieldNames.zip(fieldTypes).zip(fieldNullables).foreach {
      case ((name:String, tpe:InternalType), nullable:Boolean) =>
        builder.field(name, tpe, nullable)
    }
    if (uniqueKeySet != null) {
      uniqueKeySet.foreach { uniqueKey =>
          builder.uniqueIndex(uniqueKey.toArray(new Array[String](0)):_*)
      }
    }
    if (indexKeySet != null) {
      indexKeySet.foreach { indexKey =>
        builder.normalIndex(indexKey.toArray(new Array[String](0)):_*)
      }
    }
    builder.build()
  }

  override def getLookupFunction(lookupKeys: Array[Int]): TableFunction[BaseRow] = {
    val keyFieldNames = lookupKeys.map(fieldNames(_))
    val unique = getTableSchema.isUniqueColumns(keyFieldNames)
    val lookuper = new CsvLookupFunction(
      path,
      returnType,
      IndexKey.of(unique, lookupKeys:_*),
      emptyColumnAsNull,
      timezone,
      nestedEnumerate)
    lookuper.setCharsetName(charset)
    lookuper.setFieldDelim(fieldDelim)
    lookuper.setLineDelim(rowDelim)
    lookuper.setIgnoreComments(ignoreComments)
    lookuper.setLenient(lenient)
    lookuper.setQuoteCharacter(quoteCharacter)
    lookuper.setIgnoreFirstLine(ignoreFirstLine)
    lookuper
  }

  override def getAsyncLookupFunction(lookupKeys: Array[Int]): AsyncTableFunction[BaseRow] = {
    throw new UnsupportedOperationException("CSV do not support async lookup")
  }

  override def getLookupConfig: LookupConfig = new LookupConfig
}

object CsvTableSource {

  /**
    * A builder for creating [[CsvTableSource]] instances.
    *
    * For example:
    *
    * {{{
    *   val source: CsvTableSource = new CsvTableSource.builder()
    *     .path("/path/to/your/file.csv")
    *     .field("myfield", Types.STRING)
    *     .field("myfield2", Types.INT)
    *     .build()
    * }}}
    *
    */
  class Builder {

    private val schema: mutable.LinkedHashMap[String, (InternalType, Boolean)] =
      new mutable.LinkedHashMap()
    private var quoteCharacter: Character = _
    private var path: String = _
    private var fieldDelim: String = CsvInputFormat.DEFAULT_FIELD_DELIMITER
    private var lineDelim: String = CsvInputFormat.DEFAULT_LINE_DELIMITER
    private var isIgnoreFirstLine: Boolean = false
    private var commentPrefix: String = _
    private var lenient: Boolean = false
    private var charset: String = _
    private var emptyColumnAsNull: Boolean = _
    private var uniqueKeySet: JSet[JSet[String]] = _
    private var indexKeySet: JSet[JSet[String]] = _
    private var timezone: TimeZone = _
    private var enumerateNestedFile: Boolean = true

    /**
     * Sets charset of the CSV file.
     * @param charset charset.
     * @return
     */
    def charset(charset: String): Builder = {
      this.charset = charset
      this
    }

    /**
      * Sets the path to the CSV file. Required.
      *
      * @param path the path to the CSV file
      */
    def path(path: String): Builder = {
      this.path = path
      this
    }

    /**
      * Sets the field delimiter, "," by default.
      *
      * @param delim the field delimiter
      */
    def fieldDelimiter(delim: String): Builder = {
      this.fieldDelim = delim
      this
    }

    /**
      * Sets the line delimiter, "\n" by default.
      *
      * @param delim the line delimiter
      */
    def lineDelimiter(delim: String): Builder = {
      this.lineDelim = delim
      this
    }

    /**
      * Adds a field with the field name and the type information. Required.
      * This method can be called multiple times. The call order of this method defines
      * also the order of thee fields in a row.
      *
      * @param fieldName the field name
      * @param fieldType the type information of the field
      */
    def field(fieldName: String, fieldType: InternalType): Builder = {
      if (schema.contains(fieldName)) {
        throw new IllegalArgumentException(s"Duplicate field name $fieldName.")
      }
      schema += (fieldName ->(fieldType, !FlinkTypeFactory.isTimeIndicatorType(fieldType)))
      this
    }

    /**
      * Adds a field with the field name and the type information. Required.
      * This method can be called multiple times. The call order of this method defines
      * also the order of the fields in a row.
      *
      * @param fieldName the field name
      * @param fieldType the type information of the field
      */
    def field(fieldName: String, fieldType: TypeInformation[_]): Builder = {
      if (schema.contains(fieldName)) {
        throw new IllegalArgumentException(s"Duplicate field name $fieldName.")
      }
      val internalType = TypeConverters.createInternalTypeFromTypeInfo(fieldType)
      schema += (fieldName ->(internalType, !FlinkTypeFactory.isTimeIndicatorType(internalType)))
      this
    }

    /**
      * Adds a field with the field name and the type information. Required.
      * This method can be called multiple times. The call order of this method defines
      * also the order of thee fields in a row.
      *
      * @param fieldName     the field name
      * @param fieldType     the type of the field
      * @param isNullable true if the field could be nullable, else false
      */
    def field(fieldName: String, fieldType: InternalType, isNullable: Boolean): Builder = {
      if (schema.contains(fieldName)) {
        throw new IllegalArgumentException(s"Duplicate field name $fieldName.")
      }
      schema += (fieldName ->(fieldType, isNullable))
      this
    }

    def fields(
        fieldNames: Array[String],
        fieldTypes: Array[InternalType]): Builder = {
      fieldNames.zip(fieldTypes).foreach {
        case (name, t) => field(name, t)
      }
      this
    }

    def fields(
        fieldNames: Array[String],
        fieldTypes: Array[InternalType],
        isNullables: Array[Boolean]): Builder = {
      fieldNames.zip(fieldTypes).zip(isNullables).foreach {
        case ((name, t), isNull) => field(name, t, isNull)
      }
      this
    }

    /**
      * Sets a quote character for String values, null by default.
      *
      * @param quote the quote character
      */
    def quoteCharacter(quote: Character): Builder = {
      this.quoteCharacter = quote
      this
    }

    /**
      * Sets a prefix to indicate comments, null by default.
      *
      * @param prefix the prefix to indicate comments
      */
    def commentPrefix(prefix: String): Builder = {
      this.commentPrefix = prefix
      this
    }

    /**
      * Ignore the first line. Not skip the first line by default.
      */
    def ignoreFirstLine(): Builder = {
      this.isIgnoreFirstLine = true
      this
    }

    /**
      * Skip records with parse error instead to fail. Throw an exception by default.
      */
    def ignoreParseErrors(): Builder = {
      this.lenient = true
      this
    }

    /**
      * enableEmptyColumnAsNull.
      */
    def enableEmptyColumnAsNull(): Builder = {
      this.emptyColumnAsNull = true
      this
    }

    def uniqueKeys(uniqueKeys: JSet[JSet[String]]): Builder = {
      this.uniqueKeySet = uniqueKeys
      this
    }

    def indexKeys(indexKeys: JSet[JSet[String]]): Builder = {
      this.indexKeySet = indexKeys
      this
    }

    def timezone(tz: TimeZone): Builder = {
      this.timezone = tz
      this
    }

    def setNestedFileEnumerate(isEnabled: Boolean): Builder = {
      this.enumerateNestedFile = isEnabled
      this
    }

    /**
      * Apply the current values and constructs a newly-created [[CsvTableSource]].
      *
      * @return a newly-created [[CsvTableSource]].
      */
    def build(): CsvTableSource = {
      if (path == null) {
        throw new IllegalArgumentException("Path must be defined.")
      }
      if (schema.isEmpty) {
        throw new IllegalArgumentException("Fields can not be empty.")
      }
      new CsvTableSource(
        path,
        schema.keys.toArray,
        schema.values.map(_._1).toArray,
        schema.values.map(_._2).toArray,
        fieldDelim,
        lineDelim,
        quoteCharacter,
        isIgnoreFirstLine,
        commentPrefix,
        lenient,
        charset,
        emptyColumnAsNull,
        false,
        Long.MaxValue,
        uniqueKeySet,
        indexKeySet,
        timezone,
        enumerateNestedFile)
    }

  }

  /**
    * Return a new builder that builds a [[CsvTableSource]].
    *
    * For example:
    *
    * {{{
    *   val source: CsvTableSource = CsvTableSource
    *     .builder()
    *     .path("/path/to/your/file.csv")
    *     .field("myfield", Types.STRING)
    *     .field("myfield2", Types.INT)
    *     .build()
    * }}}
    *
    * @return a new builder to build a [[CsvTableSource]]
    */
  def builder(): Builder = new Builder
}
