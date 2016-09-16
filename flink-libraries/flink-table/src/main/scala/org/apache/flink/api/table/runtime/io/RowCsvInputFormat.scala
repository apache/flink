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

package org.apache.flink.api.table.runtime.io

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.io.ParseException
import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.api.java.io.CsvInputFormat.{DEFAULT_FIELD_DELIMITER, DEFAULT_LINE_DELIMITER, createDefaultMask, toBooleanMask}
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.runtime.io.RowCsvInputFormat.extractTypeClasses
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.core.fs.Path
import org.apache.flink.types.parser.FieldParser
import org.apache.flink.types.parser.FieldParser.ParseErrorState

@Internal
@SerialVersionUID(1L)
class RowCsvInputFormat(
    filePath: Path,
    rowTypeInfo: RowTypeInfo,
    lineDelimiter: String = DEFAULT_LINE_DELIMITER,
    fieldDelimiter: String = DEFAULT_FIELD_DELIMITER,
    includedFieldsMask: Array[Boolean] = null)
  extends CsvInputFormat[Row](filePath) {

  if (rowTypeInfo.getArity == 0) {
    throw new IllegalArgumentException("Row arity must be greater than 0.")
  }
  private val arity = rowTypeInfo.getArity
  private lazy val defaultFieldMask = createDefaultMask(arity)
  private val fieldsMask = Option(includedFieldsMask).getOrElse(defaultFieldMask)

  // prepare CsvInputFormat
  setDelimiter(lineDelimiter)
  setFieldDelimiter(fieldDelimiter)
  setFieldsGeneric(fieldsMask, extractTypeClasses(rowTypeInfo))

  def this(
      filePath: Path,
      rowTypeInfo: RowTypeInfo,
      lineDelimiter: String,
      fieldDelimiter: String,
      includedFieldsMask: Array[Int]) {
    this(
      filePath,
      rowTypeInfo,
      lineDelimiter,
      fieldDelimiter,
      if (includedFieldsMask == null) {
        null
      } else {
        toBooleanMask(includedFieldsMask)
      })
  }

  def this(
      filePath: Path,
      rowTypeInfo: RowTypeInfo,
      includedFieldsMask: Array[Int]) {
    this(
      filePath,
      rowTypeInfo,
      DEFAULT_LINE_DELIMITER,
      DEFAULT_FIELD_DELIMITER,
      includedFieldsMask)
  }

  def fillRecord(reuse: Row, parsedValues: Array[AnyRef]): Row = {
    val reuseRow = if (reuse == null) {
      new Row(arity)
    } else {
      reuse
    }
    var i: Int = 0
    while (i < parsedValues.length) {
      reuse.setField(i, parsedValues(i))
      i += 1
    }
    reuseRow
  }

  @throws[ParseException]
  override protected def parseRecord(
      holders: Array[AnyRef],
      bytes: Array[Byte],
      offset: Int,
      numBytes: Int)
    : Boolean = {
    val fieldDelimiter = this.getFieldDelimiter
    val fieldIncluded: Array[Boolean] = this.fieldIncluded

    var startPos = offset
    val limit = offset + numBytes

    var field = 0
    var output = 0
    while (field < fieldIncluded.length) {

      // check valid start position
      if (startPos >= limit) {
        if (isLenient) {
          return false
        } else {
          throw new ParseException("Row too short: " + new String(bytes, offset, numBytes))
        }
      }

      if (fieldIncluded(field)) {
        // parse field
        val parser: FieldParser[AnyRef] = this.getFieldParsers()(output)
          .asInstanceOf[FieldParser[AnyRef]]
        val latestValidPos = startPos
        startPos = parser.resetErrorStateAndParse(
          bytes,
          startPos,
          limit,
          fieldDelimiter,
          holders(output))

        if (!isLenient && (parser.getErrorState ne ParseErrorState.NONE)) {
          // Row is able to handle null values
          if (parser.getErrorState ne ParseErrorState.EMPTY_STRING) {
            throw new ParseException(s"Parsing error for column $field of row '"
              + new String(bytes, offset, numBytes)
              + s"' originated by ${parser.getClass.getSimpleName}: ${parser.getErrorState}.")
          }
        }
        holders(output) = parser.getLastResult

        // check parse result
        if (startPos < 0) {
          holders(output) = null
          startPos = skipFields(bytes, latestValidPos, limit, fieldDelimiter)
        }
        output += 1
      } else {
        // skip field
        startPos = skipFields(bytes, startPos, limit, fieldDelimiter)
      }

      // check if something went wrong
      if (startPos < 0) {
        throw new ParseException(s"Unexpected parser position for column $field of row '"
          + new String(bytes, offset, numBytes) + "'")
      }

      field += 1
    }
    true
  }
}

object RowCsvInputFormat {

  private def extractTypeClasses(rowTypeInfo: RowTypeInfo): Array[Class[_]] = {
    val classes = for (i <- 0 until rowTypeInfo.getArity)
      yield rowTypeInfo.getTypeAt(i).getTypeClass
    classes.toArray
  }

}
