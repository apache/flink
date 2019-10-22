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

package org.apache.flink.table.descriptors

import java.util
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.{DataTypes, ValidationException}
import org.apache.flink.table.descriptors.RowtimeTest.{CustomAssigner, CustomExtractor}
import org.apache.flink.table.expressions._
import org.apache.flink.table.expressions.utils.ApiExpressionUtils
import org.apache.flink.table.functions.BuiltInFunctionDefinitions
import org.apache.flink.table.sources.tsextractors.TimestampExtractor
import org.apache.flink.table.sources.wmstrategies.PunctuatedWatermarkAssigner
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row

import org.junit.Test

import scala.collection.JavaConverters._

class RowtimeTest extends DescriptorTestBase {

  @Test(expected = classOf[ValidationException])
  def testInvalidWatermarkType(): Unit = {
    addPropertyAndVerify(descriptors().get(0), "rowtime.watermarks.type", "xxx")
  }

  @Test(expected = classOf[ValidationException])
  def testMissingWatermarkClass(): Unit = {
    removePropertyAndVerify(descriptors().get(1), "rowtime.watermarks.class")
  }

  @Test(expected = classOf[ValidationException])
  def testUnsupportedSourceWatermarks(): Unit = {
    addPropertyAndVerify(descriptors().get(0), "rowtime.watermarks.type", "from-source")
  }

  // ----------------------------------------------------------------------------------------------

  override def descriptors(): util.List[Descriptor] = {
    val desc1 = new Rowtime()
      .timestampsFromField("otherField")
      .watermarksPeriodicBounded(1000L)

    val desc2 = new Rowtime()
      .timestampsFromSource()
      .watermarksFromStrategy(new CustomAssigner())

    val desc3 = new Rowtime()
      .timestampsFromExtractor(new CustomExtractor("tsField"))
      .watermarksPeriodicBounded(1000L)

    util.Arrays.asList(desc1, desc2, desc3)
  }

  override def validator(): DescriptorValidator = {
    new RowtimeValidator(true, false)
  }

  override def properties(): util.List[util.Map[String, String]] = {
    val props1 = Map(
      "rowtime.timestamps.type" -> "from-field",
      "rowtime.timestamps.from" -> "otherField",
      "rowtime.watermarks.type" -> "periodic-bounded",
      "rowtime.watermarks.delay" -> "1000"
    )

    val props2 = Map(
      "rowtime.timestamps.type" -> "from-source",
      "rowtime.watermarks.type" -> "custom",
      "rowtime.watermarks.class" -> "org.apache.flink.table.descriptors.RowtimeTest$CustomAssigner",
      "rowtime.watermarks.serialized" -> ("rO0ABXNyAD1vcmcuYXBhY2hlLmZsaW5rLnRhYmxlLmRlc2NyaX" +
        "B0b3JzLlJvd3RpbWVUZXN0JEN1c3RvbUFzc2lnbmVyeDcuDvfbu0kCAAB4cgBHb3JnLmFwYWNoZS5mbGluay" +
        "50YWJsZS5zb3VyY2VzLndtc3RyYXRlZ2llcy5QdW5jdHVhdGVkV2F0ZXJtYXJrQXNzaWduZXKBUc57oaWu9A" +
        "IAAHhyAD1vcmcuYXBhY2hlLmZsaW5rLnRhYmxlLnNvdXJjZXMud21zdHJhdGVnaWVzLldhdGVybWFya1N0cm" +
        "F0ZWd53nt-g2OWaT4CAAB4cA")
    )

    val props3 = Map(
      "rowtime.timestamps.type" -> "custom",
      "rowtime.timestamps.class" -> ("org.apache.flink.table.descriptors." +
        "RowtimeTest$CustomExtractor"),
      "rowtime.timestamps.serialized" -> ("rO0ABXNyAD5vcmcuYXBhY2hlLmZsaW5rLnRhYmxlLmRlc2NyaXB0b3" +
        "JzLlJvd3RpbWVUZXN0JEN1c3RvbUV4dHJhY3RvcoaChjMg55xwAgABTAAFZmllbGR0ABJMamF2YS9sYW5nL1N0cm" +
        "luZzt4cgA-b3JnLmFwYWNoZS5mbGluay50YWJsZS5zb3VyY2VzLnRzZXh0cmFjdG9ycy5UaW1lc3RhbXBFeHRyYW" +
        "N0b3Jf1Y6piFNsGAIAAHhwdAAHdHNGaWVsZA"),
      "rowtime.watermarks.type" -> "periodic-bounded",
      "rowtime.watermarks.delay" -> "1000"
    )

    util.Arrays.asList(props1.asJava, props2.asJava, props3.asJava)
  }
}

object RowtimeTest {

  class CustomAssigner extends PunctuatedWatermarkAssigner() {
    override def getWatermark(row: Row, timestamp: Long): Watermark =
      throw new UnsupportedOperationException()
  }

  class CustomExtractor(val field: String) extends TimestampExtractor {
    def this() = {
      this("ts")
    }

    override def getArgumentFields: Array[String] = Array(field)

    override def validateArgumentFields(argumentFieldTypes: Array[TypeInformation[_]]): Unit = {
      argumentFieldTypes(0) match {
        case Types.SQL_TIMESTAMP =>
        case _ =>
          throw new ValidationException(
            s"Field 'ts' must be of type Timestamp but is of type ${argumentFieldTypes(0)}.")
      }
    }

    override def getExpression(fieldAccesses: Array[ResolvedFieldReference]): Expression = {
      val fieldAccess = fieldAccesses(0)
      require(fieldAccess.resultType == Types.SQL_TIMESTAMP)
      val fieldReferenceExpr = new FieldReferenceExpression(
        fieldAccess.name,
        TypeConversions.fromLegacyInfoToDataType(fieldAccess.resultType),
        0,
        fieldAccess.fieldIndex)
      ApiExpressionUtils.unresolvedCall(
        BuiltInFunctionDefinitions.CAST,
        fieldReferenceExpr,
        ApiExpressionUtils.typeLiteral(DataTypes.BIGINT()))
    }

    override def equals(other: Any): Boolean = other match {
      case that: CustomExtractor => field == that.field
      case _ => false
    }

    override def hashCode(): Int = {
      field.hashCode
    }
  }
}
