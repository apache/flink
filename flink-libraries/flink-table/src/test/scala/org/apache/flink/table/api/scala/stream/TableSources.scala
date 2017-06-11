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
package org.apache.flink.table.api.scala.stream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.sources.{DefinedRowtimeAttribute, StreamTableSource}
import org.apache.flink.types.Row

object TableSources {

  class StreamTableSource0(
      datas: List[Row],
      returnTypeInfo: TypeInformation[Row],
      timeField: String,
      watermarkWithOffset: Long) extends StreamTableSource[Row] with DefinedRowtimeAttribute {

    override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {

      val timeFileIndex = getReturnType.asInstanceOf[RowTypeInfo].getFieldIndex(timeField)

      var dataWithTsAndWatermark: Seq[Either[(Long, Row), Long]] = Seq[Either[(Long, Row), Long]]()
      datas.foreach {
        data =>
          val left = Left(data.getField(timeFileIndex).asInstanceOf[Long], data)
          val right = Right(data.getField(timeFileIndex).asInstanceOf[Long] - watermarkWithOffset)
          dataWithTsAndWatermark = dataWithTsAndWatermark ++ Seq(left) ++ Seq(right)
      }

      execEnv
      .addSource(new EventTimeSourceFunction(dataWithTsAndWatermark))
      .returns(getReturnType)
    }

    override def getRowtimeAttribute: String = "rowtime"

    override def getReturnType: TypeInformation[Row] = returnTypeInfo
  }

  class EventTimeSourceFunction(
      dataWithTimestampList: Seq[Either[(Long, Row), Long]]) extends SourceFunction[Row] {
    override def run(ctx: SourceContext[Row]): Unit = {
      dataWithTimestampList.foreach {
        case Left(t) => ctx.collectWithTimestamp(t._2, t._1)
        case Right(w) => ctx.emitWatermark(new Watermark(w))
      }
    }

    override def cancel(): Unit = ???
  }

}
