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

package org.apache.flink.table.sinks.orc

import java.io.File
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api._
import org.apache.flink.table.api.types.RowType
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sinks.{BatchTableSink, TableSinkBase}

import org.apache.hadoop.fs.FileUtil
import org.apache.orc.CompressionKind

/**
  * A subclass of [[BatchTableSink]] to write [[BaseRow]] to Orc files.
  *
  * @param dir         The output path to write the Table to.
  * @param writeMode   The write mode to specify whether existing files are overwritten or not.
  * @param compression compression algorithms.
  */
class OrcTableSink(
    dir: String,
    writeMode: Option[WriteMode] = None,
    compression: CompressionKind = CompressionKind.NONE)
  extends TableSinkBase[BaseRow]
  with BatchTableSink[BaseRow] {

  /** Return a deep copy of the [[org.apache.flink.table.sinks.TableSink]]. */
  override protected def copy: TableSinkBase[BaseRow] = {
    new OrcTableSink(dir, writeMode, compression)
  }

  /** Emits the BoundedStream. */
  override def emitBoundedStream(
      boundedStream: DataStream[BaseRow],
      tableConfig: TableConfig,
      executionConfig: ExecutionConfig): DataStreamSink[_] = {
    writeMode match {
      case Some(wm) if wm == WriteMode.OVERWRITE =>
        FileUtil.fullyDelete(new File(dir))
      case _ =>
        val path = new Path(dir)
        if (path.getFileSystem.exists(path) && !path.getFileSystem.getFileStatus(path).isDir) {
          throw new RuntimeException( "output dir [" + dir + "] already existed.")
        }
    }
    boundedStream.writeUsingOutputFormat(new RowOrcOutputFormat(
        getFieldTypes.map(_.toInternalType), getFieldNames, dir, compression))
      .name("Orc Sink: " + dir)
  }

  /**
    * Return the type expected by this [[org.apache.flink.table.sinks.TableSink]].
    *
    * This type should depend on the types returned by [[getFieldNames]].
    *
    * @return The type expected by this [[org.apache.flink.table.sinks.TableSink]].
    */
  override def getOutputType: RowType =
    new RowType(getFieldTypes: _*)
}
