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

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.table.data.{RowData, StringData}
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction}
import org.apache.flink.types.Row

import _root_.java.lang.{Long => JLong}
import _root_.java.time.LocalDateTime
import _root_.java.util.concurrent.CompletableFuture
import _root_.java.util.{Collection => JCollection}

import scala.annotation.varargs


@SerialVersionUID(1L)
class InvalidTableFunctionResultType extends TableFunction[String] {
  @varargs
  def eval(obj: AnyRef*): Unit = {
  }
}

@SerialVersionUID(1L)
class InvalidTableFunctionEvalSignature extends TableFunction[RowData] {
  def eval(a: Integer, b: String, c: LocalDateTime): Unit = {
  }
}

@SerialVersionUID(1L)
class TableFunctionWithRowDataVarArg extends TableFunction[RowData] {
  @varargs
  def eval(obj: AnyRef*): Unit = {
  }
}

@SerialVersionUID(1L)
class TableFunctionWithRow extends TableFunction[Row] {
  def eval(a: Integer, b: String, c: LocalDateTime): Unit = {
  }
}

@SerialVersionUID(1L)
class InvalidAsyncTableFunctionEvalSignature1 extends AsyncTableFunction[RowData] {
  def eval(a: Integer, b: StringData, c: LocalDateTime): Unit = {
  }
}

@SerialVersionUID(1L)
class InvalidAsyncTableFunctionEvalSignature2 extends AsyncTableFunction[Row] {
  def eval(a: Integer, b: String,  c: LocalDateTime): Unit = {
  }
}

@SerialVersionUID(1L)
class InvalidAsyncTableFunctionEvalSignature3 extends AsyncTableFunction[RowData] {
  def eval(resultFuture: ResultFuture[RowData],
    a: Integer, b: StringData,  c: JLong): Unit = {
  }
}

@SerialVersionUID(1L)
class AsyncTableFunctionWithRowData extends AsyncTableFunction[RowData] {
  def eval(resultFuture: CompletableFuture[JCollection[RowData]],
    a: Integer, b: StringData, c: JLong): Unit = {
  }
}

@SerialVersionUID(1L)
class AsyncTableFunctionWithRowDataVarArg extends AsyncTableFunction[RowData] {
  @varargs
  def eval(resultFuture: CompletableFuture[JCollection[RowData]], objs: AnyRef*): Unit = {
  }
}

@SerialVersionUID(1L)
class AsyncTableFunctionWithRow extends AsyncTableFunction[Row] {
  @varargs
  def eval(obj: AnyRef*): Unit = {
  }
}
