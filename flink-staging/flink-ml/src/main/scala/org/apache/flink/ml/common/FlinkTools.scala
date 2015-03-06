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

package org.apache.flink.ml.common

import org.apache.flink.api.common.io.FileOutputFormat.OutputDirectoryMode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path

import scala.reflect.ClassTag

/**
 * Collection of convenience functions
 */
object FlinkTools {

  /**
   *
   * @param dataset
   * @param path
   * @tparam T
   * @return
   */
  def persist[T: ClassTag: TypeInformation](dataset: DataSet[T], path: String): DataSet[T] = {
    val env = dataset.getExecutionEnvironment
    val outputFormat = new TypeSerializerOutputFormat[T]

    val filePath = new Path(path)

    outputFormat.setOutputFilePath(filePath)
    outputFormat.setWriteMode(WriteMode.OVERWRITE)

    dataset.output(outputFormat)
    env.execute("FlinkTools persist")

    val inputFormat = new TypeSerializerInputFormat[T](dataset.getType.createSerializer())
    inputFormat.setFilePath(filePath)

    env.createInput(inputFormat)
  }

  def persist[A: ClassTag: TypeInformation ,B: ClassTag: TypeInformation](ds1: DataSet[A], ds2:
  DataSet[B], path1: String, path2: String):(DataSet[A], DataSet[B])  = {
    val env = ds1.getExecutionEnvironment

    val f1 = new Path(path1)

    val of1 = new TypeSerializerOutputFormat[A]
    of1.setOutputFilePath(f1)
    of1.setWriteMode(WriteMode.OVERWRITE)

    ds1.output(of1)

    val f2 = new Path(path2)

    val of2 = new TypeSerializerOutputFormat[B]
    of2.setOutputFilePath(f2)
    of2.setWriteMode(WriteMode.OVERWRITE)

    ds2.output(of2)

    env.execute("FlinkTools persist")

    val if1 = new TypeSerializerInputFormat[A](ds1.getType.createSerializer())
    if1.setFilePath(f1)

    val if2 = new TypeSerializerInputFormat[B](ds2.getType.createSerializer())
    if2.setFilePath(f2)

    (env.createInput(if1), env.createInput(if2))
  }

  def persist[A: ClassTag: TypeInformation ,B: ClassTag: TypeInformation,
  C: ClassTag: TypeInformation](ds1: DataSet[A], ds2:  DataSet[B], ds3: DataSet[C], path1:
  String, path2: String, path3: String): (DataSet[A], DataSet[B], DataSet[C])  = {
    val env = ds1.getExecutionEnvironment

    val f1 = new Path(path1)

    val of1 = new TypeSerializerOutputFormat[A]
    of1.setOutputFilePath(f1)
    of1.setWriteMode(WriteMode.OVERWRITE)

    ds1.output(of1)

    val f2 = new Path(path2)

    val of2 = new TypeSerializerOutputFormat[B]
    of2.setOutputFilePath(f2)
    of2.setWriteMode(WriteMode.OVERWRITE)

    ds2.output(of2)

    val f3 = new Path(path3)

    val of3 = new TypeSerializerOutputFormat[C]
    of3.setOutputFilePath(f3)
    of3.setWriteMode(WriteMode.OVERWRITE)

    ds3.output(of3)

    env.execute("FlinkTools persist")

    val if1 = new TypeSerializerInputFormat[A](ds1.getType.createSerializer())
    if1.setFilePath(f1)

    val if2 = new TypeSerializerInputFormat[B](ds2.getType.createSerializer())
    if2.setFilePath(f2)

    val if3 = new TypeSerializerInputFormat[C](ds3.getType.createSerializer())
    if3.setFilePath(f3)

    (env.createInput(if1), env.createInput(if2), env.createInput(if3))
  }

  def persist[A: ClassTag: TypeInformation ,B: ClassTag: TypeInformation,
  C: ClassTag: TypeInformation, D: ClassTag: TypeInformation](ds1: DataSet[A], ds2:  DataSet[B],
                                                              ds3: DataSet[C], ds4: DataSet[D],
                                                              path1: String, path2: String, path3:
                                                              String, path4: String):
  (DataSet[A], DataSet[B], DataSet[C], DataSet[D])  = {
    val env = ds1.getExecutionEnvironment

    val f1 = new Path(path1)

    val of1 = new TypeSerializerOutputFormat[A]
    of1.setOutputFilePath(f1)
    of1.setWriteMode(WriteMode.OVERWRITE)

    ds1.output(of1)

    val f2 = new Path(path2)

    val of2 = new TypeSerializerOutputFormat[B]
    of2.setOutputFilePath(f2)
    of2.setWriteMode(WriteMode.OVERWRITE)

    ds2.output(of2)

    val f3 = new Path(path3)

    val of3 = new TypeSerializerOutputFormat[C]
    of3.setOutputFilePath(f3)
    of3.setWriteMode(WriteMode.OVERWRITE)

    ds3.output(of3)

    val f4 = new Path(path4)

    val of4 = new TypeSerializerOutputFormat[D]
    of4.setOutputFilePath(f4)
    of4.setWriteMode(WriteMode.OVERWRITE)

    ds4.output(of4)

    env.execute("FlinkTools persist")

    val if1 = new TypeSerializerInputFormat[A](ds1.getType.createSerializer())
    if1.setFilePath(f1)

    val if2 = new TypeSerializerInputFormat[B](ds2.getType.createSerializer())
    if2.setFilePath(f2)

    val if3 = new TypeSerializerInputFormat[C](ds3.getType.createSerializer())
    if3.setFilePath(f3)

    val if4 = new TypeSerializerInputFormat[D](ds4.getType.createSerializer())
    if4.setFilePath(f4)

    (env.createInput(if1), env.createInput(if2), env.createInput(if3), env.createInput(if4))
  }

  def persist[A: ClassTag: TypeInformation ,B: ClassTag: TypeInformation,
  C: ClassTag: TypeInformation, D: ClassTag: TypeInformation, E: ClassTag: TypeInformation]
  (ds1: DataSet[A], ds2:  DataSet[B], ds3: DataSet[C], ds4: DataSet[D], ds5: DataSet[E], path1:
  String, path2: String, path3: String, path4: String, path5: String): (DataSet[A], DataSet[B],
    DataSet[C], DataSet[D], DataSet[E])  = {
    val env = ds1.getExecutionEnvironment

    val f1 = new Path(path1)

    val of1 = new TypeSerializerOutputFormat[A]
    of1.setOutputFilePath(f1)
    of1.setWriteMode(WriteMode.OVERWRITE)

    ds1.output(of1)

    val f2 = new Path(path2)

    val of2 = new TypeSerializerOutputFormat[B]
    of2.setOutputFilePath(f2)
    of2.setOutputDirectoryMode(OutputDirectoryMode.ALWAYS)
    of2.setWriteMode(WriteMode.OVERWRITE)

    ds2.output(of2)

    val f3 = new Path(path3)

    val of3 = new TypeSerializerOutputFormat[C]
    of3.setOutputFilePath(f3)
    of3.setWriteMode(WriteMode.OVERWRITE)

    ds3.output(of3)

    val f4 = new Path(path4)

    val of4 = new TypeSerializerOutputFormat[D]
    of4.setOutputFilePath(f4)
    of4.setWriteMode(WriteMode.OVERWRITE)

    ds4.output(of4)

    val f5 = new Path(path5)

    val of5 = new TypeSerializerOutputFormat[E]
    of5.setOutputFilePath(f5)
    of5.setWriteMode(WriteMode.OVERWRITE)

    ds5.output(of5)

    env.execute("FlinkTools persist")

    val if1 = new TypeSerializerInputFormat[A](ds1.getType.createSerializer())
    if1.setFilePath(f1)

    val if2 = new TypeSerializerInputFormat[B](ds2.getType.createSerializer())
    if2.setFilePath(f2)

    val if3 = new TypeSerializerInputFormat[C](ds3.getType.createSerializer())
    if3.setFilePath(f3)

    val if4 = new TypeSerializerInputFormat[D](ds4.getType.createSerializer())
    if4.setFilePath(f4)

    val if5 = new TypeSerializerInputFormat[E](ds5.getType.createSerializer())
    if5.setFilePath(f5)

    (env.createInput(if1), env.createInput(if2), env.createInput(if3), env.createInput(if4), env
      .createInput(if5))
  }
}
