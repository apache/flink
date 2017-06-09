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

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.io.FileOutputFormat.OutputDirectoryMode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path

import scala.reflect.ClassTag

/** FlinkMLTools contains a set of convenience functions for Flink's machine learning library:
  *
  *  - persist:
  *  Takes up to 5 [[DataSet]]s and file paths. Each [[DataSet]] is written to the specified
  *  path and subsequently re-read from disk. This method can be used to effectively split the
  *  execution graph at the given [[DataSet]]. Writing it to disk triggers its materialization
  *  and specifying it as a source will prevent the re-execution of it.
  *
  *  - block:
  *  Takes a DataSet of elements T and groups them in n blocks.
  *
  */
object FlinkMLTools {
  val EXECUTION_ENVIRONMENT_NAME = "FlinkMLTools persist"

  /** Registers the different FlinkML related types for Kryo serialization
    *
    * @param env The Flink execution environment where the types need to be registered
    */
  def registerFlinkMLTypes(env: ExecutionEnvironment): Unit = {

    // Vector types
    env.registerType(classOf[org.apache.flink.ml.math.DenseVector])
    env.registerType(classOf[org.apache.flink.ml.math.SparseVector])

    // Matrix types
    env.registerType(classOf[org.apache.flink.ml.math.DenseMatrix])
    env.registerType(classOf[org.apache.flink.ml.math.SparseMatrix])

    // Breeze Vector types
    env.registerType(classOf[breeze.linalg.DenseVector[_]])
    env.registerType(classOf[breeze.linalg.SparseVector[_]])

    // Breeze specialized types
    env.registerType(breeze.linalg.DenseVector.zeros[Double](0).getClass)
    env.registerType(breeze.linalg.SparseVector.zeros[Double](0).getClass)

    // Breeze Matrix types
    env.registerType(classOf[breeze.linalg.DenseMatrix[Double]])
    env.registerType(classOf[breeze.linalg.CSCMatrix[Double]])

    // Breeze specialized types
    env.registerType(breeze.linalg.DenseMatrix.zeros[Double](0, 0).getClass)
    env.registerType(breeze.linalg.CSCMatrix.zeros[Double](0, 0).getClass)
  }

  /** Writes a [[DataSet]] to the specified path and returns it as a DataSource for subsequent
    * operations.
    *
    * @param dataset [[DataSet]] to write to disk
    * @param path File path to write dataset to
    * @tparam T Type of the [[DataSet]] elements
    * @return [[DataSet]] reading the just written file
    */
  def persist[T: ClassTag: TypeInformation](dataset: DataSet[T], path: String): DataSet[T] = {
    val env = dataset.getExecutionEnvironment
    val outputFormat = new TypeSerializerOutputFormat[T]

    val filePath = new Path(path)

    outputFormat.setOutputFilePath(filePath)
    outputFormat.setWriteMode(WriteMode.OVERWRITE)

    dataset.output(outputFormat)
    env.execute(EXECUTION_ENVIRONMENT_NAME)

    val inputFormat = new TypeSerializerInputFormat[T](dataset.getType)
    inputFormat.setFilePath(filePath)

    env.createInput(inputFormat)
  }

  /** Writes multiple [[DataSet]]s to the specified paths and returns them as DataSources for
    * subsequent operations.
    *
    * @param ds1 First [[DataSet]] to write to disk
    * @param ds2 Second [[DataSet]] to write to disk
    * @param path1 Path for ds1
    * @param path2 Path for ds2
    * @tparam A Type of the first [[DataSet]]'s elements
    * @tparam B Type of the second [[DataSet]]'s elements
    * @return Tuple of [[DataSet]]s reading the just written files
    */
  def persist[A: ClassTag: TypeInformation ,B: ClassTag: TypeInformation](ds1: DataSet[A], ds2:
  DataSet[B], path1: String, path2: String): (DataSet[A], DataSet[B]) = {
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

    env.execute(EXECUTION_ENVIRONMENT_NAME)

    val if1 = new TypeSerializerInputFormat[A](ds1.getType)
    if1.setFilePath(f1)

    val if2 = new TypeSerializerInputFormat[B](ds2.getType)
    if2.setFilePath(f2)

    (env.createInput(if1), env.createInput(if2))
  }

  /** Writes multiple [[DataSet]]s to the specified paths and returns them as DataSources for
    * subsequent operations.
    *
    * @param ds1 First [[DataSet]] to write to disk
    * @param ds2 Second [[DataSet]] to write to disk
    * @param ds3 Third [[DataSet]] to write to disk
    * @param path1 Path for ds1
    * @param path2 Path for ds2
    * @param path3 Path for ds3
    * @tparam A Type of first [[DataSet]]'s elements
    * @tparam B Type of second [[DataSet]]'s elements
    * @tparam C Type of third [[DataSet]]'s elements
    * @return Tuple of [[DataSet]]s reading the just written files
    */
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

    env.execute(EXECUTION_ENVIRONMENT_NAME)

    val if1 = new TypeSerializerInputFormat[A](ds1.getType)
    if1.setFilePath(f1)

    val if2 = new TypeSerializerInputFormat[B](ds2.getType)
    if2.setFilePath(f2)

    val if3 = new TypeSerializerInputFormat[C](ds3.getType)
    if3.setFilePath(f3)

    (env.createInput(if1), env.createInput(if2), env.createInput(if3))
  }

  /** Writes multiple [[DataSet]]s to the specified paths and returns them as DataSources for
    * subsequent operations.
    *
    * @param ds1 First [[DataSet]] to write to disk
    * @param ds2 Second [[DataSet]] to write to disk
    * @param ds3 Third [[DataSet]] to write to disk
    * @param ds4 Fourth [[DataSet]] to write to disk
    * @param path1 Path for ds1
    * @param path2 Path for ds2
    * @param path3 Path for ds3
    * @param path4 Path for ds4
    * @tparam A Type of first [[DataSet]]'s elements
    * @tparam B Type of second [[DataSet]]'s elements
    * @tparam C Type of third [[DataSet]]'s elements
    * @tparam D Type of fourth [[DataSet]]'s elements
    * @return Tuple of [[DataSet]]s reading the just written files
    */
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

    env.execute(EXECUTION_ENVIRONMENT_NAME)

    val if1 = new TypeSerializerInputFormat[A](ds1.getType)
    if1.setFilePath(f1)

    val if2 = new TypeSerializerInputFormat[B](ds2.getType)
    if2.setFilePath(f2)

    val if3 = new TypeSerializerInputFormat[C](ds3.getType)
    if3.setFilePath(f3)

    val if4 = new TypeSerializerInputFormat[D](ds4.getType)
    if4.setFilePath(f4)

    (env.createInput(if1), env.createInput(if2), env.createInput(if3), env.createInput(if4))
  }

  /** Writes multiple [[DataSet]]s to the specified paths and returns them as DataSources for
    * subsequent operations.
    *
    * @param ds1 First [[DataSet]] to write to disk
    * @param ds2 Second [[DataSet]] to write to disk
    * @param ds3 Third [[DataSet]] to write to disk
    * @param ds4 Fourth [[DataSet]] to write to disk
    * @param ds5 Fifth [[DataSet]] to write to disk
    * @param path1 Path for ds1
    * @param path2 Path for ds2
    * @param path3 Path for ds3
    * @param path4 Path for ds4
    * @param path5 Path for ds5
    * @tparam A Type of first [[DataSet]]'s elements
    * @tparam B Type of second [[DataSet]]'s elements
    * @tparam C Type of third [[DataSet]]'s elements
    * @tparam D Type of fourth [[DataSet]]'s elements
    * @tparam E Type of fifth [[DataSet]]'s elements
    * @return Tuple of [[DataSet]]s reading the just written files
    */
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

    env.execute(EXECUTION_ENVIRONMENT_NAME)

    val if1 = new TypeSerializerInputFormat[A](ds1.getType)
    if1.setFilePath(f1)

    val if2 = new TypeSerializerInputFormat[B](ds2.getType)
    if2.setFilePath(f2)

    val if3 = new TypeSerializerInputFormat[C](ds3.getType)
    if3.setFilePath(f3)

    val if4 = new TypeSerializerInputFormat[D](ds4.getType)
    if4.setFilePath(f4)

    val if5 = new TypeSerializerInputFormat[E](ds5.getType)
    if5.setFilePath(f5)

    (env.createInput(if1), env.createInput(if2), env.createInput(if3), env.createInput(if4), env
      .createInput(if5))
  }

  /** Groups the DataSet input into numBlocks blocks.
    * 
    * @param input the input dataset
    * @param numBlocks Number of Blocks
    * @param partitionerOption Optional partitioner to control the partitioning
    * @tparam T Type of the [[DataSet]]'s elements
    * @return The different datasets grouped into blocks
    */
  def block[T: TypeInformation: ClassTag](
    input: DataSet[T],
    numBlocks: Int,
    partitionerOption: Option[Partitioner[Int]] = None)
  : DataSet[Block[T]] = {
    val blockIDInput = input map {
      element =>
        val blockID = element.hashCode() % numBlocks

        val blockIDResult = if(blockID < 0){
          blockID + numBlocks
        } else {
          blockID
        }

        (blockIDResult, element)
    }

    val preGroupBlockIDInput = partitionerOption match {
      case Some(partitioner) =>
        blockIDInput partitionCustom(partitioner, 0)

      case None => blockIDInput
    }

    preGroupBlockIDInput.groupBy(0).reduceGroup {
      iterator => {
        val array = iterator.toVector

        val blockID = array.head._1
        val elements = array.map(_._2)

        Block[T](blockID, elements)
      }
    }.withForwardedFields("0 -> index")
  }

  /** Distributes the elements by taking the modulo of their keys and assigning it to this channel
    *
    */
  object ModuloKeyPartitioner extends Partitioner[Int] {
    override def partition(key: Int, numPartitions: Int): Int = {
      val result = key % numPartitions

      if(result < 0) {
        result + numPartitions
      } else {
        result
      }
    }
  }
}
