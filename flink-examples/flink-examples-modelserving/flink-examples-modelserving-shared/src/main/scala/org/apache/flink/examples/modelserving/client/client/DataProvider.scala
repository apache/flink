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

package org.apache.flink.examples.modelserving.client.client

import org.apache.flink.modelserving.wine.winerecord.WineRecord
import org.apache.flink.examples.modelserving.configuration.ModelServingConfiguration
import org.apache.flink.model.modeldescriptor.ModelDescriptor
import java.io.{ByteArrayOutputStream, File}
import java.nio.file.{Files, Paths}

import com.google.protobuf.ByteString
import org.apache.flink.examples.modelserving.client.{KafkaLocalServer, MessageSender}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source

/**
 * Application publishing models from /data directory to Kafka
 */
object DataProvider {

  import ModelServingConfiguration._
  val file = "flink-examples/flink-examples-modelserving/flink-examples-modelserving-shared/" +
    "data/winequality_red.csv"
  val tensorfilesaved = "flink-examples/flink-examples-modelserving/" +
    "flink-examples-modelserving-shared/data/saved"
  val tensorfile = "flink-examples/flink-examples-modelserving/flink-examples-modelserving-shared" +
    "/data/optimized_WineQuality.pb"

  var dataTimeInterval = 1000 * 1 // 1 sec
  var modelTimeInterval = 1000 * 60 * 1 // 5 mins

  def main(args: Array[String]) {

    println(s"Using kafka brokers at $LOCAL_KAFKA_BROKER")
    println(s"Data Message delay $dataTimeInterval")
    println(s"Model Message delay $modelTimeInterval")

    val kafka = KafkaLocalServer(true)
    kafka.start()
    kafka.createTopic(DATA_TOPIC)
    kafka.createTopic(MODELS_TOPIC)

    println(s"Cluster created")

    publishData()
    publishModels()

    while(true)
      pause(600000)
  }

  /**
    * Publish data.
    */
  def publishData() : Future[Unit] = Future {

    val sender = MessageSender(LOCAL_KAFKA_BROKER)
    val bos = new ByteArrayOutputStream()
    val records = getListOfDataRecords(file)
    var nrec = 0
    while (true) {
      records.foreach(r => {
        bos.reset()
        r.writeTo(bos)
        sender.writeValue(DATA_TOPIC, bos.toByteArray)
        nrec = nrec + 1
        if (nrec % 10 == 0) {
          println(s"printed $nrec records")
        }
        pause(dataTimeInterval)
      })
    }
  }

  /**
    * Publish models.
    */
  def publishModels() : Future[Unit] = Future {

    val sender = MessageSender(LOCAL_KAFKA_BROKER)
    val bos = new ByteArrayOutputStream()
    while (true) {
      // TF model bundled
      val tbRecord = ModelDescriptor(name = "tensorflow saved model",
        description = "generated from TensorFlow saved bundle", modeltype =
          ModelDescriptor.ModelType.TENSORFLOWSAVED, dataType = "wine").
        withLocation(tensorfilesaved)
      bos.reset()
      tbRecord.writeTo(bos)
      sender.writeValue(MODELS_TOPIC, bos.toByteArray)
      println(s"Published Model ${tbRecord.description}")
      pause(modelTimeInterval)
      // TF model
      val tByteArray = Files.readAllBytes(Paths.get(tensorfile))
      val tRecord = ModelDescriptor(name = tensorfile.dropRight(3),
        description = "generated from TensorFlow", modeltype =
          ModelDescriptor.ModelType.TENSORFLOW, dataType = "wine").
        withData(ByteString.copyFrom(tByteArray))
      bos.reset()
      tRecord.writeTo(bos)
      sender.writeValue(MODELS_TOPIC, bos.toByteArray)
      println(s"Published Model ${tRecord.description}")
      pause(modelTimeInterval)
    }
  }

  /**
    * Pause.
    */
  private def pause(timeInterval : Long): Unit = {
    try {
      Thread.sleep(timeInterval)
    } catch {
      case _: Throwable => // Ignore
    }
  }

  /**
    * Pause.
    */
  def getListOfDataRecords(file: String): Seq[WineRecord] = {
    var result = Seq.empty[WineRecord]
    val bufferedSource = Source.fromFile(file)
    for (line <- bufferedSource.getLines) {
      val cols = line.split(";").map(_.trim)
      val record = new WineRecord(
        fixedAcidity = cols(0).toDouble,
        volatileAcidity = cols(1).toDouble,
        citricAcid = cols(2).toDouble,
        residualSugar = cols(3).toDouble,
        chlorides = cols(4).toDouble,
        freeSulfurDioxide = cols(5).toDouble,
        totalSulfurDioxide = cols(6).toDouble,
        density = cols(7).toDouble,
        pH = cols(8).toDouble,
        sulphates = cols(9).toDouble,
        alcohol = cols(10).toDouble,
        dataType = "wine"
      )
      result = record +: result
    }
    bufferedSource.close
    result
  }

  /**
    * Get list of the model files.
    *
    * @return dir directory of files.
    */
  private def getListOfModelFiles(dir: String): Seq[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(f => (f.isFile) && (f.getName.endsWith(".pmml"))).map(_.getName)
    } else {
      Seq.empty[String]
    }
  }
}
