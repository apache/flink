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

package org.apache.flink.modelserving.scala.query

import java.util.function.BiFunction

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.{ExecutionConfig, JobID}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.modelserving.scala.model.ModelToServeStats
import org.apache.flink.queryablestate.client.QueryableStateClient
import org.apache.flink.queryablestate.client.state.ImmutableValueState
import org.joda.time.DateTime

/**
  * ModelStateQuery - query model state (works only for keyed implementation).
  */
object ModelStateQuery {

  // Default Timeout between queries
  val defaulttimeInterval = 1000 * 20        // 20 sec

  /**
    * Main method. When using make sure that you set job ID correctly
    *
    */
  def query(job: String, keys: Seq[String], host: String = "127.0.0.1", port: Int = 9069,
            timeInterval: Long = defaulttimeInterval): Unit = {

    // JobID, has to correspond to a running job
    val jobId = JobID.fromHexString(job)
    // Client
    val client = new QueryableStateClient(host, port)

    // the state descriptor of the state to be fetched.
    val descriptor = new ValueStateDescriptor[ModelToServeStats](
      "currentModel",   // state name
      createTypeInformation[ModelToServeStats].createSerializer(new ExecutionConfig)
    )
    // Key type
    val keyType = BasicTypeInfo.STRING_TYPE_INFO

    println("                   Name                      |       Description       |" +
      "       Since       |       Average       |       Min       |       Max       |")
    while(true) {
      for (key <- keys) {
        // For every key
        try {
          // Get statistics
          val future = client.getKvState(jobId, "currentModelState", key, keyType, descriptor)
          val stats = future.join().value()
          println(s" ${stats.name} | ${stats.description} | ${new DateTime(stats.since).
            toString("yyyy/MM/dd HH:MM:SS")} | ${stats.duration/stats.usage} |" +
            s"  ${stats.min} | ${stats.max} |")
        }
        catch {case e: Exception => e.printStackTrace()}
      }
      // Wait for next
      Thread.sleep(timeInterval)
    }
  }
}
