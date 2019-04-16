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

package org.apache.flink.examples.modelserving.scala.query

import org.apache.flink.modelserving.scala.query.ModelStateQuery

/**
  * ModelStateQueryJob - query model state (works only for keyed implementation).
  */
object ModelStateQueryJob {

  /**
    *  Main mmethod.
    *  Make sure you use the right jobID here
    *
    */
  def main(args: Array[String]): Unit = {
    ModelStateQuery.query("cd1c745f58691c424fef5fd509b98594", Seq("wine"))
  }
}
