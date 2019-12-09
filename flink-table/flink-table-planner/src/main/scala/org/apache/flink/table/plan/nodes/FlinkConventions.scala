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

package org.apache.flink.table.plan.nodes

import org.apache.calcite.plan.Convention
import org.apache.flink.table.plan.nodes.dataset.DataSetRel
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalRel

object FlinkConventions {

  val LOGICAL: Convention = new Convention.Impl("LOGICAL", classOf[FlinkLogicalRel])

  val DATASET: Convention = new Convention.Impl("DATASET", classOf[DataSetRel])

  val DATASTREAM: Convention = new Convention.Impl("DATASTREAM", classOf[DataStreamRel])
}
