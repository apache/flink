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

package org.apache.flink.graph.examples;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.examples.data.AffinityPropagationData;
import org.apache.flink.graph.library.AffinityPropagation;
import org.apache.flink.types.NullValue;

public class AffinityPropagationClustering implements ProgramDescription {

	// --------------------------------------------------------------------------------------------
	//  Program
	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//AffinityPropagation ap = new AffinityPropagation(1000, 0.1f, 0.000001f);
		AffinityPropagation ap = new AffinityPropagation(1000, 10);

		Graph<Long, AffinityPropagation.APVertexValue, NullValue> apGraph =
			ap.createAPGraph(AffinityPropagationData.getArray(),env);

		DataSet<Tuple2<Long, Long>> result =  apGraph.run(ap);

		result.print();

	}

	@Override
	public String getDescription() {
		return "Affinity propagation";
	}

}
