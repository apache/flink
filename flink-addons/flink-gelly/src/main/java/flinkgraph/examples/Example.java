/**
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

package flinkgraph.examples;

import org.apache.flink.api.java.ExecutionEnvironment;

import flinkgraph.api.Graph;
import flinkgraph.api.io.EdgesCsvBuilder;

public class Example {

	
	public static void main(String[] args) throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		
		Graph<String> graph = Graph.fromEdges(env, String.class,
								new EdgesCsvBuilder<String>("/the/edges/file")
								.sourceColumn(2).targetColumn(5));
				
	}
}
