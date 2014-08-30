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

package flinkgraph.api.io;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import flinkgraph.api.Edge;
import flinkgraph.internal.utils.Tuple2ToEdgeMapper;

public class EdgesCsvBuilder<T extends Comparable<T>> implements EdgeBuilder<T> {
	
	private final String path;
	
	private int sourcePos = 0;
	
	private int targetPos = 1;

	
	public EdgesCsvBuilder(String path) {
		this.path = path;
	}
	
	
	public EdgesCsvBuilder<T> sourceColumn(int column) {
		this.sourcePos = column;
		return this;
	}
	
	public EdgesCsvBuilder<T> targetColumn(int column) {
		this.targetPos = column;
		return this;
	}
	
	@Override
	public DataSet<? extends Edge<T>> createEdges(ExecutionEnvironment env, Class<T> idType) {
		long mask = 0;
		mask |= 0x1 << sourcePos;
		mask |= 0x1 << targetPos;
		
		DataSet<Tuple2<T, T>> tuples = env.readCsvFile(path).includeFields(mask).types(idType, idType);
		return tuples.map(new Tuple2ToEdgeMapper<T>());
	}
}
