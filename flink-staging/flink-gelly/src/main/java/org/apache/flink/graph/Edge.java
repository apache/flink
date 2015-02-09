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

package flink.graphs;

public class Edge<K, V> {

	private K source;
	
	private K target;
	
	private V value;
	
	public Edge(K src, K trg) {
		this.source = src;
		this.target = trg;
	}

	public Edge(K src, K trg, V val) {
		this.source = src;
		this.target = trg;
		this.value = val;
	}

	public Edge<K, V> reverse() {
		if (value != null) {
			return new Edge<K, V>(this.target, this.source, this.value);
		}
		else {
			return new Edge<K, V>(this.target, this.source);
		}
		
	}
}
