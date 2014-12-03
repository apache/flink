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

import java.io.Serializable;

import org.apache.flink.api.java.tuple.Tuple3;

public class Edge<K extends Comparable<K> & Serializable, V extends Serializable> 
	extends Tuple3<K, K, V>{

	private static final long serialVersionUID = 1L;

	public Edge(){}

	public Edge(K src, K trg) {
		this.f0 = src;
		this.f1 = trg;
	}

	public Edge(K src, K trg, V val) {
		this.f0 = src;
		this.f1 = trg;
		this.f2 = val;
	}

	public Edge<K, V> reverse() {
			return new Edge<K, V>(this.f1, this.f0, this.f2);
	}

	public void setSource(K src) {
		this.f0 = src;
	}

	public K getSource() {
		return this.f0;
	}

	public void setTarget(K target) {
		this.f1 = target;
	}

	public K getTarget() {
		return f1;
	}

	public void setValue(V value) {
		this.f2 = value;
	}

	public V getValue() {
		return f2;
	}
}
