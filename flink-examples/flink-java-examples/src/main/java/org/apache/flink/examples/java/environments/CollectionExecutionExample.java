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

package org.apache.flink.examples.java.environments;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.CollectionEnvironment;

/**
 * This example shows how to use Flink's collection execution functionality.
 * Collection-based execution is an extremely lightweight, non-parallel way to
 * execute programs on small data: The programs are s
 * 
 * Because this method of execution spawns no background threads, managed memory, 
 * coordinator, or parallel worker, it has a minimal execution footprint. 
 */
public class CollectionExecutionExample {

	public static void main(String[] args) throws Exception {
		
		CollectionEnvironment env = new CollectionEnvironment();
		
		env.fromElements("A", "B", "C", "D")
			.map(new MapFunction<String, String>() {
				public String map(String value) {
					return value + " " + 1;
				};
			})
		.print();
		
		env.execute();
	}
}
