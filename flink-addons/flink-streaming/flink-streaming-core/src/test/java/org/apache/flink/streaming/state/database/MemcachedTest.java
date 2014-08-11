/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.state.database;

import org.junit.Test;

public class MemcachedTest {
	
	@Test
	public void databaseState(){
		MemcachedState state=new MemcachedState();
		state.setTuple("hello", "world");
		state.setTuple("big", "data");
		state.setTuple("flink", "streaming");
		System.out.println(state.getTuple("hello"));
		System.out.println(state.getTuple("big"));
		System.out.println(state.getTuple("flink"));
		state.close();
	}
}
