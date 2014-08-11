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

public class LeveldbTest {
	@Test
	public void databaseTest(){
		LeveldbState state=new LeveldbState("test");
		state.setTuple("hello", "world");
		System.out.println(state.getTuple("hello"));
		state.setTuple("big", "data");
		state.setTuple("flink", "streaming");
		LeveldbStateIterator iterator=state.getIterator();
		while(iterator.hasNext()){
			String key=iterator.getNextKey();
			String value=iterator.getNextValue();
			System.out.println("key="+key+", value="+value);
			iterator.next();
		}
		state.close();
	}
}
