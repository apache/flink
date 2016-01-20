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

package org.apache.flink.api.java.table.test;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

public class ExecuteProcessTest {

	@Test
	public void testProcess1() throws Exception{
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		DataSet<Tuple1<Integer>> input1 = env.fromElements(new Tuple1<Integer>(1),
				new Tuple1<Integer>(2), new Tuple1<Integer>(3)).partitionByHash(0);
		DataSet<Tuple1<Integer>> input2 = env.fromElements(new Tuple1<Integer>(1),
				new Tuple1<Integer>(2), new Tuple1<Integer>(4)).partitionByRange(0);
		
		DataSet<Tuple2<Tuple1<Integer>, Tuple1<Integer>>> result = input1.join(input2).where(0).equalTo(0);
		
		System.out.println(result.collect());
		
		env.execute();
		
	}
}
