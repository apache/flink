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

package org.apache.flink.examples.java.misc.testing;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class CollectTesting implements java.io.Serializable {

	public static void main(String[] args) throws Exception {

		final long numSamples = args.length > 0 ? Long.parseLong(args[0]) : 1000000;

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		System.out.println("Creating a dataset of "+ numSamples +" records, or "+
				(numSamples * Long.SIZE / 8)+" Bytes for testing.");

		DataSet<Long> bigEnough = env.generateSequence(1, numSamples);
		long theCount = bigEnough.collect().size();

		System.out.println("The output has " + theCount +" records.");
	}
}
