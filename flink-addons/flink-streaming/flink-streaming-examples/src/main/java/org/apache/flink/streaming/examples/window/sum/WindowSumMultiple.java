/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package org.apache.flink.streaming.examples.window.sum;

import org.apache.flink.api.java.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class WindowSumMultiple extends MapFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>> {
	private static final long serialVersionUID = 1L;
	
	private Tuple2<Integer, Long> outTuple = new Tuple2<Integer, Long>();

	@Override
	public Tuple2<Integer, Long> map(Tuple2<Integer, Long> inTuple) throws Exception {
		outTuple.f0 = inTuple.f0 * 2;
		outTuple.f1 = inTuple.f1;
		return outTuple;
	}
}