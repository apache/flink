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

import org.apache.flink.streaming.api.function.SourceFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WindowSumSource extends SourceFunction<Tuple2<Integer, Long>> {
	private static final long serialVersionUID = 1L;
	
	private Tuple2<Integer, Long> outRecord = new Tuple2<Integer, Long>();
	private Long timestamp = 0L;

	@Override
	public void invoke(Collector<Tuple2<Integer, Long>> collector) throws Exception {
		for (int i = 0; i < 1000; ++i) {
			outRecord.f0 = i;
			outRecord.f1 = timestamp;
			collector.collect(outRecord);
			timestamp++;
		}		
	}
}
