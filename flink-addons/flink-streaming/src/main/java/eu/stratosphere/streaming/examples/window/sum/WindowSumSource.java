/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.examples.window.sum;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.streaming.api.SourceFunction;
import eu.stratosphere.util.Collector;

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
