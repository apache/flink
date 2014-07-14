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
package eu.stratosphere.streaming.api;

import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class FilterInvokable<IN extends Tuple> extends UserTaskInvokable<IN, IN>  {
	FilterFunction<IN> filterFunction;
	
	public FilterInvokable(FilterFunction<IN> filterFunction) {
		this.filterFunction = filterFunction;
	}
	
	@Override
	public void invoke(StreamRecord record, StreamCollector<IN> collector) throws Exception {
		for (int i = 0; i < record.getBatchSize(); i++) {
			IN tuple = (IN) record.getTuple(i);
			if (filterFunction.filter(tuple)) {
				collector.collect(tuple);
			}
		}
	}
}
