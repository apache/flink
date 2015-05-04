/*
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
 */

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.NumberSequenceIterator;

/**
 * Source Function used to generate the number sequence
 * 
 */
public class GenSequenceFunction extends RichParallelSourceFunction<Long> {

	private static final long serialVersionUID = 1L;

	private NumberSequenceIterator fullIterator;
	private NumberSequenceIterator splitIterator;

	public GenSequenceFunction(long from, long to) {
		fullIterator = new NumberSequenceIterator(from, to);
	}

	@Override
	public void open(Configuration config) {
		int splitNumber = getRuntimeContext().getIndexOfThisSubtask();
		int numOfSubTasks = getRuntimeContext().getNumberOfParallelSubtasks();
		splitIterator = fullIterator.split(numOfSubTasks)[splitNumber];
	}

	@Override
	public boolean reachedEnd() throws Exception {
		return !splitIterator.hasNext();
	}

	@Override
	public Long next() throws Exception {
		return splitIterator.next();
	}

}
