/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.arraymodel;

import java.util.Collection;

import eu.stratosphere.api.operators.GenericDataSink;


public class ArrayModelJob extends eu.stratosphere.api.Job {

	public ArrayModelJob(Collection<GenericDataSink> sinks, String jobName) {
		super(sinks, jobName);
	}

	public ArrayModelJob(Collection<GenericDataSink> sinks) {
		super(sinks);
	}

	public ArrayModelJob(GenericDataSink sink, String jobName) {
		super(sink, jobName);
	}

	public ArrayModelJob(GenericDataSink sink) {
		super(sink);
	}

	@Override
	public String getPostPassClassName() {
		return "eu.stratosphere.arraymodel.optimizer.ArrayRecordOptimizerPostPass";
	}
}
