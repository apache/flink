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

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.test.TestSinkInvokable;
import eu.stratosphere.types.Record;

public class StreamSink extends AbstractOutputTask{
	
	//TODO: Refactor names
	private RecordReader<Record> input = null;
	private Class<? extends UserSinkInvokable> UserFunction;
	private UserSinkInvokable userFunction;
	
	public StreamSink(){
		//TODO: Make configuration file visible and call setClassInputs() here
		UserFunction = null;
		userFunction = null;
	}
	
	//TODO:Refactor key names,
	//TODO:Add output/input number to config and store class instances in list
	//TODO:Change default classes when done with JobGraphBuilder
	//TODO:Change partitioning from component level to connection level -> output_1_partitioner, output_2_partitioner etc.
	private void setClassInputs() {
		UserFunction = getTaskConfiguration().getClass("userfunction",
						TestSinkInvokable.class, UserSinkInvokable.class);
		}
	
	@Override
	public void registerInputOutput() {
		setClassInputs();
		this.input = new RecordReader<Record>(this, Record.class);
	}

	@Override
	public void invoke() throws Exception {
		try {
		    userFunction = UserFunction.newInstance();
		} catch (Exception e) {

		}
		
		boolean hasInput = true;
		while (hasInput){
		hasInput = false;
			if (input.hasNext()){
				hasInput = true;
				userFunction.invoke(
						input.next(), 
						input);
			}
		}
	}

}
