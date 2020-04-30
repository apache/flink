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


package org.apache.flink.api.common.operators.util;

import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

/**
 * Same as the non rich test output format, except it provide access to runtime context.
 */
public class TestRichOutputFormat extends RichOutputFormat<String> {
	public List<String> output = new LinkedList<String>();

	@Override
	public void configure(Configuration parameters){}

	@Override
	public void open(int a, int b){}

	@Override
	public void close(){}

	@Override
	public void writeRecord(String record){
		output.add(record + getRuntimeContext().getIndexOfThisSubtask() + "" +
				getRuntimeContext().getNumberOfParallelSubtasks());
	}

	public void clear(){
		output.clear();
	}
}
