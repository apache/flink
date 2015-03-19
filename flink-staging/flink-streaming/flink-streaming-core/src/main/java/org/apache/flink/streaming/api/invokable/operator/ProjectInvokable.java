/**
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

package org.apache.flink.streaming.api.invokable.operator;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.invokable.ChainableInvokable;

public class ProjectInvokable<IN, OUT extends Tuple> extends ChainableInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;

	transient OUT outTuple;
	TypeSerializer<OUT> outTypeSerializer;
	TypeInformation<OUT> outTypeInformation;
	int[] fields;
	int numFields;

	public ProjectInvokable(int[] fields, TypeInformation<OUT> outTypeInformation) {
		super(null);
		this.fields = fields;
		this.numFields = this.fields.length;
		this.outTypeInformation = outTypeInformation;
	}

	@Override
	public void invoke() throws Exception {
		while (isRunning && readNext() != null) {
			callUserFunctionAndLogException();
		}
	}

	@Override
	protected void callUserFunction() throws Exception {
		for (int i = 0; i < this.numFields; i++) {
			outTuple.setField(((Tuple)nextObject).getField(fields[i]), i);
		}
		collector.collect(outTuple);
	}

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		this.outTypeSerializer = outTypeInformation.createSerializer(executionConfig);
		outTuple = outTypeSerializer.createInstance();
	}

	@Override
	public void collect(IN record) {
		if (isRunning) {
			nextObject = copy(record);
			callUserFunctionAndLogException();
		}
	}
}
