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

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.util.serialization.TypeWrapper;

public class ProjectInvokable<IN, OUT extends Tuple> extends StreamInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;

	transient OUT outTuple;
	TypeWrapper<OUT> outTypeWrapper;
	int[] fields;
	int numFields;

	public ProjectInvokable(int[] fields, TypeWrapper<OUT> outTypeWrapper) {
		super(null);
		this.fields = fields;
		this.numFields = this.fields.length;
		this.outTypeWrapper = outTypeWrapper;
	}

	@Override
	protected void immutableInvoke() throws Exception {
		mutableInvoke();
	}

	@Override
	protected void mutableInvoke() throws Exception {
		while ((reuse = recordIterator.next(reuse)) != null) {
			callUserFunctionAndLogException();
		}
	}

	@Override
	protected void callUserFunction() throws Exception {
		for (int i = 0; i < this.numFields; i++) {
			outTuple.setField(reuse.getField(fields[i]), i);
		}
		collector.collect(outTuple);
	}

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		outTuple = outTypeWrapper.getTypeInfo().createSerializer().createInstance();
	}
}
