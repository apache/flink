/**
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
 */

package org.apache.flink.streaming.util.serialization;

import java.io.IOException;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class ObjectTypeWrapper<IN1 extends Tuple, IN2 extends Tuple, OUT extends Tuple> extends
		TypeSerializerWrapper<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	private Object inInstance1;
	private Object inInstance2;
	private Object outInstance;

	public ObjectTypeWrapper(Object inInstance1, Object inInstance2, Object outInstance) {
		this.inInstance1 = inInstance1;
		this.inInstance2 = inInstance2;
		this.outInstance = outInstance;
		setTupleTypeInfo();
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		in.defaultReadObject();
		setTupleTypeInfo();
	}

	@Override
	protected void setTupleTypeInfo() {
		if (inInstance1 != null) {
			inTupleTypeInfo1 = new TupleTypeInfo<IN1>(TypeExtractor.getForObject(inInstance1));
		}
		if (inInstance2 != null) {
			inTupleTypeInfo2 = new TupleTypeInfo<IN2>(TypeExtractor.getForObject(inInstance2));
		}
		if (outInstance != null) {
			outTupleTypeInfo = new TupleTypeInfo<OUT>(TypeExtractor.getForObject(outInstance));
		}
	}
}