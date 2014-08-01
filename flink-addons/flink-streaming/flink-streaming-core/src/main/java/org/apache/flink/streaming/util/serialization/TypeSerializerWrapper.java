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

import java.io.Serializable;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public abstract class TypeSerializerWrapper<IN1 extends Tuple, IN2 extends Tuple, OUT extends Tuple>
		implements Serializable {
	private static final long serialVersionUID = 1L;

	protected transient TupleTypeInfo<IN1> inTupleTypeInfo1 = null;
	protected transient TupleTypeInfo<IN2> inTupleTypeInfo2 = null;
	protected transient TupleTypeInfo<OUT> outTupleTypeInfo = null;

	public TupleTypeInfo<IN1> getInputTupleTypeInfo1() {
		if (inTupleTypeInfo1 == null) {
			throw new RuntimeException("There is no TypeInfo for the first input");
		}
		return inTupleTypeInfo1;
	}

	public TupleTypeInfo<IN2> getInputTupleTypeInfo2() {
		if (inTupleTypeInfo1 == null) {
			throw new RuntimeException("There is no TypeInfo for the first input");
		}
		return inTupleTypeInfo2;
	}

	public TupleTypeInfo<OUT> getOutputTupleTypeInfo() {
		if (inTupleTypeInfo1 == null) {
			throw new RuntimeException("There is no TypeInfo for the first input");
		}
		return outTupleTypeInfo;
	}

	protected abstract void setTupleTypeInfo();
}