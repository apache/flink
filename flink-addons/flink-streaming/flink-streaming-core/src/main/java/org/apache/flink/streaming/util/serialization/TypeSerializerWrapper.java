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

import org.apache.flink.types.TypeInformation;

public abstract class TypeSerializerWrapper<IN1, IN2, OUT>
		implements Serializable {
	private static final long serialVersionUID = 1L;

	protected transient TypeInformation<IN1> inTypeInfo1 = null;
	protected transient TypeInformation<IN2> inTypeInfo2 = null;
	protected transient TypeInformation<OUT> outTypeInfo = null;

	public TypeInformation<IN1> getInputTypeInfo1() {
		if (inTypeInfo1 == null) {
			throw new RuntimeException("There is no TypeInfo for the first input");
		}
		return inTypeInfo1;
	}

	public TypeInformation<IN2> getInputTypeInfo2() {
		if (inTypeInfo2 == null) {
			throw new RuntimeException("There is no TypeInfo for the second input");
		}
		return inTypeInfo2;
	}

	public TypeInformation<OUT> getOutputTypeInfo() {
		if (outTypeInfo == null) {
			throw new RuntimeException("There is no TypeInfo for the output");
		}
		return outTypeInfo;
	}

	protected abstract void setTypeInfo();
}