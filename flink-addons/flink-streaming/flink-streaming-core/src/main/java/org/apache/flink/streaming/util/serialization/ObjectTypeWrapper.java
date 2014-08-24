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

import org.apache.flink.api.java.typeutils.TypeExtractor;

public class ObjectTypeWrapper<IN1, IN2, OUT> extends
		TypeSerializerWrapper<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	private IN1 inInstance1;
	private IN2 inInstance2;
	private OUT outInstance;

	public ObjectTypeWrapper(IN1 inInstance1, IN2 inInstance2, OUT outInstance) {
		this.inInstance1 = inInstance1;
		this.inInstance2 = inInstance2;
		this.outInstance = outInstance;
		setTypeInfo();
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		in.defaultReadObject();
		setTypeInfo();
	}

	@Override
	protected void setTypeInfo() {
		if (inInstance1 != null) {
			inTypeInfo1 = TypeExtractor.getForObject(inInstance1);
		}
		if (inInstance2 != null) {
			inTypeInfo2 = TypeExtractor.getForObject(inInstance2);
		}
		if (outInstance != null) {
			outTypeInfo = TypeExtractor.getForObject(outInstance);
		}
	}
}