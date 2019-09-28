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

package org.apache.flink.streaming.connectors.kinesis.testutils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.testutils.OneShotLatch;

import java.io.IOException;

/**
 * A DeserializationSchema which always throws an exception when the deserialize method is called. Also supports
 * waiting on a latch until at least one exception has been thrown.
 */
public class AlwaysThrowsDeserializationSchema implements DeserializationSchema<String> {
	public static final String EXCEPTION_MESSAGE = "This method always throws an exception.";

	public transient OneShotLatch isExceptionThrown = new OneShotLatch();

	@Override
	public String deserialize(final byte[] bytes) throws IOException {
		isExceptionThrown.trigger();
		throw new RuntimeException(EXCEPTION_MESSAGE);
	}

	@Override
	public boolean isEndOfStream(final String s) {
		return false;
	}

	@Override
	public TypeInformation<String> getProducedType() {
		return BasicTypeInfo.STRING_TYPE_INFO;
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		this.isExceptionThrown = new OneShotLatch();
	}
}
