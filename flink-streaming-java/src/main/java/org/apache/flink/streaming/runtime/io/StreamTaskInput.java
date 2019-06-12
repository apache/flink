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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.NullableAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import java.io.Closeable;

/**
 * Basic interface for inputs of stream operators.
 */
@Internal
public interface StreamTaskInput extends NullableAsyncDataInput<StreamElement>, Closeable {
	int UNSPECIFIED = -1;

	/**
	 * @return channel index of last returned {@link StreamElement}, or {@link #UNSPECIFIED} if
	 * it is unspecified.
	 */
	int getLastChannel();

	/**
	 * Returns the input index of this input.
	 */
	int getInputIndex();
}
