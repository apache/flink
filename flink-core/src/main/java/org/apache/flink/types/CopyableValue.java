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

package org.apache.flink.types;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Interface to be implemented by basic types that support to be copied efficiently.
 */
public interface CopyableValue<T> extends Value {
	
	/**
	 * Gets the length of the data type when it is serialized, in bytes.
	 * 
	 * @return The length of the data type, or {@code -1}, if variable length.
	 */
	int getBinaryLength();
	
	void copyTo(T target);
	
	void copy(DataInputView source, DataOutputView target) throws IOException;
}
