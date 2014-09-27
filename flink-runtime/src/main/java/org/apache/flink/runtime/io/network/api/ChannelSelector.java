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


package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.io.IOReadableWritable;

/**
 * Objects implementing this interface are passed to an {@link org.apache.flink.runtime.io.network.gates.OutputGate}. When a record is sent through the output
 * gate, the channel selector object is called to determine to which {@link org.apache.flink.runtime.io.network.channels.OutputChannel} objects the record
 * shall be passed on.
 * 
 * @param <T>
 *        the type of record which is sent through the attached output gate
 */
public interface ChannelSelector<T extends IOReadableWritable> {

	/**
	 * Called to determine to which attached {@link org.apache.flink.runtime.io.network.channels.OutputChannel} objects the given record shall be forwarded.
	 * 
	 * @param record
	 *        the record to the determine the output channels for
	 * @param numberOfOutputChannels
	 *        the total number of output channels which are attached to respective output gate
	 * @return a (possibly empty) array of integer numbers which indicate the indices of the output channels through
	 *         which the record shall be forwarded
	 */
	int[] selectChannels(T record, int numberOfOutputChannels);
}
