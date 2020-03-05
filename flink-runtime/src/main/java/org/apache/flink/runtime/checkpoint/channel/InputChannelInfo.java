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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * Identifies {@link org.apache.flink.runtime.io.network.partition.consumer.InputChannel} in a given subtask.
 * Note that {@link org.apache.flink.runtime.io.network.partition.consumer.InputChannelID InputChannelID}
 * can not be used because it is generated randomly.
 */
@Internal
public class InputChannelInfo implements Serializable {
	private static final long serialVersionUID = 1L;

	private final int gateIdx;
	private final int inputChannelIdx;

	public InputChannelInfo(int gateIdx, int inputChannelIdx) {
		this.gateIdx = gateIdx;
		this.inputChannelIdx = inputChannelIdx;
	}

	public int getGateIdx() {
		return gateIdx;
	}

	public int getInputChannelIdx() {
		return inputChannelIdx;
	}
}
