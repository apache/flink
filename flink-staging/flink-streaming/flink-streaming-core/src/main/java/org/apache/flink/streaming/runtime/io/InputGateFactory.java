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

import java.util.Collection;

import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;

public class InputGateFactory {

	public static InputGate createInputGate(Collection<InputGate> inputGates) {
		return createInputGate(inputGates.toArray(new InputGate[inputGates.size()]));
	}

	public static InputGate createInputGate(InputGate[] inputGates) {
		if (inputGates.length <= 0) {
			throw new RuntimeException("No such input gate.");
		}

		if (inputGates.length < 2) {
			return inputGates[0];
		} else {
			return new UnionInputGate(inputGates);
		}
	}
}
