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
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utility for dealing with input gates. This will either just return
 * the single {@link InputGate} that was passed in or create a {@link UnionInputGate} if several
 * {@link InputGate input gates} are given.
 */
@Internal
public class InputGateUtil {

	public static InputGate createInputGate(Collection<InputGate>... inputGateGroups) {
		List<InputGate> gates = new ArrayList<InputGate>();
		for (Collection<InputGate> inputGates : inputGateGroups) {
			gates.addAll(inputGates);
		}
		return createInputGate(gates.toArray(new InputGate[0]));
	}

	public static InputGate createInputGate(InputGate[]... inputGateGroups) {
		if (inputGateGroups.length <= 0) {
			throw new RuntimeException("No such input gate.");
		}

		if (inputGateGroups.length == 1) {
			if (inputGateGroups[0].length == 1) {
				return inputGateGroups[0][0];
			} else {
				if (inputGateGroups[0].length <= 0) {
					throw new RuntimeException("No such input gate.");
				}
				return new UnionInputGate(inputGateGroups[0]);
			}
		} else {
			int numGates = 0;
			for (InputGate[] inputGates : inputGateGroups) {
				numGates += inputGates.length;
			}
			if (numGates <= 0) {
				throw new RuntimeException("No such input gate.");
			}

			InputGate[] gates = new InputGate[numGates];
			int index = 0;
			for (InputGate[] inputGates : inputGateGroups) {
				for (InputGate gate : inputGates) {
					gates[index++] = gate;
				}
			}

			if (gates.length == 1) {
				return gates[0];
			} else {
				return new UnionInputGate(gates);
			}
		}
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private InputGateUtil() {
		throw new RuntimeException();
	}
}
