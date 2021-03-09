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

package org.apache.flink.runtime.io.network.partition.consumer;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A test input gate to mock reading data. */
public class TestSingleInputGate {

    protected final SingleInputGate inputGate;

    protected final TestInputChannel[] inputChannels;

    public TestSingleInputGate(int numberOfInputChannels, int gateIndex, boolean initialize) {
        checkArgument(numberOfInputChannels >= 1);

        inputGate =
                new SingleInputGateBuilder()
                        .setNumberOfChannels(numberOfInputChannels)
                        .setSingleInputGateIndex(gateIndex)
                        .build();
        inputChannels = new TestInputChannel[numberOfInputChannels];

        if (initialize) {
            for (int i = 0; i < numberOfInputChannels; i++) {
                inputChannels[i] = new TestInputChannel(inputGate, i);
            }
            inputGate.setInputChannels(inputChannels);
        }
    }

    public SingleInputGate getInputGate() {
        return inputGate;
    }
}
