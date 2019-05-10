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

/**
 * Listener interface implemented by consumers of {@link InputGate} instances
 * that want to be notified of availability of buffer or event instances.
 */
public interface InputGateListener {

	/**
	 * Notification callback if the input gate moves from zero to non-zero
	 * available input channels with data.
	 *
	 * @param inputGate Input Gate that became available.
	 */
	void notifyInputGateNonEmpty(InputGate inputGate);

}
