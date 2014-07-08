/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.network.bufferprovider;

/**
 * This interface must be implemented to receive a notification from a {@link BufferProvider} when an empty
 * {@link eu.stratosphere.runtime.io.Buffer} has
 * become available again.
 * 
 */
public interface BufferAvailabilityListener {

	/**
	 * Indicates that at least one {@link eu.stratosphere.runtime.io.Buffer} has become available again.
	 */
	void bufferAvailable();
}
