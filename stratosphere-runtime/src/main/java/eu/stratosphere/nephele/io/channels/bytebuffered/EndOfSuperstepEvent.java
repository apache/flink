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

package eu.stratosphere.nephele.io.channels.bytebuffered;

import eu.stratosphere.nephele.event.task.AbstractEvent;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Marks the end of a superstep of one particular iteration head
 */
public class EndOfSuperstepEvent extends AbstractEvent {
	
	public static final EndOfSuperstepEvent INSTANCE = new EndOfSuperstepEvent();

	@Override
	public void write(DataOutput out) throws IOException {}

	@Override
	public void read(DataInput in) throws IOException {}
}
