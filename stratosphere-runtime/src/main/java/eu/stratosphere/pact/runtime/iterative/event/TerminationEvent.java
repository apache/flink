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

package eu.stratosphere.pact.runtime.iterative.event;

import java.io.IOException;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;

/**
 * Signals that the iteration is completely executed, participating tasks must terminate now
 */
public class TerminationEvent extends AbstractTaskEvent {

	public static final TerminationEvent INSTANCE = new TerminationEvent();
	
	@Override
	public void write(DataOutputView out) throws IOException {}

	@Override
	public void read(DataInputView in) throws IOException {}
}
