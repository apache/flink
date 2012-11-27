/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.checkpointing;

import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.taskmanager.routing.GateContext;

abstract class AbstractReplayGateContext implements GateContext {

	/**
	 * The ID of the gate this context has been created for.
	 */
	private final GateID gateID;

	/**
	 * Constructs a new replay gate context.
	 * 
	 * @param gateID
	 *        the ID of the gate this context has been created for
	 */
	AbstractReplayGateContext(final GateID gateID) {
		this.gateID = gateID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GateID getGateID() {

		return this.gateID;
	}

}
