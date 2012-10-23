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

package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

/**
 * A class for statistically unique execution vertex IDs.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class ExecutionVertexID extends AbstractID {

	/**
	 * Default constructor required by kryo.
	 */
	private ExecutionVertexID() {
	}

	/**
	 * Constructs a new execution vertex ID.
	 * 
	 * @param lowerPart
	 *        the lower bytes of the ID
	 * @param upperPart
	 *        the higher bytes of the ID
	 */
	private ExecutionVertexID(final long lowerPart, final long upperPart) {
		super(lowerPart, upperPart);
	}

	/**
	 * Constructs a execution vertex ID from the given management vertex ID.
	 * 
	 * @param id
	 *        the management vertex ID to construct the new ID from
	 */
	private ExecutionVertexID(final ManagementVertexID id) {
		super(id);
	}

	/**
	 * Converts the execution vertex ID into a management vertex ID. The new management vertex ID will be equal to the
	 * execution vertex ID in the sense that the <code>equals</code> method will return <code>true</code> when both IDs
	 * are compared.
	 * 
	 * @return the new management vertex ID
	 */
	public ManagementVertexID toManagementVertexID() {

		return ManagementVertexID.fromOtherID(this);
	}

	/**
	 * Converts the given management vertex ID into an execution vertex ID. The new execution vertex ID will be equal to
	 * the management vertex ID in the sense that the <code>equals</code> method will return <code>true</code> when both
	 * IDs are compared.
	 * 
	 * @return the new execution vertex ID
	 */
	public static ExecutionVertexID fromManagementVertexID(final ManagementVertexID id) {
		return new ExecutionVertexID(id);
	}

	/**
	 * Generates a new statistically unique execution vertex ID.
	 * 
	 * @return a new statistically unique execution vertex ID
	 */
	public static ExecutionVertexID generate() {

		final long lowerPart = AbstractID.generateRandomBytes();
		final long upperPart = AbstractID.generateRandomBytes();

		return new ExecutionVertexID(lowerPart, upperPart);
	}
}
