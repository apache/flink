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

package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

/**
 * A class for statistically unique execution vertex IDs.
 * 
 * @author warneke
 */
public class ExecutionVertexID extends AbstractID {

	/**
	 * Converts the execution vertex ID into a
	 * management vertex ID. The new management vertex ID
	 * will be equal to the execution vertex ID in the sense
	 * that the <code>equals</code> method will return <code>
	 * true</code> when both IDs are compared.
	 * 
	 * @return the new management vertex ID
	 */
	public ManagementVertexID toManagementVertexID() {

		final ManagementVertexID newID = new ManagementVertexID();
		newID.setID(this);

		return newID;
	}

	/**
	 * Converts the given management vertex ID into the corresponding execution vertex ID. The new execution vertex ID
	 * will be equals to the management vertex ID in the sense that the <code>equals</code> method will return
	 * <code>true</code> when both IDs are compared.
	 * 
	 * @param vertexID
	 *        the management vertex ID to be converted
	 * @return the resulting execution vertex ID
	 */
	public static ExecutionVertexID fromManagementVertexID(final ManagementVertexID vertexID) {

		final ExecutionVertexID newID = new ExecutionVertexID();
		newID.setID(vertexID);

		return newID;
	}
}
