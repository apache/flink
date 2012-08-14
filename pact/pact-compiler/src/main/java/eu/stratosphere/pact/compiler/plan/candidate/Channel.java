/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.compiler.plan.candidate;


/**
 *
 *
 * @author Stephan Ewen
 */
public class Channel
{
	/**
	 * Enumeration to indicate the mode of temporarily materializing the data that flows across a connection.
	 * Introducing such an artificial dam is sometimes necessary to avoid that a certain data flows deadlock
	 * themselves.
	 */
	public enum TempMode {
		NONE, TEMP_SENDER_SIDE, TEMP_RECEIVER_SIDE
	}
	
	// --------------------------------------------------------------------------------------------
	
	private final PlanNode source = null;
	
	private final PlanNode target = null;
	
	private TempMode tempMode;
	
	private int replicationFactor;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the source of this Channel.
	 *
	 * @return The source.
	 */
	public PlanNode getSource() {
		return source;
	}
	
	/**
	 * Gets the target of this Channel.
	 *
	 * @return The target.
	 */
	public PlanNode getTarget() {
		return target;
	}
	
	/**
	 * Returns the TempMode of the Connection. NONE if the connection is not temped,
	 * TEMP_SENDER_SIDE if the connection is temped on the sender node, and
	 * TEMP_RECEIVER_SIDE if the connection is temped on the receiver node.
	 * 
	 * @return TempMode of the connection
	 */
	public TempMode getTempMode() {
		return this.tempMode;
	}

	/**
	 * Sets the temp mode of the connection.
	 * 
	 * @param tempMode
	 *        The temp mode of the connection.
	 */
	public void setTempMode(TempMode tempMode) {
		this.tempMode = tempMode;
	}
	
	/**
	 * Returns the replication factor of the connection.
	 * 
	 * @return The replication factor of the connection.
	 */
	public int getReplicationFactor() {
		return this.replicationFactor;
	}
}
