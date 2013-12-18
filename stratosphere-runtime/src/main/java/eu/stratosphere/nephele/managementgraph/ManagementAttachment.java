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

package eu.stratosphere.nephele.managementgraph;

/**
 * This class allows to attach an arbitrary {@link Object} to a part of a {@link ManagementGraph}. However, the
 * attachment is not included in a possible serialization/deserialization of the graph or its components.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public abstract class ManagementAttachment {

	/**
	 * The attachment to this part of the {@link ManagementGraph}.
	 */
	private Object attachment;

	/**
	 * Sets an attachment for this part of the {@link ManagementGraph}.
	 * 
	 * @param attachment
	 *        the attachment for this part of the {@link ManagementGraph}
	 */
	public final void setAttachment(final Object attachment) {
		this.attachment = attachment;
	}

	/**
	 * Returns the attachment for this part of the {@link ManagementGraph}.
	 * 
	 * @return the attachment for this part of the {@link ManagementGraph} or <code>null</code> if no attachment is set
	 */
	public final Object getAttachment() {
		return this.attachment;
	}
}
