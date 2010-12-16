/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.managementgraph;

import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;

public class ManagementGroupEdge {

	private final ManagementGroupVertex source;

	private final ManagementGroupVertex target;

	private final int sourceIndex;

	private final int targetIndex;

	private final ChannelType channelType;

	private final CompressionLevel compressionLevel;

	private Object attachment = null;

	public ManagementGroupEdge(ManagementGroupVertex source, int sourceIndex, ManagementGroupVertex target,
			int targetIndex, ChannelType channelType, CompressionLevel compressionLevel) {
		this.source = source;
		this.target = target;
		this.sourceIndex = sourceIndex;
		this.targetIndex = targetIndex;
		this.channelType = channelType;
		this.compressionLevel = compressionLevel;

		source.insertForwardEdge(this, sourceIndex);
		target.insertBackwardEdge(this, targetIndex);
	}

	public ChannelType getChannelType() {
		return this.channelType;
	}

	public CompressionLevel getCompressionLevel() {
		return this.compressionLevel;
	}

	public ManagementGroupVertex getSource() {
		return this.source;
	}

	public ManagementGroupVertex getTarget() {
		return this.target;
	}

	public int getSourceIndex() {
		return this.sourceIndex;
	}

	public int getTargetIndex() {
		return this.targetIndex;
	}

	public void setAttachment(Object attachment) {
		this.attachment = attachment;
	}

	public Object getAttachment() {
		return this.attachment;
	}
}
