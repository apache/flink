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

package eu.stratosphere.nephele.multicast;

import java.util.LinkedList;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.channels.ChannelID;

/**
 * Each physical Node (instance) within a multicast-tree is represented by a TreeNode object.
 * It contains the connection info for the certain node and a list of the local output channels.
 * 
 * @author casp
 *
 */

public class TreeNode implements Comparable<TreeNode>{

	private final InstanceConnectionInfo nodeConnectionInfo;
	
	private final LinkedList<ChannelID> localTargets;
	
	private final LinkedList<InstanceConnectionInfo> remoteTargets = new LinkedList<InstanceConnectionInfo>();
	
	public TreeNode(InstanceConnectionInfo nodeConnectionInfo, LinkedList<ChannelID> localTargets){
		this.nodeConnectionInfo = nodeConnectionInfo;
		this.localTargets = localTargets;
	}
	
	public TreeNode(InstanceConnectionInfo nodeConnectionInfo){
		this.nodeConnectionInfo = nodeConnectionInfo;
		this.localTargets = new LinkedList<ChannelID>();
	}
	
	public void addLocalTarget(ChannelID channel){
		this.localTargets.add(channel);
	}
	
	public void addRemoteTarget(InstanceConnectionInfo target){
		this.remoteTargets.add(target);
	}
	
	public LinkedList<InstanceConnectionInfo> getRemoteTargets(){
		return this.remoteTargets;
	}
	
	public LinkedList<ChannelID> getLocalTargets(){
		return this.localTargets;
	}
	
	public InstanceConnectionInfo getConnectionInfo(){
		return this.nodeConnectionInfo;
	}

	@Override
	public int compareTo(TreeNode o) {
		return this.nodeConnectionInfo.compareTo(o.nodeConnectionInfo);
	}
	
}
