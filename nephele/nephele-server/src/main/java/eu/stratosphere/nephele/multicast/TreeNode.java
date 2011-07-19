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

public class TreeNode {

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
	
}
