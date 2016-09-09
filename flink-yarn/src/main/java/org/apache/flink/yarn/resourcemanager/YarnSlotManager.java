package org.apache.flink.yarn.resourcemanager;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.ResourceSlot;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.SlotRequest;

import java.util.Map;

public class YarnSlotManager extends SlotManager {

	private final YarnClusterCommunicator yarnClusterCommunicator;

	public YarnSlotManager(ResourceManagerGateway resourceManagerGateway, YarnClusterCommunicator yarnClusterCommunicator) throws Exception {
		super(resourceManagerGateway);
		this.yarnClusterCommunicator = yarnClusterCommunicator;
	}

	@Override
	protected ResourceSlot chooseSlotToUse(SlotRequest request, Map<SlotID, ResourceSlot> freeSlots) {
		double maxMatchDegree = 0;
		ResourceSlot mostMatchResourceSlot = null;
		ResourceProfile requiredResourceProfile = request.getResourceProfile();
		// find the most match free slot for request resource profile
		for(ResourceSlot resourceSlot : freeSlots.values()) {
			double matchDegree = resourceSlot.getResourceProfile().calMatchDegree(requiredResourceProfile);
			if(matchDegree > maxMatchDegree) {
				maxMatchDegree = matchDegree;
				mostMatchResourceSlot = resourceSlot;
			}
		}
		return mostMatchResourceSlot;
	}

	@Override
	protected SlotRequest chooseRequestToFulfill(ResourceSlot offeredSlot,
		Map<AllocationID, SlotRequest> pendingRequests) {
		double maxMatchDegree = 0;
		SlotRequest mostMatchRequest = null;
		ResourceProfile offeredResourceProfile = offeredSlot.getResourceProfile();
		// find the most match request for offered resource profile
		for(SlotRequest slotRequest : pendingRequests.values()) {
			double matchDegree = offeredResourceProfile.calMatchDegree(slotRequest.getResourceProfile());
			if(matchDegree > maxMatchDegree) {
				maxMatchDegree = matchDegree;
				mostMatchRequest = slotRequest;
			}
		}
		return mostMatchRequest;
	}

	@Override
	protected void allocateContainer(ResourceProfile resourceProfile) {
		yarnClusterCommunicator.requestNewContainer(resourceProfile);
	}
}
