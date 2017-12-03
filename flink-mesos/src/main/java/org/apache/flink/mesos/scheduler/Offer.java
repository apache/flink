/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.mesos.scheduler;

import com.netflix.fenzo.VirtualMachineLease;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.mesos.Utils.print;

/**
 * An adapter class to transform a Mesos resource offer to a Fenzo {@link VirtualMachineLease}.
 *
 * <p>The default implementation provided by Fenzo isn't compatible with reserved resources.
 * This implementation properly combines resources, e.g. a combination of reserved and unreserved cpus.
 *
 */
public class Offer implements VirtualMachineLease {

	private static final Logger logger = LoggerFactory.getLogger(Offer.class);

	private final Protos.Offer offer;
	private final String hostname;
	private final String vmID;
	private final long offeredTime;

	private final List<Protos.Resource> resources;
	private final Map<String, List<Protos.Resource>> resourceMap;
	private final Map<String, Protos.Attribute> attributeMap;

	public Offer(Protos.Offer offer) {
		this.offer = offer;
		this.hostname = offer.getHostname();
		this.vmID = offer.getSlaveId().getValue();
		this.offeredTime = System.currentTimeMillis();

		this.resources = new ArrayList<>(offer.getResourcesList().size());
		this.resourceMap = new HashMap<>();
		for (Protos.Resource resource : offer.getResourcesList()) {
			switch (resource.getType()) {
				case SCALAR:
				case RANGES:
					resources.add(resource);
					resourceMap.computeIfAbsent(resource.getName(), k -> new ArrayList<>(2)).add(resource);
					break;
				default:
					logger.debug("Unknown resource type " + resource.getType() + " for resource " + resource.getName() +
						" in offer, hostname=" + hostname + ", offerId=" + offer.getId());
			}
		}

		if (offer.getAttributesCount() > 0) {
			Map<String, Protos.Attribute> attributeMap = new HashMap<>();
			for (Protos.Attribute attribute: offer.getAttributesList()) {
				attributeMap.put(attribute.getName(), attribute);
			}
			this.attributeMap = Collections.unmodifiableMap(attributeMap);
		} else {
			this.attributeMap = Collections.emptyMap();
		}
	}

	public List<Protos.Resource> getResources() {
		return Collections.unmodifiableList(resources);
	}

	@Override
	public String hostname() {
		return hostname;
	}

	@Override
	public String getVMID() {
		return vmID;
	}

	@Override
	public double cpuCores() {
		return aggregateScalarResource("cpus");
	}

	@Override
	public double memoryMB() {
		return aggregateScalarResource("mem");
	}

	@Override
	public double networkMbps() {
		return aggregateScalarResource("network");
	}

	@Override
	public double diskMB() {
		return aggregateScalarResource("disk");
	}

	public Protos.Offer getOffer(){
		return offer;
	}

	@Override
	public String getId() {
		return offer.getId().getValue();
	}

	@Override
	public long getOfferedTime() {
		return offeredTime;
	}

	@Override
	public List<Range> portRanges() {
		return aggregateRangesResource("ports");
	}

	@Override
	public Map<String, Protos.Attribute> getAttributeMap() {
		return attributeMap;
	}

	@Override
	public String toString() {
		return "Offer{" +
			"offer=" + offer +
			", resources='" + print(resources) + '\'' +
			", hostname='" + hostname + '\'' +
			", vmID='" + vmID + '\'' +
			", attributeMap=" + attributeMap +
			", offeredTime=" + offeredTime +
			'}';
	}

	private double aggregateScalarResource(String resourceName) {
		if (resourceMap.get(resourceName) == null) {
			return 0.0;
		}
		return resourceMap.get(resourceName).stream().mapToDouble(r -> r.getScalar().getValue()).sum();
	}

	private List<Range> aggregateRangesResource(String resourceName) {
		if (resourceMap.get(resourceName) == null) {
			return Collections.emptyList();
		}
		return resourceMap.get(resourceName).stream()
			.flatMap(r -> r.getRanges().getRangeList().stream())
			.map(r -> new Range((int) r.getBegin(), (int) r.getEnd()))
			.collect(Collectors.toList());
	}
}
