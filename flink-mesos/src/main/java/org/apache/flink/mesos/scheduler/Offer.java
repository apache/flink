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

import org.apache.flink.mesos.Utils;

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

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An adapter class to transform a Mesos resource offer to a Fenzo {@link VirtualMachineLease}.
 *
 * <p>The default implementation provided by Fenzo isn't compatible with reserved resources. This
 * implementation properly combines resources, e.g. a combination of reserved and unreserved cpus.
 */
public class Offer implements VirtualMachineLease {

    private static final Logger logger = LoggerFactory.getLogger(Offer.class);

    private final Protos.Offer offer;
    private final String hostname;
    private final String vmID;
    private final long offeredTime;

    private final List<Protos.Resource> resources;
    private final Map<String, Double> aggregatedScalarResourceMap;
    private final Map<String, Protos.Attribute> attributeMap;

    private final double cpuCores;
    private final double memoryMB;
    private final double networkMbps;
    private final double diskMB;

    private final List<Range> portRanges;

    public Offer(Protos.Offer offer) {
        this(offer, "network");
    }

    public Offer(Protos.Offer offer, String networkResourceName) {
        this.offer = checkNotNull(offer);
        this.hostname = offer.getHostname();
        this.vmID = offer.getSlaveId().getValue();
        this.offeredTime = System.currentTimeMillis();

        List<Protos.Resource> resources = new ArrayList<>(offer.getResourcesList().size());
        Map<String, Double> aggregatedScalarResourceMap =
                new HashMap<String, Double>() {
                    @Override
                    public Double remove(Object key) {
                        if (super.containsKey(key)) {
                            return super.remove(key);
                        } else {
                            return 0.0;
                        }
                    }
                };
        Map<String, List<Protos.Resource>> rangesResourceMap = new HashMap<>();
        for (Protos.Resource resource : offer.getResourcesList()) {
            switch (resource.getType()) {
                case SCALAR:
                    resources.add(resource);
                    aggregatedScalarResourceMap.merge(
                            resource.getName(), resource.getScalar().getValue(), Double::sum);
                    break;
                case RANGES:
                    resources.add(resource);
                    rangesResourceMap
                            .computeIfAbsent(resource.getName(), k -> new ArrayList<>(2))
                            .add(resource);
                    break;
                default:
                    logger.debug(
                            "Unknown resource type "
                                    + resource.getType()
                                    + " for resource "
                                    + resource.getName()
                                    + " in offer, hostname="
                                    + hostname
                                    + ", offerId="
                                    + offer.getId());
            }
        }
        this.resources = Collections.unmodifiableList(resources);

        this.cpuCores = aggregatedScalarResourceMap.remove("cpus");
        this.memoryMB = aggregatedScalarResourceMap.remove("mem");
        this.networkMbps = aggregatedScalarResourceMap.remove(networkResourceName);
        this.diskMB = aggregatedScalarResourceMap.remove("disk");
        this.aggregatedScalarResourceMap = Collections.unmodifiableMap(aggregatedScalarResourceMap);
        this.portRanges =
                Collections.unmodifiableList(aggregateRangesResource(rangesResourceMap, "ports"));

        if (offer.getAttributesCount() > 0) {
            Map<String, Protos.Attribute> attributeMap = new HashMap<>();
            for (Protos.Attribute attribute : offer.getAttributesList()) {
                attributeMap.put(attribute.getName(), attribute);
            }
            this.attributeMap = Collections.unmodifiableMap(attributeMap);
        } else {
            this.attributeMap = Collections.emptyMap();
        }
    }

    public List<Protos.Resource> getResources() {
        return resources;
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
        return cpuCores;
    }

    public double gpus() {
        return getScalarValue("gpus");
    }

    @Override
    public double memoryMB() {
        return memoryMB;
    }

    @Override
    public double networkMbps() {
        return networkMbps;
    }

    @Override
    public double diskMB() {
        return diskMB;
    }

    public Protos.Offer getOffer() {
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
        return portRanges;
    }

    @Override
    public Map<String, Protos.Attribute> getAttributeMap() {
        return attributeMap;
    }

    @Override
    public Double getScalarValue(String name) {
        return aggregatedScalarResourceMap.getOrDefault(name, 0.0);
    }

    @Override
    public Map<String, Double> getScalarValues() {
        return aggregatedScalarResourceMap;
    }

    @Override
    public String toString() {
        return "Offer{"
                + "offer="
                + offer
                + ", resources='"
                + Utils.toString(resources)
                + '\''
                + ", hostname='"
                + hostname
                + '\''
                + ", vmID='"
                + vmID
                + '\''
                + ", attributeMap="
                + attributeMap
                + ", offeredTime="
                + offeredTime
                + '}';
    }

    private static List<Range> aggregateRangesResource(
            Map<String, List<Protos.Resource>> resourceMap, String resourceName) {
        if (resourceMap.get(resourceName) == null) {
            return Collections.emptyList();
        }
        return resourceMap.get(resourceName).stream()
                .flatMap(r -> r.getRanges().getRangeList().stream())
                .map(r -> new Range((int) r.getBegin(), (int) r.getEnd()))
                .collect(Collectors.toList());
    }
}
