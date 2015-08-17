/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.kafka_backport.common;

import org.apache.flink.kafka_backport.common.utils.Utils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// ----------------------------------------------------------------------------
//  This class is copied from the Apache Kafka project.
// 
//  The class is part of a "backport" of the new consumer API, in order to
//  give Flink access to its functionality until the API is properly released.
// 
//  This is a temporary workaround!
// ----------------------------------------------------------------------------

/**
 * A representation of a subset of the nodes, topics, and partitions in the Kafka cluster.
 */
public final class Cluster {

    private final List<Node> nodes;
    private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
    private final Map<String, List<PartitionInfo>> partitionsByTopic;
    private final Map<String, List<PartitionInfo>> availablePartitionsByTopic;
    private final Map<Integer, List<PartitionInfo>> partitionsByNode;
    private final Map<Integer, Node> nodesById;

    /**
     * Create a new cluster with the given nodes and partitions
     * @param nodes The nodes in the cluster
     * @param partitions Information about a subset of the topic-partitions this cluster hosts
     */
    public Cluster(Collection<Node> nodes, Collection<PartitionInfo> partitions) {
        // make a randomized, unmodifiable copy of the nodes
        List<Node> copy = new ArrayList<Node>(nodes);
        Collections.shuffle(copy);
        this.nodes = Collections.unmodifiableList(copy);
        
        this.nodesById = new HashMap<Integer, Node>();
        for (Node node: nodes)
            this.nodesById.put(node.id(), node);

        // index the partitions by topic/partition for quick lookup
        this.partitionsByTopicPartition = new HashMap<TopicPartition, PartitionInfo>(partitions.size());
        for (PartitionInfo p : partitions)
            this.partitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);

        // index the partitions by topic and node respectively, and make the lists
        // unmodifiable so we can hand them out in user-facing apis without risk
        // of the client modifying the contents
        HashMap<String, List<PartitionInfo>> partsForTopic = new HashMap<String, List<PartitionInfo>>();
        HashMap<Integer, List<PartitionInfo>> partsForNode = new HashMap<Integer, List<PartitionInfo>>();
        for (Node n : this.nodes) {
            partsForNode.put(n.id(), new ArrayList<PartitionInfo>());
        }
        for (PartitionInfo p : partitions) {
            if (!partsForTopic.containsKey(p.topic()))
                partsForTopic.put(p.topic(), new ArrayList<PartitionInfo>());
            List<PartitionInfo> psTopic = partsForTopic.get(p.topic());
            psTopic.add(p);

            if (p.leader() != null) {
                List<PartitionInfo> psNode = Utils.notNull(partsForNode.get(p.leader().id()));
                psNode.add(p);
            }
        }
        this.partitionsByTopic = new HashMap<String, List<PartitionInfo>>(partsForTopic.size());
        this.availablePartitionsByTopic = new HashMap<String, List<PartitionInfo>>(partsForTopic.size());
        for (Map.Entry<String, List<PartitionInfo>> entry : partsForTopic.entrySet()) {
            String topic = entry.getKey();
            List<PartitionInfo> partitionList = entry.getValue();
            this.partitionsByTopic.put(topic, Collections.unmodifiableList(partitionList));
            List<PartitionInfo> availablePartitions = new ArrayList<PartitionInfo>();
            for (PartitionInfo part : partitionList) {
                if (part.leader() != null)
                    availablePartitions.add(part);
            }
            this.availablePartitionsByTopic.put(topic, Collections.unmodifiableList(availablePartitions));
        }
        this.partitionsByNode = new HashMap<Integer, List<PartitionInfo>>(partsForNode.size());
        for (Map.Entry<Integer, List<PartitionInfo>> entry : partsForNode.entrySet())
            this.partitionsByNode.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));

    }

    /**
     * Create an empty cluster instance with no nodes and no topic-partitions.
     */
    public static Cluster empty() {
        return new Cluster(new ArrayList<Node>(0), new ArrayList<PartitionInfo>(0));
    }

    /**
     * Create a "bootstrap" cluster using the given list of host/ports
     * @param addresses The addresses
     * @return A cluster for these hosts/ports
     */
    public static Cluster bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<Node>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses)
            nodes.add(new Node(nodeId--, address.getHostName(), address.getPort()));
        return new Cluster(nodes, new ArrayList<PartitionInfo>(0));
    }

    /**
     * @return The known set of nodes
     */
    public List<Node> nodes() {
        return this.nodes;
    }
    
    /**
     * Get the node by the node id (or null if no such node exists)
     * @param id The id of the node
     * @return The node, or null if no such node exists
     */
    public Node nodeById(int id) {
        return this.nodesById.get(id);
    }

    /**
     * Get the current leader for the given topic-partition
     * @param topicPartition The topic and partition we want to know the leader for
     * @return The node that is the leader for this topic-partition, or null if there is currently no leader
     */
    public Node leaderFor(TopicPartition topicPartition) {
        PartitionInfo info = partitionsByTopicPartition.get(topicPartition);
        if (info == null)
            return null;
        else
            return info.leader();
    }

    /**
     * Get the metadata for the specified partition
     * @param topicPartition The topic and partition to fetch info for
     * @return The metadata about the given topic and partition
     */
    public PartitionInfo partition(TopicPartition topicPartition) {
        return partitionsByTopicPartition.get(topicPartition);
    }

    /**
     * Get the list of partitions for this topic
     * @param topic The topic name
     * @return A list of partitions
     */
    public List<PartitionInfo> partitionsForTopic(String topic) {
        return this.partitionsByTopic.get(topic);
    }

    /**
     * Get the list of available partitions for this topic
     * @param topic The topic name
     * @return A list of partitions
     */
    public List<PartitionInfo> availablePartitionsForTopic(String topic) {
        return this.availablePartitionsByTopic.get(topic);
    }

    /**
     * Get the list of partitions whose leader is this node
     * @param nodeId The node id
     * @return A list of partitions
     */
    public List<PartitionInfo> partitionsForNode(int nodeId) {
        return this.partitionsByNode.get(nodeId);
    }

    /**
     * Get all topics.
     * @return a set of all topics
     */
    public Set<String> topics() {
        return this.partitionsByTopic.keySet();
    }

    @Override
    public String toString() {
        return "Cluster(nodes = " + this.nodes + ", partitions = " + this.partitionsByTopicPartition.values() + ")";
    }

}
