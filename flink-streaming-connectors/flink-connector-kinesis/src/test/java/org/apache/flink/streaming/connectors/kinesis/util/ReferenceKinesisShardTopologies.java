/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ReferenceKinesisShardTopologies {

	private static final String DEFAULT_REGION = "us-east-1";
	private static final String DEFAULT_STREAM = "flink-kinesis-test";

	/**
	 * A basic topology with 4 shards, where each shard is still open,
	 * and have no parent-child relationships due to shard split or merge.
	 *
	 * Topology layout:
	 *
	 * +- shard 0 (seq:   0 ~ open)
	 * |
	 * +- shard 1 (seq: 250 ~ open)
	 * |
	 * +- shard 2 (seq: 500 ~ open)
	 * |
	 * +- shard 3 (seq: 750 ~ open)
	 *
	 */
	public static List<KinesisStreamShard> flatTopologyWithFourOpenShards() {
		int shardCount = 4;
		List<KinesisStreamShard> topology = new ArrayList<>(shardCount);
		topology.add(new KinesisStreamShard(
			DEFAULT_REGION, DEFAULT_STREAM,
			KinesisShardIdGenerator.generateFromShardOrder(0),
			"0", null, null, null));
		topology.add(new KinesisStreamShard(
			DEFAULT_REGION, DEFAULT_STREAM,
			KinesisShardIdGenerator.generateFromShardOrder(1),
			"250", null, null, null));
		topology.add(new KinesisStreamShard(
			DEFAULT_REGION, DEFAULT_STREAM,
			KinesisShardIdGenerator.generateFromShardOrder(2),
			"500", null, null, null));
		topology.add(new KinesisStreamShard(
			DEFAULT_REGION, DEFAULT_STREAM,
			KinesisShardIdGenerator.generateFromShardOrder(3),
			"750", null, null, null));
		return topology;
	}

	/**
	 * A basic topology with 4 shards, where each shard is still open,
	 * and have no parent-child relationships due to shard split or merge.
	 *
	 * Topology layout:
	 *
	 * +- shard 0 (seq:   0 ~ 120) --+
	 * |                             +- (merge) -- shard 3 (750 ~ open)
	 * +- shard 1 (seq: 250 ~ 289) --+
	 * |
	 * +- shard 2 (seq: 500 ~ open)
	 *
	 */
	public static List<KinesisStreamShard> topologyWithThreeInitialShardsAndFirstTwoMerged() {
		int shardCount = 4;

		String firstShardId = KinesisShardIdGenerator.generateFromShardOrder(0);
		String secondShardId = KinesisShardIdGenerator.generateFromShardOrder(1);
		String thirdShardId = KinesisShardIdGenerator.generateFromShardOrder(2);
		String fourthShardId = KinesisShardIdGenerator.generateFromShardOrder(3);

		List<KinesisStreamShard> topology = new ArrayList<>(shardCount);
		topology.add(new KinesisStreamShard(DEFAULT_REGION, DEFAULT_STREAM, firstShardId, "0", "120", null, null));
		topology.add(new KinesisStreamShard(DEFAULT_REGION, DEFAULT_STREAM, secondShardId, "250", "289", null, null));
		topology.add(new KinesisStreamShard(DEFAULT_REGION, DEFAULT_STREAM, thirdShardId, "500", null, null, null));
		topology.add(new KinesisStreamShard(DEFAULT_REGION, DEFAULT_STREAM, fourthShardId, "750", null, firstShardId, secondShardId));

		return topology;
	}

}
