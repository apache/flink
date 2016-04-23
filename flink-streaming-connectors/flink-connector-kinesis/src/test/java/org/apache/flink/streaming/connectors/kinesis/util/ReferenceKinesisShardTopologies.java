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

import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ReferenceKinesisShardTopologies {

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
			DEFAULT_STREAM,
			new Shard()
				.withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))
				.withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("0"))));
		topology.add(new KinesisStreamShard(
			DEFAULT_STREAM,
			new Shard()
				.withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))
				.withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("250"))));
		topology.add(new KinesisStreamShard(
			DEFAULT_STREAM,
			new Shard()
				.withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))
				.withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("500"))));
		topology.add(new KinesisStreamShard(
			DEFAULT_STREAM,
			new Shard()
				.withShardId(KinesisShardIdGenerator.generateFromShardOrder(3))
				.withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("750"))));
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

		List<KinesisStreamShard> topology = new ArrayList<>(shardCount);
		topology.add(new KinesisStreamShard(
			DEFAULT_STREAM,
			new Shard()
				.withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))
				.withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("0").withEndingSequenceNumber("120"))));
		topology.add(new KinesisStreamShard(
			DEFAULT_STREAM,
			new Shard()
				.withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))
				.withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("250").withEndingSequenceNumber("289"))));
		topology.add(new KinesisStreamShard(
			DEFAULT_STREAM,
			new Shard()
				.withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))
				.withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("500"))));
		topology.add(new KinesisStreamShard(
			DEFAULT_STREAM,
			new Shard()
				.withShardId(KinesisShardIdGenerator.generateFromShardOrder(3))
				.withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("750"))));

		return topology;
	}

}
