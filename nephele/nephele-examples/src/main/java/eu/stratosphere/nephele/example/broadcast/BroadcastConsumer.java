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

package eu.stratosphere.nephele.example.broadcast;

import eu.stratosphere.nephele.io.BipartiteDistributionPattern;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;

/**
 * This is a sample consumer task for the broadcast test job.
 * 
 * @author warneke
 */
public class BroadcastConsumer extends AbstractOutputTask {

	/**
	 * The key to access the output path configuration entry.
	 */
	public static final String OUTPUT_PATH_KEY = "broadcast.output.path";

	/**
	 * The key to access the output path configuration entry.
	 */
	public static final String TOPOLOGY_TREE_KEY = "broadcast.topology.tree";

	/**
	 * The record record through which this task receives incoming records.
	 */
	private MutableRecordReader<BroadcastRecord> input;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {

		this.input = new MutableRecordReader<BroadcastRecord>(this, new BipartiteDistributionPattern());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {

		int i = 0;

		final BroadcastRecord record = new BroadcastRecord();

		while (this.input.next(record)) {

			if ((i++ % BroadcastProducer.TIMESTAMP_INTERVAL) == 0) {
				/*
				 * final long timestamp = record.getTimestamp();
				 * final long now = System.currentTimeMillis();
				 * System.out.println(now - timestamp);
				 */
				// TODO: Calculate time stamp here
			}

		}
	}
}
