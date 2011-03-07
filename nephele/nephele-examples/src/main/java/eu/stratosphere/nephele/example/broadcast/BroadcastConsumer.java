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
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;

/**
 * This is a sample consumer task for the broadcast test job.
 * 
 * @author warneke
 */
public class BroadcastConsumer extends AbstractFileOutputTask {

	/**
	 * The record record through which this task receives incoming records.
	 */
	private RecordReader<BroadcastRecord> input;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {

		this.input = new RecordReader<BroadcastRecord>(this, BroadcastRecord.class, new BipartiteDistributionPattern());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {

		int count = 1;

		while (this.input.hasNext()) {

			final BroadcastRecord record = this.input.next();

			// Check content of record
			for (int i = 0; i < record.getSize(); i++) {

				if (record.getData(i) != i) {
					throw new RuntimeException(count + "th record has unexpected byte " + record.getData(i)
						+ " at position " + i);
				}
			}

			++count;
		}
	}
}
