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

package eu.stratosphere.nephele.streaming;

import eu.stratosphere.nephele.io.InputGateListener;
import eu.stratosphere.nephele.io.OutputGateListener;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;

public final class StreamingTaskListener implements InputGateListener, OutputGateListener {

	private final static int SIZEOFLONG = 8;

	private static enum TaskType {
		INPUT, REGULAR, OUTPUT
	};

	private final TaskType taskType;

	private final int taggingInterval;

	private final int aggregationInterval;

	private byte[] tag = null;

	private int tagCounter = 0;

	private int aggregationCounter = 0;

	private double aggregatedValue = -1.0;

	static StreamingTaskListener createForInputTask(final int taggingInterval, final int aggregationInterval) {

		return new StreamingTaskListener(TaskType.INPUT, taggingInterval, aggregationInterval);
	}

	static StreamingTaskListener createForRegularTask(final int aggregationInterval) {

		return new StreamingTaskListener(TaskType.REGULAR, 0, aggregationInterval);
	}

	static StreamingTaskListener createForOutputTask(final int aggregationInterval) {

		return new StreamingTaskListener(TaskType.OUTPUT, 0, aggregationInterval);
	}

	private StreamingTaskListener(final TaskType taskType, final int taggingInterval, final int aggregationInterval) {

		this.taskType = taskType;
		this.taggingInterval = taggingInterval;
		this.aggregationInterval = aggregationInterval;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void channelCapacityExhausted(final int channelIndex) {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void recordEmitted(final Record record) {

		switch (this.taskType) {
		case INPUT:
			if (this.tagCounter++ == this.taggingInterval) {
				final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
				taggableRecord.setTag(createTag());
				this.tagCounter = 0;
			}
			break;
		case REGULAR:
			final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
			taggableRecord.setTag(this.tag);
			break;
		case OUTPUT:
			throw new IllegalStateException("Output task emitted record");
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void waitingForAnyChannel() {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void recordReceived(final Record record) {

		switch (this.taskType) {
		case INPUT:
			throw new IllegalStateException("Input task received record");
		case REGULAR: {
			final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
			this.tag = taggableRecord.getTag();
		}
			break;
		case OUTPUT: {
			final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
			this.tag = taggableRecord.getTag();
			if (this.tag != null) {

				System.out.println(System.currentTimeMillis() - byteArrayToLong(this.tag));
			}
		}
			break;
		}

	}

	private byte[] createTag() {

		if (this.tag == null) {
			this.tag = new byte[SIZEOFLONG];
		}

		longToByteArray(System.currentTimeMillis(), this.tag);

		return this.tag;
	}

	private static void longToByteArray(final long longToSerialize, final byte[] buffer) {

		for (int i = 0; i < SIZEOFLONG; ++i) {
			final int shift = i << 3; // i * 8
			buffer[(SIZEOFLONG - 1) - i] = (byte) ((longToSerialize & (0xffL << shift)) >>> shift);
		}
	}

	private static long byteArrayToLong(final byte[] buffer) {

		long l = 0;

		for (int i = 0; i < SIZEOFLONG; ++i) {
			l |= (buffer[(SIZEOFLONG - 1) - i] & 0xffL) << (i << 3);
		}

		return l;
	}
}
