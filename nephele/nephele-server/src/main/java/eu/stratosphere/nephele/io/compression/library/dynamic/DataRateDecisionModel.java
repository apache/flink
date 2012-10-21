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

package eu.stratosphere.nephele.io.compression.library.dynamic;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;

/**
 * The decision model for dynamic compression as described in Hovestadt et al.
 * "Evaluating Adaptive Compression to Mitigate the Effects of Shared I/O in Clouds", DataCloud 2011, 2011.
 * 
 * @author warneke
 */
public final class DataRateDecisionModel implements DecisionModel {

	private final int GRANULARITY;

	private final int numberOfAvailableCompressionLibraries;

	private final int[] backoff;

	private int sumOfTimeStamps = 0;

	private boolean increasedCompressionLevel = true;

	private int currentSelection = 0;

	private double lastDataRate = -1.0f;

	private int callCount = 0;

	private int sumOfTransferDurations = 0;

	private long sumOfBufferSizes = 0;

	private static final double DELTA = 0.2f;

	public DataRateDecisionModel(int numberOfAvailableCompressionLibraries) {
		this.numberOfAvailableCompressionLibraries = numberOfAvailableCompressionLibraries;
		this.backoff = new int[numberOfAvailableCompressionLibraries];
		for (int i = 0; i < this.backoff.length; i++) {
			this.backoff[i] = 0;
		}

		this.GRANULARITY = GlobalConfiguration.getInteger("granularity", 2000);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getCompressionLevelForNextBuffer(int sizeOfLastUncompressedBuffer, int durationOfLastBufferTransfer) {

		this.sumOfTimeStamps += durationOfLastBufferTransfer;
		if (this.sumOfTimeStamps < GRANULARITY) {
			this.sumOfBufferSizes += sizeOfLastUncompressedBuffer;
			this.sumOfTransferDurations += durationOfLastBufferTransfer; // TODO: Remove possible redundancy with
																			// sumOfTimeStamps
		} else {

			// Calculate data rate
			final double currentDataRate = (double) this.sumOfBufferSizes
				/ ((double) this.sumOfTransferDurations * 128.0f);
			if (this.lastDataRate < 0.0f) {
				this.lastDataRate = currentDataRate;
			}

			final int nextCompressionLevel = selectNextCompressionLevel(currentDataRate);
			final int diff = nextCompressionLevel - this.currentSelection;
			if (diff > 0) {
				this.increasedCompressionLevel = true;
			} else if (diff < 0) {
				this.increasedCompressionLevel = false;
			}

			this.currentSelection = nextCompressionLevel;
			this.lastDataRate = currentDataRate;

			this.sumOfBufferSizes = 0;
			this.sumOfTransferDurations = 0;
			this.sumOfTimeStamps = 0;
		}

		return this.currentSelection;
	}

	private int selectNextCompressionLevel(double currentDataRate) {

		int nextCompressionLevel = this.currentSelection;
		this.callCount++;

		final double diff = currentDataRate - this.lastDataRate;
		final boolean diffIsPositive = (diff > 0);
		final boolean diffWithinBounds = Math.abs(diff) < (DELTA * this.lastDataRate);

		if (diffWithinBounds) { // No change in data rate

			if (this.callCount >= (int) (Math.pow(2.0f, this.backoff[this.currentSelection]))) {

				if (this.currentSelection == 0) {
					this.increasedCompressionLevel = true;
				}

				if (this.currentSelection == this.numberOfAvailableCompressionLibraries - 1) {
					this.increasedCompressionLevel = false;
				}

				if (this.increasedCompressionLevel) {
					++nextCompressionLevel;
				} else {
					--nextCompressionLevel;
				}
				this.callCount = 0;
			}

		} else {

			if (diffIsPositive) { // Improvement in data rate

				this.backoff[nextCompressionLevel]++;

			} else { // Degradation of data rate

				this.backoff[nextCompressionLevel] = 0;
				if (this.increasedCompressionLevel) {
					--nextCompressionLevel;
				} else {
					++nextCompressionLevel;
				}
			}

			this.callCount = 0;
		}

		// Check bounds
		if (nextCompressionLevel < 0) {
			nextCompressionLevel = 0;
		} else if (nextCompressionLevel == numberOfAvailableCompressionLibraries) {
			nextCompressionLevel = this.numberOfAvailableCompressionLibraries - 1;
		}

		return nextCompressionLevel;
	}
}
