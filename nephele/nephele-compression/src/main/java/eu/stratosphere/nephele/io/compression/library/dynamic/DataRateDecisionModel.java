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

/**
 * A simple Decision Model for the Dynamic Compressor which tries to maximize the
 * achievable data rate.
 * TODO: Add decision tree here
 * 
 * @author akli
 */
public class DataRateDecisionModel implements DecisionModel {

	private static final int GRANULARITY = 400;

	private final int numberOfAvailableCompressionLibraries;

	private final int[] backoff;

	private boolean increasedCompressionLevel = true;

	private int currentSelection = 0;

	private double lastDataRate = -1.0f;

	private int bufferCount = 0;

	private int sumOfTransferDurations = 0;

	private long sumOfBufferSizes = 0;

	private double DELTA = 0.15f;

	private final DecisionLogger decisionLogger;

	private boolean loggerInitialized = false;

	public DataRateDecisionModel(int numberOfAvailableCompressionLibraries) {
		this.numberOfAvailableCompressionLibraries = numberOfAvailableCompressionLibraries;
		this.backoff = new int[numberOfAvailableCompressionLibraries];
		for (int i = 0; i < this.backoff.length; i++) {
			this.backoff[i] = 0;
		}

		this.decisionLogger = new DecisionLogger("compressionDecision");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getCompressionLevelForNextBuffer(int sizeOfLastUncompressedBuffer, int durationOfLastBufferTransfer) {

		if (++this.bufferCount % GRANULARITY != 0) {
			this.sumOfBufferSizes += sizeOfLastUncompressedBuffer;
			this.sumOfTransferDurations += durationOfLastBufferTransfer;
		} else {

			// Calculate data rate
			final double currentDataRate = (double) this.sumOfBufferSizes
				/ ((double) this.sumOfTransferDurations * 128.0f);
			if (this.lastDataRate < 0.0f) {
				this.lastDataRate = currentDataRate;
			}

			this.decisionLogger.log(System.currentTimeMillis(), currentDataRate, this.currentSelection);
			final int nextCompressionLevel = selectNextCompressionLevel(currentDataRate);
			final int diff = nextCompressionLevel - this.currentSelection;
			if (diff > 0) {
				this.increasedCompressionLevel = true;
			} else if (diff < 0) {
				this.increasedCompressionLevel = false;
			}

			this.currentSelection = nextCompressionLevel;
			System.out.println("Next compression level " + this.currentSelection);
			System.out.println("Backoff level for compression level " + this.currentSelection + ": "
				+ this.backoff[this.currentSelection]);
			this.lastDataRate = currentDataRate;

			this.sumOfBufferSizes = 0;
			this.sumOfTransferDurations = 0;
		}

		if (!this.loggerInitialized) {
			this.decisionLogger.log(System.currentTimeMillis(), 0.0, this.currentSelection);
			this.loggerInitialized = true;
		}

		return this.currentSelection;
	}

	private int count = 0;

	private int selectNextCompressionLevel(double currentDataRate) {

		int nextCompressionLevel = this.currentSelection;

		final double diff = currentDataRate - this.lastDataRate;
		final boolean diffIsPositive = (diff > 0);
		final boolean diffWithinBounds = Math.abs(diff) < (DELTA * this.lastDataRate);

		System.out.println("++++++ " + (++count) + ". decision (" + (this.increasedCompressionLevel ? "up" : "down")
			+ ") current compression level: " + this.currentSelection + " ++++");
		System.out.println("Current data rate: " + currentDataRate + "(" + this.sumOfBufferSizes + " / "
			+ this.sumOfTransferDurations + ")");

		if (diffWithinBounds) { // No change in data rate

			System.out.println("No change: " + diff);

			if (this.bufferCount >= (int) (Math.pow(2.0f, this.backoff[this.currentSelection]) * GRANULARITY)) {
				System.out.println("Buffer count is " + this.bufferCount + ", current selection "
					+ this.currentSelection + ", backoff: " + this.backoff[this.currentSelection]);

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
				this.bufferCount = 0;
			} else {
				System.out.println("Backoff: buffer count is " + this.bufferCount);
			}

		} else {

			if (diffIsPositive) { // Improvement in data rate

				System.out.println("Improvement: " + diff);

				this.backoff[nextCompressionLevel]++;

			} else { // Degradation of data rate

				System.out.println("Degradation: " + diff);

				this.backoff[nextCompressionLevel] = 0;
				if (this.increasedCompressionLevel) {
					--nextCompressionLevel;
				} else {
					++nextCompressionLevel;
				}
			}

			this.bufferCount = 0;
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
