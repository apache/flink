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

import java.io.IOException;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.CompressionLibrary;
import eu.stratosphere.nephele.io.compression.Compressor;

public class DynamicCompressor implements Compressor {

	private final Compressor[] compressors;

	private int selectedCompressor = 0;

	private long lastTimestamp = -1;

	private int sizeOfLastUncompressedBuffer = -1;

	private final DecisionModel decisionModel;

	public DynamicCompressor(CompressionLibrary[] compressionLibraries)
																		throws CompressionException {

		// Initialize the different compressors
		this.compressors = new Compressor[compressionLibraries.length];
		for (int i = 0; i < this.compressors.length; i++) {
			this.compressors[i] = compressionLibraries[i].getCompressor();
		}

		// Initialize decision model
		this.decisionModel = new DataRateDecisionModel(this.compressors.length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void compress() throws IOException {

		this.compressors[this.selectedCompressor].compress();

		this.sizeOfLastUncompressedBuffer = this.compressors[this.selectedCompressor].getUncompresssedDataBuffer()
			.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer getCompressedDataBuffer() {

		return this.compressors[this.selectedCompressor].getCompressedDataBuffer();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer getUncompresssedDataBuffer() {

		return this.compressors[this.selectedCompressor].getUncompresssedDataBuffer();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setCompressedDataBuffer(Buffer buffer) {

		this.compressors[this.selectedCompressor].setCompressedDataBuffer(buffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setUncompressedDataBuffer(Buffer buffer) {

		this.compressors[this.selectedCompressor].setUncompressedDataBuffer(buffer);
	}

	@Override
	public int getCurrentInternalCompressionLibraryIndex() {

		int oldVal = this.selectedCompressor;
		final long timestamp = System.currentTimeMillis();

		if (this.lastTimestamp > 0) {
			this.selectedCompressor = this.decisionModel.getCompressionLevelForNextBuffer(
				this.sizeOfLastUncompressedBuffer, (int) (timestamp - this.lastTimestamp));
		}

		this.lastTimestamp = timestamp;

		return oldVal;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {

		for (int i = 0; i < this.compressors.length; i++) {
			this.compressors[i].shutdown();
		}
	}
}
