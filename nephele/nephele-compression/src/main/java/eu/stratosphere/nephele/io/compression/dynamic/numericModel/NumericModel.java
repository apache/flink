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

package eu.stratosphere.nephele.io.compression.dynamic.numericModel;

import java.util.HashMap;

import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.compression.library.dynamic.DynamicCompressor;
import eu.stratosphere.nephele.io.compression.profiling.CompressionInstanceProfiler;
import eu.stratosphere.nephele.io.compression.profiling.InternalInstanceProfilingDataCompression;

public class NumericModel /* implements CompressionDecisionModel */{

	private CompressionInstanceProfiler profiler = null;

	private InternalInstanceProfilingDataCompression profilingData = null;

	private CompressionLevel lastCompressionLevel = null;

	private HashMap<CompressionLevel, InternalInstanceProfilingDataCompression> profilingResultsPerAlgorithm = null;

	private boolean allLevelsTested = false;

	private ChannelType channelType = null;

	private DynamicCompressor dc;

	private int nextEvaluationRound = 0;

	private int nextResetRound = 0;

	private static final int ROUNDS_WITHOUT_EVALUATION = 3;

	private static final int ROUNDS_UNTIL_RESET = 30;

	public NumericModel(CompressionInstanceProfiler cp, ChannelType channelType) {
		this.profiler = cp;
		this.profilingResultsPerAlgorithm = new HashMap<CompressionLevel, InternalInstanceProfilingDataCompression>();
		this.channelType = channelType;
	}

	// @Override
	public CompressionLevel getCompressionLevelForNextBlock(int bufferLength, CompressionLevel lastCompressionLevel) {
		if (nextEvaluationRound == 0) {

			if (this.lastCompressionLevel == null) {
				this.lastCompressionLevel = CompressionLevel.LIGHT_COMPRESSION;
				nextEvaluationRound += ROUNDS_WITHOUT_EVALUATION;
				return CompressionLevel.LIGHT_COMPRESSION;
			}

			// update profiling results for Algorithm
			if (lastCompressionLevel != null)
				this.profilingResultsPerAlgorithm.put(lastCompressionLevel, profilingData);

			if (profilingData == null) {
				this.lastCompressionLevel = CompressionLevel.LIGHT_COMPRESSION;
				return CompressionLevel.LIGHT_COMPRESSION;
			} else {
				nextEvaluationRound += ROUNDS_WITHOUT_EVALUATION;
				System.out.println("LastLevel: " + lastCompressionLevel + " CompressionRatio: "
					+ profilingData.getCompressionRatio() + " CompressionTime: " + profilingData.getCompressionTime()
					+ " IOWaitCPU: " + profilingData.getIOWaitCPU() + " IdleCPU: " + profilingData.getIdleCPU()
					+ " SystemCPU: " + profilingData.getSystemCPU() + " UserCPU: " + profilingData.getUserCPU());

				// first check if we have profiling data for all levels
				CompressionLevel next = null;
				if (!allLevelsTested)
					next = checkIfLevelNotTested(lastCompressionLevel);
				else if (nextResetRound <= 0) {
					allLevelsTested = false;
					this.profilingResultsPerAlgorithm.clear();
					next = checkIfLevelNotTested(lastCompressionLevel);
					System.out.println("Resetting results ...");
				}

				if (next == null) {
					next = getNextLevel();
				}

				this.lastCompressionLevel = next;
				return next;

			}
		} else {
			nextEvaluationRound--;
			nextResetRound--;

			// update profiling results for Algorithm
			if (lastCompressionLevel != null)
				this.profilingResultsPerAlgorithm.put(lastCompressionLevel, profilingData);

			return lastCompressionLevel;
		}
		// return null;
	}

	private CompressionLevel getNextLevel() {
		int lzocpuwait = -1, lzoiowait = Integer.MAX_VALUE, lzoresult = Integer.MAX_VALUE;
		int lzmacpuwait = -1, lzmaiowait = Integer.MAX_VALUE, lzmaresult = Integer.MAX_VALUE;
		int bzip2cpuwait = -1, bzip2iowait = Integer.MAX_VALUE, bzip2result = Integer.MAX_VALUE;
		int zlibcpuwait = -1, zlibiowait = Integer.MAX_VALUE, zlibresult = Integer.MAX_VALUE;
		int nocompressioncpuwait = -1, nocompressioniowait = Integer.MAX_VALUE, nocompressionresult = Integer.MAX_VALUE;

		InternalInstanceProfilingDataCompression tmp = null;
		tmp = this.profilingResultsPerAlgorithm.get(CompressionLevel.LIGHT_COMPRESSION);
		if (tmp != null) {
			lzocpuwait = tmp.getUserCPU() + tmp.getSystemCPU() - tmp.getIOWaitCPU();
			lzoiowait = tmp.getIOWaitCPU();
			lzoresult = (tmp.getIOWaitCPU() * 100) + (int) (tmp.getCompressionTime() * 10 * tmp.getCompressionRatio());
		}

		tmp = this.profilingResultsPerAlgorithm.get(CompressionLevel.MEDIUM_COMPRESSION);
		if (tmp != null) {
			zlibcpuwait = tmp.getUserCPU() + tmp.getSystemCPU() - tmp.getIOWaitCPU();
			zlibiowait = tmp.getIOWaitCPU();
			zlibresult = (tmp.getIOWaitCPU() * 100) + (int) (tmp.getCompressionTime() * 10 * tmp.getCompressionRatio());
		}

		tmp = this.profilingResultsPerAlgorithm.get(CompressionLevel.HEAVY_COMPRESSION);
		if (tmp != null) {
			lzmacpuwait = tmp.getUserCPU() + tmp.getSystemCPU() - tmp.getIOWaitCPU();
			lzmaiowait = tmp.getIOWaitCPU();
			lzmaresult = (tmp.getIOWaitCPU() * 100) + (int) (tmp.getCompressionTime() * 10 * tmp.getCompressionRatio());
		}

		tmp = this.profilingResultsPerAlgorithm.get(CompressionLevel.NO_COMPRESSION);
		if (tmp != null) {
			nocompressioncpuwait = tmp.getUserCPU() + tmp.getSystemCPU() - tmp.getIOWaitCPU();
			nocompressioniowait = tmp.getIOWaitCPU();
			nocompressionresult = (tmp.getIOWaitCPU() * 100)
				+ (int) (tmp.getCompressionTime() * tmp.getCompressionRatio());
		}
		System.out.println("Results: NC: " + nocompressionresult + " LZO: " + lzoresult + " ZLIB: " + zlibresult
			+ " BZIP2: " + bzip2result + " LZMA: " + lzmaresult);

		if (nocompressioniowait == 0)
			return CompressionLevel.NO_COMPRESSION;
		else if (lzoiowait == 0)
			return CompressionLevel.LIGHT_COMPRESSION;
		else if (zlibiowait == 0)
			return CompressionLevel.MEDIUM_COMPRESSION;
		else if (lzmaiowait == 0)
			return CompressionLevel.HEAVY_COMPRESSION;

		if (nocompressionresult <= lzoresult && nocompressionresult <= zlibresult && nocompressionresult <= bzip2result
			&& nocompressionresult <= lzmaresult) {
			return CompressionLevel.NO_COMPRESSION;
		} else if (lzoresult <= zlibresult && lzoresult <= bzip2result && lzoresult <= lzmaresult) {
			return CompressionLevel.LIGHT_COMPRESSION;
		} else if (zlibresult <= bzip2result && zlibresult <= lzmaresult) {
			return CompressionLevel.MEDIUM_COMPRESSION;
		} else
			return CompressionLevel.HEAVY_COMPRESSION;

		// return CompressionLevel.LIGHT_COMPRESSION;
	}

	private CompressionLevel checkIfLevelNotTested(CompressionLevel lastCompressionLevel) {

		if (!profilingResultsPerAlgorithm.containsKey(CompressionLevel.LIGHT_COMPRESSION)) {
			// check if level is loaded by DynamicCompressor
			if (this.lastCompressionLevel == CompressionLevel.LIGHT_COMPRESSION
				&& lastCompressionLevel != CompressionLevel.LIGHT_COMPRESSION) {
				this.profilingResultsPerAlgorithm.put(CompressionLevel.LIGHT_COMPRESSION, null);
			} else {
				return CompressionLevel.LIGHT_COMPRESSION;
			}
		}

		if (!profilingResultsPerAlgorithm.containsKey(CompressionLevel.MEDIUM_COMPRESSION)) {
			// check if level is loaded by DynamicCompressor
			if (this.lastCompressionLevel == CompressionLevel.MEDIUM_COMPRESSION
				&& lastCompressionLevel != CompressionLevel.MEDIUM_COMPRESSION) {
				this.profilingResultsPerAlgorithm.put(CompressionLevel.MEDIUM_COMPRESSION, null);
			} else {
				return CompressionLevel.MEDIUM_COMPRESSION;
			}
		}

		if (!profilingResultsPerAlgorithm.containsKey(CompressionLevel.HEAVY_COMPRESSION)) {
			// check if level is loaded by DynamicCompressor
			if (this.lastCompressionLevel == CompressionLevel.HEAVY_COMPRESSION
				&& lastCompressionLevel != CompressionLevel.HEAVY_COMPRESSION) {
				this.profilingResultsPerAlgorithm.put(CompressionLevel.HEAVY_COMPRESSION, null);
			} else {
				return CompressionLevel.HEAVY_COMPRESSION;
			}
		}

		if (!profilingResultsPerAlgorithm.containsKey(CompressionLevel.NO_COMPRESSION)) {
			return CompressionLevel.NO_COMPRESSION;
		}

		allLevelsTested = true;
		nextResetRound = ROUNDS_UNTIL_RESET;
		return null;
	}

	// @Override
	public void setCompressionRatio(double compressionRatio, long compressionTime) {
		profilingData = profiler.generateProfilingDataCompression(System.currentTimeMillis());
		profilingData.setCompressionRatio(compressionRatio);
		profilingData.setCompressionTime(compressionTime);

	}

	// @Override
	public ChannelType getChannelType() {
		return this.channelType;
	}

	// @Override
	public void setDynamicCompressor(DynamicCompressor dc) {
		this.dc = dc;

	}

}
