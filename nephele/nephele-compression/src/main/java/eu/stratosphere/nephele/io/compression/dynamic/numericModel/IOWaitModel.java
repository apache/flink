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

import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.compression.library.dynamic.DynamicCompressor;
import eu.stratosphere.nephele.io.compression.profiling.CompressionInstanceProfiler;
import eu.stratosphere.nephele.io.compression.profiling.InternalInstanceProfilingDataCompression;

/**
 * A simple Decision Model for the Dynamic Compressor which tries to minimize IOWait.
 * 
 * @author akli
 *         Decision tree:
 *         LCL = Last Compression Level
 *         CCL = Current Compression Level
 *         LW = Last IOWait
 *         CW = Current IOWait
 *         LCL > CCL
 *         | | |
 *         LW > CW LW = CW LW < CW
 *         | | | | | |
 *         CW = 0 CW > 0 CW = 0 CW > 0 CW = 0 CW > 0
 *         -> switch -> stay -> switch ->switch -> switch -> switch
 *         back back back forward forward
 *         (unreachable)
 *         LCL = CCL
 *         | | |
 *         LW > CW LW = CW LW < CW
 *         | | | | | |
 *         CW = 0 CW > 0 CW = 0 CW > 0 CW = 0 CW > 0
 *         -> switch -> stay -> switch ->switch -> switch -> switch
 *         back back forward forward forward
 *         (unreachable)
 *         LCL < CCL
 *         | | |
 *         LW > CW LW = CW LW < CW
 *         | | | | | |
 *         CW = 0 CW > 0 CW = 0 CW > 0 CW = 0 CW > 0
 *         -> stay -> switch -> switch ->switch -> switch -> switch
 *         forward back forward back back
 *         (unreachable)
 */

public class IOWaitModel /* implements CompressionDecisionModel */{

	private CompressionInstanceProfiler profiler = null;

	private InternalInstanceProfilingDataCompression profilingDataCurrent = null;

	private InternalInstanceProfilingDataCompression profilingDataPast = null;

	private CompressionLevel pastCompressionLevel = null;

	private CompressionLevel currentCompressionLevel = null;

	private DynamicCompressor dc = null;

	private int dynamicCompressorID = 0;

	private ChannelType channelType;

	private int nextEvaluationRound = 0;

	private static final int ROUNDS_WITHOUT_EVALUATION = 3;

	public IOWaitModel(CompressionInstanceProfiler cp, ChannelType channelType) {
		this.profiler = cp;
		this.channelType = channelType;
	}

	// @Override
	public CompressionLevel getCompressionLevelForNextBlock(int bufferLength, CompressionLevel lastCompressionLevel) {
		CompressionLevel next = lastCompressionLevel;
		if (nextEvaluationRound == 0) {

			if (this.pastCompressionLevel == null || profilingDataCurrent == null) {
				this.pastCompressionLevel = CompressionLevel.LIGHT_COMPRESSION;
				this.currentCompressionLevel = CompressionLevel.LIGHT_COMPRESSION;
				return CompressionLevel.LIGHT_COMPRESSION;
			} else {
				System.out.println("IOWaitModel: LastLevel: " + lastCompressionLevel + " CompressionRatio: "
					+ profilingDataCurrent.getCompressionRatio() + " CompressionTime: "
					+ profilingDataCurrent.getCompressionTime() + " IOWaitCPU: " + profilingDataCurrent.getIOWaitCPU()
					+ " IdleCPU: " + profilingDataCurrent.getIdleCPU() + " SystemCPU: "
					+ profilingDataCurrent.getSystemCPU() + " UserCPU: " + profilingDataCurrent.getUserCPU());

				if (profilingDataCurrent.getCompressionRatio() >= 1.0
					&& lastCompressionLevel != CompressionLevel.NO_COMPRESSION) {
					next = CompressionLevel.NO_COMPRESSION;
					nextEvaluationRound = ROUNDS_WITHOUT_EVALUATION;
				} else {
					next = getNextLevel(lastCompressionLevel);
				}

				this.pastCompressionLevel = lastCompressionLevel;

			}
		}

		nextEvaluationRound--;
		currentCompressionLevel = next;
		// next = lastCompressionLevel;

		return next;
	}

	private CompressionLevel getNextLevel(CompressionLevel lastLevel) {
		if (profilingDataPast == null || profilingDataCurrent == null) {
			nextEvaluationRound++;
			return lastLevel;
		} else {
			nextEvaluationRound += ROUNDS_WITHOUT_EVALUATION;

			int next = switchBackForwardOrStay(lastLevel);
			switch (lastLevel) {
			case NO_COMPRESSION:
				if (next > 0)
					return CompressionLevel.LIGHT_COMPRESSION;
				else
					return CompressionLevel.NO_COMPRESSION;
			case LIGHT_COMPRESSION:
				if (next < 0)
					return CompressionLevel.NO_COMPRESSION;
				else if (next == 0)
					return CompressionLevel.LIGHT_COMPRESSION;
				else
					return CompressionLevel.MEDIUM_COMPRESSION;

			case MEDIUM_COMPRESSION:
				if (next < 0)
					return CompressionLevel.LIGHT_COMPRESSION;
				else if (next == 0)
					return CompressionLevel.MEDIUM_COMPRESSION;
				else
					return CompressionLevel.HEAVY_COMPRESSION;

			case HEAVY_COMPRESSION:
				if (next < 0)
					return CompressionLevel.MEDIUM_COMPRESSION;
				else
					return CompressionLevel.HEAVY_COMPRESSION;

			default:
				return lastLevel;
			}
		}

	}

	/**
	 * @param lastLevel
	 * @return <0 = switchBack; 0 = stay; >0 switchForward
	 */
	private int switchBackForwardOrStay(CompressionLevel lastLevel) {
		int currentIndex = getIndexOfCompressionLevel(lastLevel);
		int pastIndex = getIndexOfCompressionLevel(pastCompressionLevel);

		int currentIOWait = profilingDataCurrent.getIOWaitCPU();
		int pastIOWait = profilingDataPast.getIOWaitCPU();

		// check if current compression level is higher then past one
		if (currentIndex > pastIndex) {
			// IOWait decreased
			if (pastIOWait > currentIOWait) {

				if (currentIOWait == 0)
					// if new IOWait is 0 we can stay at current level
					return 0;
				else
					// if IOwait decreased but is still not 0, we switch to higher compression
					return 1;

				// IOWait increased
			} else if (pastIOWait < currentIOWait) {
				// if past IOWait was lesser than current, we switch to lower compression
				return -1;

				// IOWait equal
			} else {
				// if IOWait was 0 and is still 0 we can switch to lower compression to avoid cpu usage
				if (currentIOWait == 0) {
					return -1;
					// if IOWait was greater 0 and is still greater 0 we try higher compression
				} else {
					return 1;
				}
			}
		} else if (currentIndex < pastIndex) {
			// IOWait decreased
			if (pastIOWait > currentIOWait) {

				if (currentIOWait == 0)
					// if new IOWait is 0 we can switch to lower compression to avoid cpu usage
					return -1;
				else
					// if IOwait decreased but is still not 0, we stay
					return 0;

				// IOWait increased
			} else if (pastIOWait < currentIOWait) {
				// if past IOWait was lesser than current, we switch to higher compression
				return 1;

				// IOWait equal
			} else {
				return -1;
			}
		} else {
			// IOWait decreased
			if (pastIOWait > currentIOWait) {
				if (currentIOWait > 0)
					return 0;
				else
					return -1;

				// IOWait increased
			} else if (pastIOWait < currentIOWait) {
				// if past IOWait was lesser than current, we switch to higher compression
				return 1;

				// IOWait equal
			} else {
				if (currentIOWait > 0)
					return 1;
				else
					return -1;
			}
		}
	}

	private int getIndexOfCompressionLevel(CompressionLevel level) {
		switch (level) {
		case NO_COMPRESSION:
			return 0;
		case LIGHT_COMPRESSION:
			return 1;
		case MEDIUM_COMPRESSION:
			return 2;
		case HEAVY_COMPRESSION:
			return 3;
		}

		return -1;
	}

	// @Override
	public void setCompressionRatio(double compressionRatio, long compressionTime) {
		if (compressionRatio >= 1.0 && currentCompressionLevel != CompressionLevel.NO_COMPRESSION)
			nextEvaluationRound = 0;

		if (nextEvaluationRound == 0) {
			profilingDataPast = profilingDataCurrent;
			profilingDataCurrent = profiler.generateProfilingDataCompression(System.currentTimeMillis());
			profilingDataCurrent.setCompressionRatio(compressionRatio);
			profilingDataCurrent.setCompressionTime(compressionTime);
		}

	}

	// @Override
	public void setDynamicCompressor(DynamicCompressor dc) {
		this.dc = dc;
	}

	// @Override
	public ChannelType getChannelType() {

		return this.channelType;
	}

}
