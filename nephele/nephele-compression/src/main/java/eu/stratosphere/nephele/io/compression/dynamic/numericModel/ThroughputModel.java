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
import eu.stratosphere.nephele.io.compression.CompressionLoader;
import eu.stratosphere.nephele.io.compression.library.dynamic.DynamicCompressor;
import eu.stratosphere.nephele.io.compression.profiling.ThroughputAnalyzerResult;

public class ThroughputModel /* implements CompressionDecisionModel */{

	private ChannelType channelType = null;

	private DynamicCompressor dc = null;

	private int dynamicCompressorID = 0;

	private int nextEvaluationRound = 0;

	private static final int ROUNDS_WITHOUT_EVALUATION = 3;

	private static final int ROUNDS_UNTIL_RESET = 30;

	private int resetIncreaseFactor = 1;

	private static final int MAX_RESET_INCREASE_FACTOR = 3;

	private boolean evaluationPhase = true;

	private CompressionLevel nextEvaluationLevel = CompressionLevel.NO_COMPRESSION;

	private CompressionLevel lastCompressionDecision = null;

	private long noCompressionTime = -1;

	private long lightCompressionTime = -1;

	private long mediumCompressionTime = -1;

	private long mediumHeavyCompressionTime = -1;

	private long heavyCompressionTime = -1;

	private long lastCompressionTime = -1;

	public ThroughputModel(ChannelType type) {
		this.channelType = type;
	}

	// @Override
	public ChannelType getChannelType() {
		return this.channelType;
	}

	// @Override
	public CompressionLevel getCompressionLevelForNextBlock(int bufferLength, CompressionLevel lastCompressionLevel) {
		CompressionLevel next = lastCompressionLevel;
		if (nextEvaluationRound == 0) {

			if (evaluationPhase) {
				switch (nextEvaluationLevel) {
				case NO_COMPRESSION:
					nextEvaluationLevel = CompressionLevel.LIGHT_COMPRESSION;
					nextEvaluationRound = ROUNDS_WITHOUT_EVALUATION;
					next = CompressionLevel.NO_COMPRESSION;
					break;
				case LIGHT_COMPRESSION:
					nextEvaluationLevel = CompressionLevel.MEDIUM_COMPRESSION;
					nextEvaluationRound = ROUNDS_WITHOUT_EVALUATION;
					next = CompressionLevel.LIGHT_COMPRESSION;
					noCompressionTime = lastCompressionTime;
					break;
				case MEDIUM_COMPRESSION:
					nextEvaluationLevel = CompressionLevel.HEAVY_COMPRESSION;
					nextEvaluationRound = ROUNDS_WITHOUT_EVALUATION;
					next = CompressionLevel.HEAVY_COMPRESSION;
					lightCompressionTime = lastCompressionTime;
					break;
				case HEAVY_COMPRESSION:
					nextEvaluationLevel = CompressionLevel.NO_COMPRESSION;
					nextEvaluationRound = ROUNDS_WITHOUT_EVALUATION;
					next = CompressionLevel.HEAVY_COMPRESSION;
					evaluationPhase = false;
					mediumHeavyCompressionTime = lastCompressionTime;
					break;

				}
			} else {
				heavyCompressionTime = lastCompressionTime;

				// TODO: Fix me
				/*
				 * ThroughputAnalyzerResult tar =
				 * CompressionLoader.getAverageCommunicationTimeForCompressor(dynamicCompressorID);
				 * System.out.println("Average communication times: NOC: " + tar.getNoCompressionTime() + " LC: " +
				 * tar.getLightCompressionTime()
				 * + " MC: " + tar.getMediumCompressionTime() + " MHC: " + tar.getMediumHeavyCompressionTime() + " HC: "
				 * + tar.getHeavyCompressionTime());
				 * next = getLevelWithShortestCommunicationTime(tar);
				 * if (next == lastCompressionDecision){
				 * if (resetIncreaseFactor < MAX_RESET_INCREASE_FACTOR)
				 * resetIncreaseFactor++;
				 * }else{
				 * resetIncreaseFactor = 1;
				 * }
				 */

				this.lastCompressionDecision = next;
				nextEvaluationRound = ROUNDS_UNTIL_RESET * resetIncreaseFactor;
			}
		}

		nextEvaluationRound--;
		return next;
	}

	private CompressionLevel getLevelWithShortestCommunicationTime(ThroughputAnalyzerResult tar) {

		// TODO: Fix me
		/*
		 * long noTime = noCompressionTime;
		 * long lightTime = lightCompressionTime;
		 * long medTime = mediumCompressionTime;
		 * long medHTime = mediumHeavyCompressionTime;
		 * long heavyTime = heavyCompressionTime;
		 * if (tar.getNoCompressionTime() != -1)
		 * noTime += tar.getNoCompressionTime();
		 * else
		 * noTime = Long.MAX_VALUE;
		 * if (tar.getLightCompressionTime() != -1)
		 * lightTime += tar.getLightCompressionTime();
		 * else
		 * lightTime = Long.MAX_VALUE;
		 * if (tar.getMediumCompressionTime() != -1)
		 * medTime += tar.getMediumCompressionTime();
		 * else
		 * medTime = Long.MAX_VALUE;
		 * if (tar.getMediumHeavyCompressionTime() != -1)
		 * medHTime += tar.getMediumHeavyCompressionTime();
		 * else
		 * medHTime = Long.MAX_VALUE;
		 * if (tar.getHeavyCompressionTime() != -1)
		 * heavyTime += tar.getHeavyCompressionTime();
		 * else
		 * heavyTime = Long.MAX_VALUE;
		 * if (noTime <= lightTime && noTime <= medTime && noTime <= medHTime && noTime <= heavyTime)
		 * return CompressionLevel.NO_COMPRESSION;
		 * else if (lightTime <= medTime && lightTime <= medHTime && lightTime <= heavyTime)
		 * return CompressionLevel.LIGHT_COMPRESSION;
		 * else if (medTime <= medHTime && medTime <= heavyTime)
		 * return CompressionLevel.MEDIUM_COMPRESSION;
		 * else if (medHTime <= heavyTime)
		 * return CompressionLevel.MEDIUM_HEAVY_COMPRESSION;
		 * else
		 * return CompressionLevel.HEAVY_COMPRESION;
		 */

		return CompressionLevel.NO_COMPRESSION;
	}

	// @Override
	public void setCompressionRatio(double compressionRatio, long compressionTime) {
		this.lastCompressionTime = compressionTime;

	}

	// @Override
	public void setDynamicCompressor(DynamicCompressor dc) {
		this.dc = dc;
	}

}
