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

package eu.stratosphere.nephele.io.compression.dynamic.trainedModel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.compression.CompressionLoader;
import eu.stratosphere.nephele.io.compression.dynamic.trainedModel.TrainingResults.AlgorithmResults;
import eu.stratosphere.nephele.io.compression.dynamic.trainedModel.TrainingResults.AlgorithmResults.ComparedToResults;
import eu.stratosphere.nephele.io.compression.library.dynamic.DynamicCompressor;

public class TrainedModel /* implements CompressionDecisionModel */{

	private static final Log LOG = LogFactory.getLog(TrainedModel.class);

	private double lastCompressionRatio = 1.0;

	private CompressionLevel lastCompressionLevel = CompressionLevel.NO_COMPRESSION;

	private CompressionLevel lastDecision = null;

	private int nextEvaluationRound = 0;

	private static final int ROUNDS_WITHOUT_EVALUATION = 3;

	private static final int MAX_EVALUATION_INCREASE_FACTOR = 10;

	private int evaluationIncreaseFactor = 1;

	private ChannelType type;

	private DynamicCompressor dc;

	private TrainingResults trainingResults = null;

	private double bandwidth = 0;

	public TrainedModel(TrainingResults trainingResults, ChannelType type) {
		this.trainingResults = trainingResults;
		this.type = type;
		// bandwidth = CompressionLoader.getCurrentBandwidthInBytesPerNS(type); //TODO: Fix me
	}

	// @Override
	public CompressionLevel getCompressionLevelForNextBlock(int bufferLength, CompressionLevel lastCompressionLevel) {
		CompressionLevel next = lastCompressionLevel;

		if (nextEvaluationRound == 0) {
			if (trainingResults == null) {
				nextEvaluationRound += Integer.MAX_VALUE;
				return CompressionLevel.LIGHT_COMPRESSION;
			}

			if (lastCompressionLevel == CompressionLevel.NO_COMPRESSION) {
				return CompressionLevel.LIGHT_COMPRESSION;
			} else {
				this.lastCompressionLevel = lastCompressionLevel;
				long noTime = computeCompressionTime(CompressionLevel.NO_COMPRESSION, bufferLength);
				long lightTime = computeCompressionTime(CompressionLevel.LIGHT_COMPRESSION, bufferLength);
				long medTime = computeCompressionTime(CompressionLevel.MEDIUM_COMPRESSION, bufferLength);
				long heavyTime = computeCompressionTime(CompressionLevel.HEAVY_COMPRESSION, bufferLength);

				System.out.println("Predicted Transfer Time: NOC: " + noTime + " LC: " + lightTime + " MC: " + medTime
					+ " HC: " + heavyTime);

				if (noTime < lightTime && noTime < medTime && noTime < heavyTime) {
					next = CompressionLevel.NO_COMPRESSION;
				} else if (lightTime < medTime && lightTime < heavyTime) {
					next = CompressionLevel.LIGHT_COMPRESSION;
				} else if (medTime < heavyTime) {
					next = CompressionLevel.MEDIUM_COMPRESSION;
				} else {
					next = CompressionLevel.HEAVY_COMPRESSION;
				}

				if (next == lastDecision) {
					if (evaluationIncreaseFactor < MAX_EVALUATION_INCREASE_FACTOR)
						evaluationIncreaseFactor++;
				} else {
					evaluationIncreaseFactor = 1;
				}

				lastDecision = next;
				nextEvaluationRound = ROUNDS_WITHOUT_EVALUATION * evaluationIncreaseFactor;
			}
		}

		nextEvaluationRound--;
		return next;

	}

	// @Override
	public void setCompressionRatio(double compressionRatio, long compressionTime) {
		lastCompressionRatio = compressionRatio;

	}

	private long computeCompressionTime(CompressionLevel cl, int bufferLength) {
		double ratio = 0.0;
		long cTime = 0;
		long dTime = 0;
		switch (cl) {
		case NO_COMPRESSION:
			return (long) ((double) bufferLength / bandwidth) + (2 * ((bufferLength * 1000000) / 50000000));// arraycopy
			// perfromance
			// of 50
			// MB/s
		case LIGHT_COMPRESSION:
		case MEDIUM_COMPRESSION:
		case HEAVY_COMPRESSION:
			ratio = computeCompressionRatio(cl);
			if (ratio <= 0)
				ratio = 0.0001;
			AlgorithmResults ar = trainingResults.getAlgorithmResultsByCompressionLevel(cl);
			if (ar == null)
				return -1;
			else {
				cTime = (long) (ar.cRToCTCoefficientA + (ar.cRToCTCoefficientB * ratio));
				dTime = (long) (ar.cRToDTCoefficientA + (ar.cRToDTCoefficientB * ratio));
				System.out.println(ar.algorithmName + " Ratio: " + ratio + " Compression-Time: " + cTime
					+ " Decompression-Time: " + dTime);
			}
			break;
		default:
			return -1;

		}

		// return (long)((bufferLength*((((double)bufferLength*ratio)/(double)bandwidth) + cTime +
		// dTime))/trainingResults.COMPARING_BLOCK_SIZE);
		return (long) ((((double) bufferLength * ratio) / (double) bandwidth) + ((bufferLength * (cTime + dTime)) / trainingResults
			.getDataBlockSize()));

	}

	private double computeCompressionRatio(CompressionLevel cl) {
		if (trainingResults == null)
			return -1;

		switch (lastCompressionLevel) {
		case NO_COMPRESSION:
			return -1;
		default: {
			if (cl == CompressionLevel.NO_COMPRESSION)
				return 1;

			AlgorithmResults ar = this.trainingResults.getAlgorithmResultsByCompressionLevel(this.lastCompressionLevel);
			if (ar != null) {
				ComparedToResults ctr = ar.getCoefficentsForCompressionLevel(cl);
				if (ctr != null) {
					return ctr.coefficientA + (ctr.coefficientB * lastCompressionRatio);
				}
			}

			return -1;
		}
		}

	}

	// @Override
	public ChannelType getChannelType() {
		return this.type;
	}

	// @Override
	public void setDynamicCompressor(DynamicCompressor dc) {
		this.dc = dc;

	}

}
