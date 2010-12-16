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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import eu.stratosphere.nephele.io.compression.CompressionLevel;

public class TrainingResults implements Serializable {

	private static final long serialVersionUID = 1L;

	private HashMap<CompressionLevel, AlgorithmResults> results = null;

	private FileInfo[] trainingFiles = null;

	private int numberOfDataBlocks = 0;

	private int numberOfRounds = 0;

	private int dataBlockSize = 0;

	private int numberOfTrainingFiles = 0;

	public TrainingResults(int numberOfDataBlocks, int numberOfRounds, int dataBlockSize, int numberOfTrainingFiles) {
		this.numberOfDataBlocks = numberOfDataBlocks;
		this.numberOfRounds = numberOfRounds;
		this.dataBlockSize = dataBlockSize;
		this.numberOfTrainingFiles = numberOfTrainingFiles;

		this.results = new HashMap<CompressionLevel, AlgorithmResults>();
		this.trainingFiles = new FileInfo[numberOfTrainingFiles];
	}

	public HashMap<CompressionLevel, AlgorithmResults> getResults() {
		return results;
	}

	public FileInfo[] getTrainingFiles() {
		return trainingFiles;
	}

	public int getNumberOfDataBlocks() {
		return numberOfDataBlocks;
	}

	public void setNumberOfDataBlocks(int numberOfDataBlocks) {
		this.numberOfDataBlocks = numberOfDataBlocks;
	}

	public int getNumberOfRounds() {
		return numberOfRounds;
	}

	public int getDataBlockSize() {
		return dataBlockSize;
	}

	public int getNumberOfTrainingFiles() {
		return numberOfTrainingFiles;
	}

	public FileInfo getFileInfo(int number) {
		if (number >= 0 && number < this.numberOfTrainingFiles)
			return trainingFiles[number];
		else {
			System.err.println("Number " + number + " for Trainingfile not valid!");
			return null;
		}
	}

	public void addFileInfo(int number, String fileName, int dataBlocks, long fileSize) {
		if (number >= 0 && number < this.numberOfTrainingFiles)
			if (trainingFiles[number] != null) {
				System.err.println("FileInfo for Trainingfile with number " + number + " already set!");
			} else {
				trainingFiles[number] = new FileInfo(fileName, dataBlocks, fileSize);
			}
		else {
			System.err.println("Number " + number + " for Trainingfile not valid!");
		}
	}

	public AlgorithmResults getAlgorithmResultsByCompressionLevel(CompressionLevel cl) {
		return results.get(cl);
	}

	public AlgorithmResults addAlgorithmResultsForCompressionLevel(CompressionLevel cl, String algorithmName) {
		if (!results.containsKey(cl)) {
			AlgorithmResults tmp = new AlgorithmResults(cl, algorithmName, this.numberOfDataBlocks, this.numberOfRounds);
			results.put(cl, tmp);
			return tmp;
		} else {
			System.err.println("AlgorithmResults for CompressionLevel " + cl + " already set!");
			return this.results.get(cl);
		}
	}

	public String toString() {
		StringBuffer result = new StringBuffer();
		result.append("********** Printing training results **************** \n");
		result.append("Used training files: \n");
		for (int i = 0; i < trainingFiles.length; i++)
			result.append(trainingFiles[i].toString());

		result.append("\n\n Algorithm-Results: \n");
		for (Iterator<CompressionLevel> i = this.results.keySet().iterator(); i.hasNext();)
			result.append(this.results.get(i.next()).toString());
		return result.toString();
	}

	public class AlgorithmResults implements Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public CompressionLevel algorithm = null;

		public String algorithmName = "";

		public double[] compressionRatios; // the Compression-Ratio (compressedSize/uncompressedSize) for each block of

		// data

		public long[][] compressionTimePerBlockPerRound; // the Compression-Time for each block of data and all rounds

		public long[] averageCompressionTimeOverRounds; // the average Compression-Time for each block of data

		// calculated over all rounds

		public long[][] decompressionTimePerBlockPerRound; // the Decompression-Time for each block of data and all

		// rounds

		public long[] averageDecompressionTimeOverRounds; // the average Decompression-Time for each block of data

		// calculated over all rounds

		public double[][] sysCpuloadPerBlockPerRoundCompression;

		public double[] averageSysCpuloadOverRoundsCompression;

		public double[][] sysCpuloadPerBlockPerRoundDecompression;

		public double[] averageSysCpuloadOverRoundsDecompression;

		public double[][] userCpuloadPerBlockPerRoundCompression;

		public double[] averageUserCpuloadOverRoundsCompression;

		public double[][] userCpuloadPerBlockPerRoundDecompression;

		public double[] averageUserCpuloadOverRoundsDecompression;

		public double[][] idleCpuPerBlockPerRoundCompression;

		public double[] averageIdleCpuOverRoundsCompression;

		public double[][] idleCpuPerBlockPerRoundDecompression;

		public double[] averageIdleCpuOverRoundsDecompression;

		public double[][] ioWaitCpuPerBlockPerRoundCompression;

		public double[] averageIOWaitCpuOverRoundsCompression;

		public double[][] ioWaitCpuPerBlockPerRoundDecompression;

		public double[] averageIOWaitCpuOverRoundsDecompression;

		public double averageCompressionRatio = 0.0; // the average Compression-Ratio over all data-blocks

		public double averageCompressionTime = 0.0; // the average Compression-Time over all data-blocks

		public double averageDecompressionTime = 0.0; // the average Decompression-Time over all data-blocks

		// coefficients for linear regression Y = a + bx to calculate (De)Compression-Time from Compression-Ratio
		public double cRToCTCoefficientA;

		public double cRToCTCoefficientB;

		public double cRToDTCoefficientA;

		public double cRToDTCoefficientB;

		private LinkedList<ComparedToResults> comparedTo;

		public AlgorithmResults(CompressionLevel cl, String algorithmName, int numberOfDataBlocks, int numberOfRounds) {
			this.comparedTo = new LinkedList<ComparedToResults>();
			this.compressionRatios = new double[numberOfDataBlocks];
			this.averageCompressionTimeOverRounds = new long[numberOfDataBlocks];
			this.averageDecompressionTimeOverRounds = new long[numberOfDataBlocks];
			this.averageSysCpuloadOverRoundsCompression = new double[numberOfDataBlocks];
			this.averageSysCpuloadOverRoundsDecompression = new double[numberOfDataBlocks];
			this.averageUserCpuloadOverRoundsCompression = new double[numberOfDataBlocks];
			this.averageUserCpuloadOverRoundsDecompression = new double[numberOfDataBlocks];
			this.averageIdleCpuOverRoundsCompression = new double[numberOfDataBlocks];
			this.averageIdleCpuOverRoundsDecompression = new double[numberOfDataBlocks];
			this.averageIOWaitCpuOverRoundsCompression = new double[numberOfDataBlocks];
			this.averageIOWaitCpuOverRoundsDecompression = new double[numberOfDataBlocks];

			this.compressionTimePerBlockPerRound = new long[numberOfDataBlocks][];
			this.decompressionTimePerBlockPerRound = new long[numberOfDataBlocks][];
			this.sysCpuloadPerBlockPerRoundCompression = new double[numberOfDataBlocks][];
			this.sysCpuloadPerBlockPerRoundDecompression = new double[numberOfDataBlocks][];
			this.userCpuloadPerBlockPerRoundCompression = new double[numberOfDataBlocks][];
			this.userCpuloadPerBlockPerRoundDecompression = new double[numberOfDataBlocks][];
			this.idleCpuPerBlockPerRoundCompression = new double[numberOfDataBlocks][];
			this.idleCpuPerBlockPerRoundDecompression = new double[numberOfDataBlocks][];
			this.ioWaitCpuPerBlockPerRoundCompression = new double[numberOfDataBlocks][];
			this.ioWaitCpuPerBlockPerRoundDecompression = new double[numberOfDataBlocks][];

			for (int i = 0; i < numberOfDataBlocks; i++) {
				this.compressionTimePerBlockPerRound[i] = new long[numberOfRounds];
				this.decompressionTimePerBlockPerRound[i] = new long[numberOfRounds];

				this.sysCpuloadPerBlockPerRoundCompression[i] = new double[numberOfDataBlocks];
				this.sysCpuloadPerBlockPerRoundDecompression[i] = new double[numberOfDataBlocks];
				this.userCpuloadPerBlockPerRoundCompression[i] = new double[numberOfDataBlocks];
				this.userCpuloadPerBlockPerRoundDecompression[i] = new double[numberOfDataBlocks];
				this.idleCpuPerBlockPerRoundCompression[i] = new double[numberOfDataBlocks];
				this.idleCpuPerBlockPerRoundDecompression[i] = new double[numberOfDataBlocks];
				this.ioWaitCpuPerBlockPerRoundCompression[i] = new double[numberOfDataBlocks];
				this.ioWaitCpuPerBlockPerRoundDecompression[i] = new double[numberOfDataBlocks];
			}

			this.algorithm = cl;
			this.algorithmName = algorithmName;
		}

		public ComparedToResults getCoefficentsForCompressionLevel(CompressionLevel cl) {
			for (Iterator<ComparedToResults> i = comparedTo.iterator(); i.hasNext();) {
				ComparedToResults tmp = i.next();
				if (tmp.comparedToalgorithm == cl)
					return tmp;
			}

			return null;
		}

		public void addCoefficentsForCompressionLevel(CompressionLevel cl, String algorithmName, double coefficientA,
				double coefficientB) {
			for (Iterator<ComparedToResults> i = comparedTo.iterator(); i.hasNext();) {
				ComparedToResults tmp = i.next();
				if (tmp.comparedToalgorithm == cl) {
					tmp.coefficientA = coefficientA;
					tmp.coefficientB = coefficientB;
					tmp.comparedToalgorithmName = algorithmName;
					return;
				}
			}

			ComparedToResults ctr = new ComparedToResults(cl, algorithmName);
			ctr.coefficientA = coefficientA;
			ctr.coefficientB = coefficientB;
			comparedTo.add(ctr);
		}

		public String toString() {
			StringBuffer result = new StringBuffer();
			result.append("Results for " + algorithmName + " (Level: " + algorithm + ") are: \n");
			result.append("Average CompressionRatio: " + averageCompressionRatio + "\n");
			result.append("Average CompressionTime: " + averageCompressionTime + "ns\n");
			result.append("Average DecompressionTime: " + averageDecompressionTime + "ns\n");
			result.append("CR to Ct: Y = " + cRToCTCoefficientA + " + " + cRToCTCoefficientB + "*x \n");
			result.append("CR to Dt: Y = " + cRToDTCoefficientA + " + " + cRToDTCoefficientB + "*x \n");
			result.append("Results compared to other algorithms: \n");
			for (Iterator<ComparedToResults> i = this.comparedTo.iterator(); i.hasNext();)
				result.append("     " + i.next().toString() + "\n");

			result.append("\n");
			return result.toString();
		}

		public class ComparedToResults implements Serializable {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public CompressionLevel comparedToalgorithm = null;

			public String comparedToalgorithmName = "";

			// coefficients for linear regression Y = a + bx
			// i.e.: compression-ratio-bzip2 = a + b*compression-ratio-lzo
			public double coefficientA;

			public double coefficientB;

			public ComparedToResults(CompressionLevel cl, String algorithmName) {
				this.comparedToalgorithm = cl;
				this.comparedToalgorithmName = algorithmName;
			}

			public String toString() {
				return "CompresssionRatio for " + comparedToalgorithmName + " (Level: " + comparedToalgorithm + ") "
					+ "is: Y= " + coefficientA + " + " + coefficientB + "*x";
			}

		}
	}

	public class FileInfo implements Serializable {

		private static final long serialVersionUID = 1L;

		public String fileName = "";

		public int dataBlocks = 0;

		public long fileSize = 0;

		private HashMap<CompressionLevel, FileInfoPerAlgorithm> algorithmResults;

		public FileInfo(String fileName, int dataBlocks, long fileSize) {
			this.fileName = fileName;
			this.dataBlocks = dataBlocks;
			this.fileSize = fileSize;

			this.algorithmResults = new HashMap<CompressionLevel, FileInfoPerAlgorithm>();
		}

		public FileInfoPerAlgorithm getAlgorithmResultsByCompressionLevel(CompressionLevel cl) {
			return algorithmResults.get(cl);
		}

		public void addAlgorithmResultsForCompressionLevel(CompressionLevel cl, String algorithmName,
				long compressionTime, long decompressionTime, long compressedFileSize,
				double averageCPULoadCompression, double averageCPULoadDecompression) {
			if (algorithmResults.containsKey(cl)) {
				FileInfoPerAlgorithm tmp = algorithmResults.get(cl);
				tmp.algorithmName = algorithmName;
				tmp.compressionTime = compressionTime;
				tmp.decompressionTime = decompressionTime;
				tmp.compressedFileSize = compressedFileSize;
				tmp.averageCPULoadCompression = averageCPULoadCompression;
				tmp.averageCPULoadDecompression = averageCPULoadDecompression;
			} else {
				FileInfoPerAlgorithm tmp = new FileInfoPerAlgorithm(cl, algorithmName);
				tmp.compressionTime = compressionTime;
				tmp.decompressionTime = decompressionTime;
				tmp.compressedFileSize = compressedFileSize;
				tmp.averageCPULoadCompression = averageCPULoadCompression;
				tmp.averageCPULoadDecompression = averageCPULoadDecompression;

				algorithmResults.put(cl, tmp);
			}
		}

		public String toString() {
			StringBuffer result = new StringBuffer();
			result.append("Filename: " + fileName + " Filesize: " + fileSize + " (Number of DataBlocks: " + dataBlocks
				+ " ) \n Details:\n");
			for (Iterator<CompressionLevel> i = algorithmResults.keySet().iterator(); i.hasNext();) {
				result.append("     " + algorithmResults.get(i.next()).toString());
			}
			return result.toString();
		}

		public class FileInfoPerAlgorithm implements Serializable {

			private static final long serialVersionUID = 1L;

			public CompressionLevel algorithm;

			public String algorithmName;

			public long compressionTime = 0;

			public long decompressionTime = 0;

			public long compressedFileSize = 0;

			public double averageCPULoadCompression = 0.0;

			public double averageCPULoadDecompression = 0.0;

			public FileInfoPerAlgorithm(CompressionLevel cl, String algorithmName) {
				this.algorithmName = algorithmName;
				this.algorithm = cl;
			}

			public String toString() {
				return "Algorithm: " + algorithmName + " (Level: " + algorithm + ") - " + "CompressedSize: "
					+ compressedFileSize + " Compression-Time: " + compressionTime + " DecompressionTime "
					+ decompressionTime + " \n";
			}

		}

	}
}
