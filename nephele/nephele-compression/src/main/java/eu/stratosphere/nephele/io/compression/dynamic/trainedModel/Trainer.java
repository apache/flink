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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;

import javax.swing.JProgressBar;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

//import eu.stratosphere.nephele.profiling.impl.InstanceProfiler;
//import eu.stratosphere.nephele.profiling.impl.types.InternalInstanceProfilingData;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.compression.CompressionLibrary;
import eu.stratosphere.nephele.io.compression.CompressionLoader;
import eu.stratosphere.nephele.io.compression.Compressor;
import eu.stratosphere.nephele.io.compression.Decompressor;
import eu.stratosphere.nephele.io.compression.dynamic.trainedModel.TrainingResults.AlgorithmResults;
import eu.stratosphere.nephele.io.compression.dynamic.trainedModel.TrainingResults.FileInfo;

public class Trainer implements Runnable {

	@SuppressWarnings("static-access")
	public static void main(String[] args) {
		Option configDirOpt = OptionBuilder.withArgName("config directory").hasArg().withDescription(
			"Specify configuration directory.").create("configDir");

		Option trainingDirOpt = OptionBuilder.withArgName("training-file directory").hasArg().withDescription(
			"Specify Training-File directory.").create("trainingFileDir");

		Option blockSizeOpt = OptionBuilder.withArgName("data-block size").hasArg().withDescription(
			"Specify the Block Size to be used in KB.").create("blockSize");

		Option roundsOpt = OptionBuilder.withArgName("compression rounds").hasArg().withDescription(
			"Specify the number of training-rounds.").create("rounds");

		Option outputOpt = OptionBuilder.withArgName("output file").hasArg().withDescription(
			"Specify the output file where the results will be saved.").create("outputFile");

		Options options = new Options();
		options.addOption(configDirOpt);
		options.addOption(trainingDirOpt);
		options.addOption(blockSizeOpt);
		options.addOption(roundsOpt);
		options.addOption(outputOpt);

		HelpFormatter help = new HelpFormatter();

		CommandLineParser parser = new GnuParser();
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("CLI Parsing failed. Reason: " + e.getMessage());
			help.printHelp("Trainer.java [OPTION]", "", options, "");
			System.exit(-1);
		}

		String configDir = line.getOptionValue(configDirOpt.getOpt(), null);
		String trainingDir = line.getOptionValue(trainingDirOpt.getOpt(), null);
		String blockSize = line.getOptionValue(blockSizeOpt.getOpt(), "128");
		String rounds = line.getOptionValue(roundsOpt.getOpt(), "10");
		String outputFile = line.getOptionValue(outputOpt.getOpt(), null);

		if (configDir == null || trainingDir == null) {
			help.printHelp("Trainer.java [OPTION]", "Missing Option - configDir and trainingFileDir must be set!",
				options, "");
			System.exit(-1);
		}

		int blockS = 128;
		int round = 0;
		try {
			blockS = Integer.parseInt(blockSize);
			round = Integer.parseInt(rounds);

			if (round <= 0 || blockS <= 0) {
				help.printHelp("Trainer.java [OPTION]",
					"Blocksize and Rounds parameter need to be a positive integer.", options, "");
				System.exit(-1);
			}
		} catch (NumberFormatException e) {
			help.printHelp("Trainer.java [OPTION]", "Blocksize and Rounds parameter need to be a positive integer.",
				options, "");
			System.exit(-1);
		}

		GlobalConfiguration.loadConfiguration(configDir);
		Trainer tr = new Trainer(blockS * 1024, round, "file://" + trainingDir);
		if (outputFile != null)
			tr.setResultFile(outputFile);

		tr.startTraining();
	}

	private int numberOfDataBlocks = 0;

	private int numberOfRounds = 0;

	private int dataBlockSize = 0;

	private TrainingResults results = null;

	private Path pathToTrainingFiles = null;

	private HashMap<CompressionLevel, Compressor> compressors = null;

	private HashMap<CompressionLevel, Decompressor> decompressors = null;

	private HashMap<CompressionLevel, CompressionLibrary> libraries = null;

	Buffer uncompressed = null;

	Buffer uncompressed2 = null;

	Buffer compressed = null;

	private JTextArea output = null;

	private JProgressBar progress = null;

	private boolean isTrainingDone = false;

	private boolean saveResultsToFile = false;

	private File resultFile = null;

	// private static InstanceProfiler profiler = null;

	public Trainer(int dataBlockSize, int numberOfRounds, String pathToTrainingFiles) {
		this.numberOfRounds = numberOfRounds;
		this.dataBlockSize = dataBlockSize;
		this.pathToTrainingFiles = new Path(pathToTrainingFiles);

		compressors = new HashMap<CompressionLevel, Compressor>();
		decompressors = new HashMap<CompressionLevel, Decompressor>();
		libraries = new HashMap<CompressionLevel, CompressionLibrary>();

		/*
		 * this.uncompressed = BufferFactory.createFromMemory(dataBlockSize, ByteBuffer.allocateDirect(dataBlockSize),
		 * null);
		 * this.uncompressed2 = BufferFactory.createFromMemory(dataBlockSize, ByteBuffer.allocateDirect(dataBlockSize),
		 * null);
		 * this.compressed = BufferFactory.createFromMemory(dataBlockSize + (dataBlockSize/2),
		 * ByteBuffer.allocateDirect(dataBlockSize + (dataBlockSize/2)), null);
		 */
		try {
			// Class<? extends CompressionInstanceProfiler> clazz = (Class<? extends CompressionInstanceProfiler>)
			// Class.forName("eu.stratosphere.nephele.profiling.impl.InstanceProfiler");

			// Constructor<? extends CompressionInstanceProfiler> constructor =
			// clazz.getConstructor(InstanceConnectionInfo.class);
			// profiler = new InstanceProfiler(new InstanceConnectionInfo());//constructor.newInstance(new
			// InstanceConnectionInfo());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Trainer(int dataBlockSize, int numberOfRounds, String pathToTrainingFiles, JTextArea output,
			JProgressBar progress) {
		this.numberOfRounds = numberOfRounds;
		this.dataBlockSize = dataBlockSize;
		this.pathToTrainingFiles = new Path(pathToTrainingFiles);

		compressors = new HashMap<CompressionLevel, Compressor>();
		decompressors = new HashMap<CompressionLevel, Decompressor>();
		libraries = new HashMap<CompressionLevel, CompressionLibrary>();

		/*
		 * this.uncompressed = BufferFactory.createFromMemory(dataBlockSize, ByteBuffer.allocateDirect(dataBlockSize),
		 * null);
		 * this.uncompressed2 = BufferFactory.createFromMemory(dataBlockSize, ByteBuffer.allocateDirect(dataBlockSize),
		 * null);
		 * this.compressed = BufferFactory.createFromMemory(dataBlockSize + (dataBlockSize/2),
		 * ByteBuffer.allocateDirect(dataBlockSize + (dataBlockSize/2)), null);
		 */
		this.output = output;
		this.progress = progress;

		try {
			// Class<? extends CompressionInstanceProfiler> clazz = (Class<? extends CompressionInstanceProfiler>)
			// Class.forName("eu.stratosphere.nephele.profiling.impl.InstanceProfiler");

			// Constructor<? extends CompressionInstanceProfiler> constructor =
			// clazz.getConstructor(InstanceConnectionInfo.class);
			// profiler = new InstanceProfiler(new InstanceConnectionInfo());//constructor.newInstance(new
			// InstanceConnectionInfo());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void startTraining() {
		print("Starting Training with " + this.numberOfRounds + " rounds and " + this.dataBlockSize
			+ " KB BlockSize  ....", true);

		print("Init Compression Libraries ... ", false);
		if (!initCompression()) {
			return;
		}
		print("Done!", true);

		print("Init Training-Files ... ", false);
		if (!initTrainingFiles()) {
			return;
		}
		print("Done!", true);

		print("Start computing training data ... ", true);
		for (Iterator<CompressionLevel> i = compressors.keySet().iterator(); i.hasNext();) {
			CompressionLevel tmp = i.next();

			print("     Computing training data for Compression-Library " + getAlgorithmNameByCompressionLevel(tmp)
				+ " (Level: " + tmp + ") ... ", false);
			if (computeTrainingData(tmp, this.compressors.get(tmp), this.decompressors.get(tmp)))
				print("Done!", true);
			else
				print("An error occured during computation for Compression-Library "
					+ getAlgorithmNameByCompressionLevel(tmp) + " (Level: " + tmp + ")", true);
		}
		print("Computing training data done!", true);

		print("Start computing linear regression ... ", true);
		for (CompressionLevel cl : CompressionLevel.values()) {
			if (compressors.containsKey(cl)) {
				AlgorithmResults ar1 = this.results.getAlgorithmResultsByCompressionLevel(cl);
				if (ar1 != null) {

					computeLinearRegressionForTime(ar1);

					for (Iterator<CompressionLevel> j = compressors.keySet().iterator(); j.hasNext();) {
						CompressionLevel cl2 = j.next();
						if (cl != cl2) {
							AlgorithmResults ar2 = this.results.getAlgorithmResultsByCompressionLevel(cl2);
							if (ar2 != null)
								computeLinearRegressionForRatio(ar1, ar2);
						}
					}
				}
			}

		}
		print("Done!", true);
		this.isTrainingDone = true;

		if (output == null)
			System.out.println(this.results.toString());

		if (saveResultsToFile) {
			FileOutputStream fos;
			ObjectOutputStream oos;
			try {
				fos = new FileOutputStream(resultFile);
				oos = new ObjectOutputStream(fos);
				oos.writeObject(results);
				oos.close();
				print("Results save to: " + resultFile.getAbsolutePath(), true);
			} catch (FileNotFoundException e1) {
				print("ERROR: Could not save results.", true);
				e1.printStackTrace();
			} catch (IOException e2) {
				print("ERROR: Could not save results.", true);
				e2.printStackTrace();
			}
		}
	}

	public void setResultFile(String pathToResultFile) {
		File f = new File(pathToResultFile);
		if (!f.exists()) {
			try {
				if (!f.createNewFile()) {
					print("ERROR: Could not create file to save results. Can not save results! Path: ("
						+ pathToResultFile + ")", true);
				} else {
					this.resultFile = f;
					this.saveResultsToFile = true;
				}
			} catch (IOException e) {
				print("ERROR: Could not create file to save results. Can not save results! Path: (" + pathToResultFile
					+ ")", true);
				e.printStackTrace();
			}
		} else {
			print("ERROR: Given path to save results already exists. Can not save results! Path: (" + pathToResultFile
				+ ")", true);
		}
	}

	private boolean initCompression() {
		CompressionLoader.init();

		Compressor ctmp = null;
		Decompressor dtmp = null;
		CompressionLibrary cl = null;

		// light
		try {
			cl = CompressionLoader.getCompressionLibraryByCompressionLevel(CompressionLevel.LIGHT_COMPRESSION);
			ctmp = cl.getCompressor();
			dtmp = cl.getDecompressor();

			if (ctmp != null && dtmp != null) {
				this.compressors.put(CompressionLevel.LIGHT_COMPRESSION, ctmp);
				this.decompressors.put(CompressionLevel.LIGHT_COMPRESSION, dtmp);
				this.libraries.put(CompressionLevel.LIGHT_COMPRESSION, cl);
			}
		} catch (CompressionException e) {
			print("WARNING: Could not load light-compression-library", true);
			e.printStackTrace();
		}

		// medium
		try {
			cl = CompressionLoader.getCompressionLibraryByCompressionLevel(CompressionLevel.MEDIUM_COMPRESSION);
			ctmp = cl.getCompressor();
			dtmp = cl.getDecompressor();

			if (ctmp != null && dtmp != null) {
				this.compressors.put(CompressionLevel.MEDIUM_COMPRESSION, ctmp);
				this.decompressors.put(CompressionLevel.MEDIUM_COMPRESSION, dtmp);
				this.libraries.put(CompressionLevel.MEDIUM_COMPRESSION, cl);
			}
		} catch (CompressionException e) {
			print("WARNING: Could not load medium-compression-library", true);
			e.printStackTrace();
		}

		// heavy
		try {
			cl = CompressionLoader.getCompressionLibraryByCompressionLevel(CompressionLevel.HEAVY_COMPRESSION);
			ctmp = cl.getCompressor();
			dtmp = cl.getDecompressor();

			if (ctmp != null && dtmp != null) {
				this.compressors.put(CompressionLevel.HEAVY_COMPRESSION, ctmp);
				this.decompressors.put(CompressionLevel.HEAVY_COMPRESSION, dtmp);
				this.libraries.put(CompressionLevel.HEAVY_COMPRESSION, cl);
			}
		} catch (CompressionException e) {
			print("WARNING: Could not load heavy-compression-library", true);
			e.printStackTrace();
		}

		if (!this.compressors.containsKey(CompressionLevel.LIGHT_COMPRESSION)
			&& !this.compressors.containsKey(CompressionLevel.MEDIUM_COMPRESSION)
			&& !this.compressors.containsKey(CompressionLevel.HEAVY_COMPRESSION)) {

			print("ERROR: Can not compute Training - No Compression-Library could be loaded!", true);
			return false;
		}

		return true;
	}

	private boolean initTrainingFiles() {
		try {
			FileSystem fs = pathToTrainingFiles.getFileSystem();
			final FileStatus file = fs.getFileStatus(pathToTrainingFiles);

			if (!file.isDir()) {
				print("ERROR: Can not compute Training - Path to test files is not valid!", true);
			} else {
				final FileStatus[] dir = fs.listStatus(pathToTrainingFiles);

				// first count number of trainingFiles
				int numberOfFiles = 0;
				for (int i = 0; i < dir.length; i++) {
					if (!dir[i].isDir())
						numberOfFiles++;
				}

				if (numberOfFiles == 0) {
					print("ERROR: Can not compute Training - No files found in given Directory!", true);
					return false;
				}

				// init the resultClass - we need to set the correct number of dataBlocks later!!
				this.results = new TrainingResults(0, numberOfRounds, dataBlockSize, numberOfFiles);

				// now init the FileInfo and count number of DataBlocks
				byte[] tmpBuffer = new byte[this.dataBlockSize];
				int fileCounter = 0;
				int blockCounter = 0;
				long fileSize = 0;
				for (int i = 0; i < dir.length; i++) {
					if (!dir[i].isDir()) {

						FSDataInputStream fdis = fs.open(dir[i].getPath());
						int readBytes = 0;
						while ((readBytes = fdis.read(tmpBuffer, 0, dataBlockSize)) != -1) {
							blockCounter++;
							numberOfDataBlocks++;
							fileSize += readBytes;
						}
						fdis.close();

						results.addFileInfo(fileCounter, dir[i].getPath().getName(), blockCounter, fileSize);
						fileCounter++;
						blockCounter = 0;
						fileSize = 0;

					}
				}
				this.results.setNumberOfDataBlocks(numberOfDataBlocks);

				if (progress != null) {
					SwingUtilities.invokeLater(new Runnable() {
						@Override
						public void run() {
							progress.setMaximum(numberOfDataBlocks * compressors.size());
							progress.setValue(0);
						}
					});
				}

			}
		} catch (IOException e) {
			print("ERROR: An Error occured during initialization of TrainingFiles!", true);
			e.printStackTrace();
			return false;
		}

		return true;
	}

	private boolean computeTrainingData(CompressionLevel cl, Compressor compressor, Decompressor decompressor) {
		try {

			AlgorithmResults ar = this.results.addAlgorithmResultsForCompressionLevel(cl,
				getAlgorithmNameByCompressionLevel(cl));
			FileSystem fs = pathToTrainingFiles.getFileSystem();
			final FileStatus[] dir = fs.listStatus(pathToTrainingFiles);

			byte[] tmpBuffer = new byte[this.dataBlockSize];
			ByteBuffer tmpByteBuffer = ByteBuffer.allocate(dataBlockSize);
			int fileCounter = 0;
			int blockCounterSum = 0;
			int blockcounterPerFile = 0;

			// iterate over all trainingFiles
			for (int i = 0; i < dir.length; i++) {
				if (!dir[i].isDir()) {
					FileInfo fi = this.results.getFileInfo(fileCounter);
					long compressionTimePerFile = 0;
					long decompressionTimePerFile = 0;
					long compressedFileSize = 0;

					// compress/decompress the training file numberOfRounds times
					for (int r = 0; r < this.numberOfRounds; r++) {
						blockcounterPerFile = 0;
						FSDataInputStream fdis = fs.open(dir[i].getPath());

						int readBytes = 0;
						// read dataBlockSize bytes into our buffer
						while ((readBytes = fdis.read(tmpBuffer, 0, dataBlockSize)) != -1) {

							tmpByteBuffer.clear();
							tmpByteBuffer.put(tmpBuffer, 0, readBytes);
							tmpByteBuffer.flip();
							// this.uncompressed.put(tmpBuffer, 0, readBytes);

							this.uncompressed = BufferFactory.createFromMemory(dataBlockSize, ByteBuffer
								.allocateDirect(dataBlockSize), null);
							this.uncompressed2 = BufferFactory.createFromMemory(dataBlockSize, ByteBuffer
								.allocateDirect(dataBlockSize), null);
							this.compressed = BufferFactory.createFromMemory(dataBlockSize + (dataBlockSize / 2),
								ByteBuffer.allocateDirect(dataBlockSize + (dataBlockSize / 2)), null);

							this.uncompressed.write(tmpByteBuffer);
							// this.uncompressed.finishWritePhase();

							// TODO: Fix me
							// compress the buffer
							compressor.setUncompressedDataBuffer(uncompressed);
							compressor.setCompressedDataBuffer(compressed);
							long startTime = System.nanoTime();
							compressor.compress();
							long endTime = System.nanoTime();
							compressed.finishWritePhase();
							int compressedBytes = this.compressed.size();

							// TODO: Fix me
							/*
							 * InternalInstanceProfilingData profilingData = null;
							 * try {
							 * profilingData = profiler.generateProfilingData(System.currentTimeMillis());
							 * } catch (ProfilingException e) {
							 * // TODO Auto-generated catch block
							 * e.printStackTrace();
							 * }
							 */

							// record time
							long time = Math.abs(endTime) - Math.abs(startTime);
							ar.compressionTimePerBlockPerRound[blockCounterSum + blockcounterPerFile][r] = time;

							// TODO: Fix me
							// record cpu-usage
							/*
							 * ar.sysCpuloadPerBlockPerRoundCompression[blockCounterSum + blockcounterPerFile][r] =
							 * profilingData.getSystemCPU();
							 * ar.userCpuloadPerBlockPerRoundCompression[blockCounterSum + blockcounterPerFile][r] =
							 * profilingData.getUserCPU();
							 * ar.idleCpuPerBlockPerRoundCompression[blockCounterSum + blockcounterPerFile][r] =
							 * profilingData.getIdleCPU();
							 * ar.ioWaitCpuPerBlockPerRoundCompression[blockCounterSum + blockcounterPerFile][r] =
							 * profilingData.getIOWaitCPU();
							 */
							compressionTimePerFile += time;

							if (r == 0) {
								compressedFileSize += compressedBytes;
								ar.compressionRatios[blockCounterSum + blockcounterPerFile] = (double) compressedBytes
									/ (double) readBytes;
								ar.averageCompressionRatio += ar.compressionRatios[blockCounterSum
									+ blockcounterPerFile];
							}

							// decompress the buffer
							decompressor.setCompressedDataBuffer(compressed);
							decompressor.setUncompressedDataBuffer(uncompressed2);
							startTime = System.nanoTime();
							decompressor.decompress();
							endTime = System.nanoTime();
							// TODO: Fix me
							/*
							 * try {
							 * profilingData = profiler.generateProfilingData(System.currentTimeMillis());
							 * } catch (ProfilingException e) {
							 * // TODO Auto-generated catch block
							 * e.printStackTrace();
							 * }
							 */

							// record time
							time = Math.abs(endTime) - Math.abs(startTime);
							decompressionTimePerFile += time;
							ar.decompressionTimePerBlockPerRound[blockCounterSum + blockcounterPerFile][r] = time;

							// record cpu-usage
							// TODO: Fix me
							/*
							 * ar.sysCpuloadPerBlockPerRoundDecompression[blockCounterSum + blockcounterPerFile][r] =
							 * profilingData.getSystemCPU();
							 * ar.userCpuloadPerBlockPerRoundDecompression[blockCounterSum + blockcounterPerFile][r] =
							 * profilingData.getUserCPU();
							 * ar.idleCpuPerBlockPerRoundDecompression[blockCounterSum + blockcounterPerFile][r] =
							 * profilingData.getIdleCPU();
							 * ar.ioWaitCpuPerBlockPerRoundDecompression[blockCounterSum + blockcounterPerFile][r] =
							 * profilingData.getIOWaitCPU();
							 */

							if (this.uncompressed2.size() != readBytes)
								System.err.println("Error " + ar.algorithmName + " Expected " + readBytes
									+ " Bytes - got " + this.uncompressed2.size() + " Bytes");

							ar.averageCompressionTimeOverRounds[blockCounterSum + blockcounterPerFile] += ar.compressionTimePerBlockPerRound[blockCounterSum
								+ blockcounterPerFile][r];
							ar.averageDecompressionTimeOverRounds[blockCounterSum + blockcounterPerFile] += ar.decompressionTimePerBlockPerRound[blockCounterSum
								+ blockcounterPerFile][r];

							ar.averageSysCpuloadOverRoundsCompression[blockCounterSum + blockcounterPerFile] += ar.sysCpuloadPerBlockPerRoundCompression[blockCounterSum
								+ blockcounterPerFile][r];
							ar.averageSysCpuloadOverRoundsDecompression[blockCounterSum + blockcounterPerFile] += ar.sysCpuloadPerBlockPerRoundDecompression[blockCounterSum
								+ blockcounterPerFile][r];
							ar.averageUserCpuloadOverRoundsCompression[blockCounterSum + blockcounterPerFile] += ar.userCpuloadPerBlockPerRoundCompression[blockCounterSum
								+ blockcounterPerFile][r];
							ar.averageUserCpuloadOverRoundsDecompression[blockCounterSum + blockcounterPerFile] += ar.userCpuloadPerBlockPerRoundDecompression[blockCounterSum
								+ blockcounterPerFile][r];
							ar.averageIdleCpuOverRoundsCompression[blockCounterSum + blockcounterPerFile] += ar.idleCpuPerBlockPerRoundCompression[blockCounterSum
								+ blockcounterPerFile][r];
							ar.averageIdleCpuOverRoundsDecompression[blockCounterSum + blockcounterPerFile] += ar.idleCpuPerBlockPerRoundDecompression[blockCounterSum
								+ blockcounterPerFile][r];
							ar.averageIOWaitCpuOverRoundsCompression[blockCounterSum + blockcounterPerFile] += ar.ioWaitCpuPerBlockPerRoundCompression[blockCounterSum
								+ blockcounterPerFile][r];
							ar.averageIOWaitCpuOverRoundsDecompression[blockCounterSum + blockcounterPerFile] += ar.ioWaitCpuPerBlockPerRoundDecompression[blockCounterSum
								+ blockcounterPerFile][r];

							blockcounterPerFile++;

						}// finish one round of one file

						fdis.close();
					}// finish all rounds of one file

					double averageCPULoadCompression = 0.0, averageCPULoadDecompression = 0.0;
					for (int b = blockCounterSum; b < (blockCounterSum + blockcounterPerFile); b++) {
						ar.averageCompressionTimeOverRounds[b] /= this.numberOfRounds;
						ar.averageDecompressionTimeOverRounds[b] /= this.numberOfRounds;
						ar.averageSysCpuloadOverRoundsCompression[b] /= (double) this.numberOfRounds;
						ar.averageSysCpuloadOverRoundsDecompression[b] /= (double) this.numberOfRounds;
						ar.averageUserCpuloadOverRoundsCompression[b] /= (double) this.numberOfRounds;
						ar.averageUserCpuloadOverRoundsDecompression[b] /= (double) this.numberOfRounds;
						ar.averageIdleCpuOverRoundsCompression[b] /= (double) this.numberOfRounds;
						ar.averageIdleCpuOverRoundsDecompression[b] /= (double) this.numberOfRounds;
						ar.averageIOWaitCpuOverRoundsCompression[b] /= (double) this.numberOfRounds;
						ar.averageIOWaitCpuOverRoundsDecompression[b] /= (double) this.numberOfRounds;

						averageCPULoadCompression += (ar.averageSysCpuloadOverRoundsCompression[b] + ar.averageUserCpuloadOverRoundsCompression[b]);
						averageCPULoadDecompression += (ar.averageSysCpuloadOverRoundsDecompression[b] + ar.averageUserCpuloadOverRoundsDecompression[b]);

						ar.averageCompressionTime += ar.averageCompressionTimeOverRounds[b];
						ar.averageDecompressionTime += ar.averageDecompressionTimeOverRounds[b];

					}

					averageCPULoadCompression /= (double) blockcounterPerFile;
					averageCPULoadDecompression /= (double) blockcounterPerFile;

					blockCounterSum += blockcounterPerFile;

					fi.addAlgorithmResultsForCompressionLevel(cl, getAlgorithmNameByCompressionLevel(cl),
						compressionTimePerFile / numberOfRounds, decompressionTimePerFile / numberOfRounds,
						compressedFileSize, averageCPULoadCompression, averageCPULoadDecompression);
					fileCounter++;

					if (progress != null) {
						final int blocks = progress.getValue() + fi.dataBlocks;
						SwingUtilities.invokeLater(new Runnable() {
							@Override
							public void run() {
								progress.setValue(blocks);
							}
						});
					}
				}
			}// finish all rounds of all file

			ar.averageCompressionRatio /= (double) this.numberOfDataBlocks;
			ar.averageCompressionTime /= (double) this.numberOfDataBlocks;
			ar.averageDecompressionTime /= (double) this.numberOfDataBlocks;

		} catch (IOException e) {
			print("An Error occured during computation of TrainingData!", true);
			e.printStackTrace();
		}
		return true;
	}

	private boolean computeLinearRegressionForRatio(AlgorithmResults ar1, AlgorithmResults ar2) {
		double ratioAr1ToRatioAr1 = 0.0, ratioAr1ToRatioAr2 = 0.0;

		for (int i = 0; i < this.numberOfDataBlocks; i++) {
			ratioAr1ToRatioAr1 += (ar1.compressionRatios[i] - ar1.averageCompressionRatio)
				* (ar1.compressionRatios[i] - ar1.averageCompressionRatio);
			ratioAr1ToRatioAr2 += (ar1.compressionRatios[i] - ar1.averageCompressionRatio)
				* (ar2.compressionRatios[i] - ar2.averageCompressionRatio);
		}

		double coefficientB = ratioAr1ToRatioAr2 / ratioAr1ToRatioAr1;
		double coefficientA = ar2.averageCompressionRatio - (coefficientB * ar1.averageCompressionRatio);

		ar1.addCoefficentsForCompressionLevel(ar2.algorithm, ar2.algorithmName, coefficientA, coefficientB);

		return true;
	}

	private boolean computeLinearRegressionForTime(AlgorithmResults ar1) {
		double ratioToCompressionTime = 0.0, ratioToDecompressionTime = 0.0, ratioToRatio = 0.0;

		for (int i = 0; i < this.numberOfDataBlocks; i++) {

			ratioToRatio += (ar1.compressionRatios[i] - ar1.averageCompressionRatio)
				* (ar1.compressionRatios[i] - ar1.averageCompressionRatio);
			ratioToCompressionTime += (ar1.compressionRatios[i] - ar1.averageCompressionRatio)
				* (ar1.averageCompressionTimeOverRounds[i] - ar1.averageCompressionTime);
			ratioToDecompressionTime += (ar1.compressionRatios[i] - ar1.averageCompressionRatio)
				* (ar1.averageDecompressionTimeOverRounds[i] - ar1.averageDecompressionTime);
		}

		ar1.cRToCTCoefficientB = ratioToCompressionTime / ratioToRatio;
		ar1.cRToCTCoefficientA = ar1.averageCompressionTime - (ar1.cRToCTCoefficientB * ar1.averageCompressionRatio);

		ar1.cRToDTCoefficientB = ratioToDecompressionTime / ratioToRatio;
		ar1.cRToDTCoefficientA = ar1.averageDecompressionTime - (ar1.cRToDTCoefficientB * ar1.averageCompressionRatio);

		return true;
	}

	private String getAlgorithmNameByCompressionLevel(CompressionLevel cl) {
		if (libraries.containsKey(cl)) {
			return libraries.get(cl).getLibraryName();
		} else {
			return "unknown";
		}
	}

	private void print(String message, boolean newLine) {
		if (output == null) {
			if (newLine)
				System.out.println(message);
			else
				System.out.print(message);
		} else {
			if (newLine)
				output.append(message + "\n");
			else
				output.append(message);
		}
	}

	@Override
	public void run() {
		startTraining();

	}

	public TrainingResults getResults() {
		return results;
	}

	public boolean isTrainingDone() {
		return isTrainingDone;
	}
}
