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

package eu.stratosphere.nephele.io.compression.profiling;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.ipc.Server;
import eu.stratosphere.nephele.types.IntegerRecord;
import eu.stratosphere.nephele.util.StringUtils;

public class ThroughputAnalyzer implements ThroughputAnalyzerProtocol {

	private static final Log LOG = LogFactory.getLog(ThroughputAnalyzer.class);

	private static final int handlerCount = 1;

	private final Server throughputAnalyzerServer;

	private static final int FAILURERETURNCODE = -1;

	/**
	 * Default network port the ThroughputAnalyzer expects incoming IPC connections
	 */
	private final static int DEFAULT_IPC_PORT = 6172;

	/**
	 * Key to retrieve the task manager's IPC port from the configuration.
	 */
	private final static String THROUGHPUTANALYZER_IPC_PORT_KEY = "throughputanalyzer.rpc.port";

	private HashMap<Integer, DataInformation> livingDataPackages = new HashMap<Integer, DataInformation>();

	private HashMap<Integer, CompressorInformation> compressorInfo = new HashMap<Integer, CompressorInformation>();

	private HashMap<Integer, ThroughputAnalyzerResult> results = new HashMap<Integer, ThroughputAnalyzerResult>();

	private LinkedList<DataInformation> analyzerQueue = new LinkedList<DataInformation>();

	private final static int BYTE_SCALE_FACTOR = 1048576; // 1MB

	private final static double EXP_SMOOTHING_ALPHA = 0.3;

	public ThroughputAnalyzer(String configDir) {

		// First, try to load global configuration
		GlobalConfiguration.loadConfiguration(configDir);

		final int ipcPort = GlobalConfiguration.getInteger(THROUGHPUTANALYZER_IPC_PORT_KEY, DEFAULT_IPC_PORT);
		final InetSocketAddress throughputAnalyzerBindAddress = new InetSocketAddress((InetAddress) null, ipcPort);

		// Start local RPC server
		Server throughputAnalyzerServer = null;
		try {
			throughputAnalyzerServer = RPC.getServer(this, throughputAnalyzerBindAddress.getHostName(),
				throughputAnalyzerBindAddress.getPort(), handlerCount, false);
			throughputAnalyzerServer.start();
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
			System.exit(FAILURERETURNCODE);
		}
		this.throughputAnalyzerServer = throughputAnalyzerServer;

		LOG.info("ThroughputAnalyzer started.");
	}

	/**
	 * Entry point for the program.
	 * 
	 * @param args
	 *        arguments from the command line
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) {

		Option configDirOpt = OptionBuilder.withArgName("config directory").hasArg().withDescription(
			"Specify configuration directory.").create("configDir");

		Options options = new Options();
		options.addOption(configDirOpt);

		CommandLineParser parser = new GnuParser();
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("CLI Parsing failed. Reason: " + e.getMessage());
			System.exit(FAILURERETURNCODE);
		}

		String configDir = line.getOptionValue(configDirOpt.getOpt(), null);

		// Create a new task manager object
		ThroughputAnalyzer ta = new ThroughputAnalyzer(configDir);

		// Run the main I/O loop
		ta.runIOLoop();

		// Clean up
		ta.cleanUp();
	}

	// This method is called by the TaskManagers main thread
	public void runIOLoop() {

		while (!Thread.interrupted()) {

			DataInformation tmp = null;
			synchronized (analyzerQueue) {
				if (analyzerQueue.isEmpty()) {
					try {
						analyzerQueue.wait();
						tmp = analyzerQueue.pollFirst();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else
					tmp = analyzerQueue.pollFirst();
			}

			if (tmp == null)
				continue;

			long scaledTime = tmp.communicationTime * (tmp.uncompressedBufferSize / tmp.uncompressedBytes);
			long scaledByteTime = (scaledTime * BYTE_SCALE_FACTOR) / tmp.bytes;
			CompressorInformation ci;
			if (compressorInfo.containsKey(tmp.compressorID)) {
				ci = compressorInfo.get(tmp.compressorID);
			} else {
				ci = new CompressorInformation();
				compressorInfo.put(tmp.compressorID, ci);
			}

			ci.addNewValue(scaledTime, scaledByteTime, tmp.compressionLevel);
			updateResults(tmp.compressorID, ci);
			// long newVal = 0;

			/*
			 * switch (tmp.compressionLevel){
			 * case 0:
			 * newVal = ci.getNoCompressionTime();
			 * if (newVal == -1)
			 * ci.setNoCompressionTime(scaledTime);
			 * else{
			 * newVal += scaledTime;
			 * ci.setNoCompressionTime(newVal/2);
			 * }
			 * break;
			 * case 1:
			 * newVal = ci.getLightCompressionTime();
			 * if (newVal == -1)
			 * ci.setLightCompressionTime(scaledTime);
			 * else{
			 * newVal += scaledTime;
			 * ci.setLightCompressionTime(newVal/2);
			 * }
			 * break;
			 * case 2:
			 * newVal = ci.getMediumCompressionTime();
			 * if (newVal == -1)
			 * ci.setMediumCompressionTime(scaledTime);
			 * else{
			 * newVal += scaledTime;
			 * ci.setMediumCompressionTime(newVal/2);
			 * }
			 * break;
			 * case 3:
			 * newVal = ci.getMediumHeavyCompressionTime();
			 * if (newVal == -1)
			 * ci.setMediumHeavyCompressionTime(scaledTime);
			 * else{
			 * newVal += scaledTime;
			 * ci.setMediumHeavyCompressionTime(newVal/2);
			 * }
			 * break;
			 * case 4:
			 * newVal = ci.getHeavyCompressionTime();
			 * if (newVal == -1)
			 * ci.setHeavyCompressionTime(scaledTime);
			 * else{
			 * newVal += scaledTime;
			 * ci.setHeavyCompressionTime(newVal/2);
			 * }
			 * break;
			 * }
			 */

		}
	}

	/**
	 * Performs clean up operations on task manager exit
	 */
	public void cleanUp() {

		LOG.info("Cleaning up");

		// Shut down the own RPC server
		this.throughputAnalyzerServer.stop();

	}

	@Override
	public void reportDatapackageReceive(IntegerRecord dataID) {
		long receiveTime = System.nanoTime();

		if (!livingDataPackages.containsKey(dataID.getValue())) {
			LOG.error("ThroughputAnalyzer: Received message for unknown dataID " + dataID.getValue());
		} else {
			LOG.debug("ThroughputAnalyzer: package received! " + dataID.getValue());
			DataInformation tmp = null;
			synchronized (livingDataPackages) {
				tmp = livingDataPackages.remove(dataID.getValue());
			}

			tmp.receiveTime = receiveTime;
			tmp.communicationTime = tmp.receiveTime - tmp.sendTime;

			synchronized (analyzerQueue) {
				analyzerQueue.addLast(tmp);
				analyzerQueue.notify();
			}
		}

	}

	@Override
	public void reportDatapackageSend(IntegerRecord compressorID, IntegerRecord dataID, IntegerRecord bytes,
			IntegerRecord uncompressedBytes, IntegerRecord uncompressedBufferSize, IntegerRecord compressionLevel) {
		long sendTime = System.nanoTime();
		synchronized (livingDataPackages) {
			livingDataPackages.put(dataID.getValue(), new DataInformation(compressorID.getValue(), dataID.getValue(),
				sendTime, bytes.getValue(), uncompressedBytes.getValue(), uncompressedBufferSize.getValue(),
				compressionLevel.getValue()));
		}

		LOG.debug("ThroughputAnalyzer: new package on the way! " + dataID.getValue());

	}

	private void updateResults(int compressorID, CompressorInformation ci) {
		ThroughputAnalyzerResult result = new ThroughputAnalyzerResult();
		result.setByteThroughputTime(ci.byteTimeSeries[3]);
		result.setNoCompressionTime(ci.noCompressionSeries[3]);
		result.setNoCompressionAge((int) ci.noCompressionSeries[2]);
		result.setLightCompressionTime(ci.lightCompressionSeries[3]);
		result.setLightCompressionAge((int) ci.lightCompressionSeries[2]);
		result.setMediumCompressionTime(ci.mediumCompressionSeries[3]);
		result.setMediumCompressionAge((int) ci.mediumCompressionSeries[2]);
		result.setMediumHeavyCompressionTime(ci.mediumHeavyCompressionSeries[3]);
		result.setMediumHeavyCompressionAge((int) ci.mediumHeavyCompressionSeries[2]);
		result.setHeavyCompressionTime(ci.heavyCompressionSeries[3]);
		result.setHeavyCompressionAge((int) ci.heavyCompressionSeries[2]);

		synchronized (results) {
			results.put(compressorID, result);
		}
	}

	private class DataInformation {
		int compressorID = 0;

		int dataID = 0;

		long sendTime = 0;

		long receiveTime = 0;

		int bytes = 0;

		int uncompressedBytes = 0;

		int uncompressedBufferSize = 0;

		int compressionLevel;

		long communicationTime = 0;

		public DataInformation(int compressorID, int dataID, long sendTime, int bytes, int uncompressedBytes,
				int uncompressedBufferSize, int compressionLevel) {
			this.compressorID = compressorID;
			this.dataID = dataID;
			this.sendTime = sendTime;
			this.compressionLevel = compressionLevel;
			this.bytes = bytes;
		}
	}

	private class CompressorInformation {
		private long[] noCompressionSeries;

		private long[] lightCompressionSeries;

		private long[] mediumCompressionSeries;

		private long[] mediumHeavyCompressionSeries;

		private long[] heavyCompressionSeries;

		private long[] byteTimeSeries;

		private long[] currentSeries = null;

		public CompressorInformation() {
			noCompressionSeries = new long[4];
			noCompressionSeries[0] = -1;// yt1
			noCompressionSeries[1] = -1;// yt2
			noCompressionSeries[2] = 0;// series age
			noCompressionSeries[3] = -1;// next predicted time

			lightCompressionSeries = new long[4];
			lightCompressionSeries[0] = -1;
			lightCompressionSeries[1] = -1;
			lightCompressionSeries[2] = 0;
			lightCompressionSeries[3] = -1;

			mediumCompressionSeries = new long[4];
			mediumCompressionSeries[0] = -1;
			mediumCompressionSeries[1] = -1;
			mediumCompressionSeries[2] = 0;
			mediumCompressionSeries[3] = -1;

			mediumHeavyCompressionSeries = new long[4];
			mediumHeavyCompressionSeries[0] = -1;
			mediumHeavyCompressionSeries[1] = -1;
			mediumHeavyCompressionSeries[2] = 0;
			mediumHeavyCompressionSeries[3] = -1;

			heavyCompressionSeries = new long[4];
			heavyCompressionSeries[0] = -1;
			heavyCompressionSeries[1] = -1;
			heavyCompressionSeries[2] = 0;
			heavyCompressionSeries[3] = -1;

			byteTimeSeries = new long[4];
			byteTimeSeries[0] = -1;
			byteTimeSeries[1] = -1;
			byteTimeSeries[2] = 0;
			byteTimeSeries[3] = -1;
		}

		public void addNewValue(long compressionTime, long scaledTime, int compressionLevel) {
			switch (compressionLevel) {
			case 0:
				insertValueComputeExpSmoothing(noCompressionSeries, compressionTime);

				lightCompressionSeries[2]++;
				mediumCompressionSeries[2]++;
				mediumHeavyCompressionSeries[2]++;
				heavyCompressionSeries[2]++;
				currentSeries = noCompressionSeries;
				break;
			case 1:
				insertValueComputeExpSmoothing(lightCompressionSeries, compressionTime);

				noCompressionSeries[2]++;
				mediumCompressionSeries[2]++;
				mediumHeavyCompressionSeries[2]++;
				heavyCompressionSeries[2]++;
				currentSeries = lightCompressionSeries;
				break;
			case 2:
				insertValueComputeExpSmoothing(mediumCompressionSeries, compressionTime);

				noCompressionSeries[2]++;
				lightCompressionSeries[2]++;
				mediumHeavyCompressionSeries[2]++;
				heavyCompressionSeries[2]++;
				currentSeries = mediumCompressionSeries;
				break;
			case 3:
				insertValueComputeExpSmoothing(mediumHeavyCompressionSeries, compressionTime);

				noCompressionSeries[2]++;
				lightCompressionSeries[2]++;
				mediumCompressionSeries[2]++;
				heavyCompressionSeries[2]++;
				currentSeries = mediumHeavyCompressionSeries;
				break;

			case 4:
				insertValueComputeExpSmoothing(heavyCompressionSeries, compressionTime);

				noCompressionSeries[2]++;
				lightCompressionSeries[2]++;
				mediumCompressionSeries[2]++;
				mediumHeavyCompressionSeries[2]++;
				currentSeries = heavyCompressionSeries;
				break;
			}

			insertValueComputeExpSmoothing(byteTimeSeries, scaledTime);

			currentSeries[2] = 0;
		}

		private void insertValueComputeExpSmoothing(long[] series, long compressionTime) {
			if (series[0] == -1) {
				series[0] = compressionTime;
				series[3] = compressionTime;
				series[2] = 0;
			} else {
				if (series[3] != -1) {
					series[0] = series[3];
				} else {
					series[0] = series[1];
				}

				series[2] = 0;
				series[1] = compressionTime;
				series[3] = (long) ((EXP_SMOOTHING_ALPHA * (double) (series[1])) + ((1 - EXP_SMOOTHING_ALPHA) * (double) (series[0])));
			}

		}

	}

	@Override
	public ThroughputAnalyzerResult getAverageCommunicationTimeForCompressor(IntegerRecord compressorID) {
		ThroughputAnalyzerResult ci = null;
		synchronized (results) {
			ci = results.get(compressorID.getValue());
		}

		if (ci == null) {
			LOG.warn("ThroughputAnalyzer: No data for compressorID " + compressorID.getValue() + " available!");
			return new ThroughputAnalyzerResult();
		} else {

			return ci;
		}
	}

}
