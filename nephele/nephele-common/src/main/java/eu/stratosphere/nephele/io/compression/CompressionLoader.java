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

package eu.stratosphere.nephele.io.compression;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.util.StringUtils;

public class CompressionLoader {

	private static final Log LOG = LogFactory.getLog(CompressionLoader.class);

	private static final Map<CompressionLevel, CompressionLibrary> compressionLibraries = new HashMap<CompressionLevel, CompressionLibrary>();

	private static boolean finished = true;

	private static boolean compressionLoaded = false;

	private static final String NATIVELIBRARYCACHENAME = "nativeLibraryCache";

	/**
	 * Initialize the CompressionLoader and load all native compression libraries.
	 * assumes that GlobalConfiguration was already loaded
	 */
	public static synchronized void init() {

		if (!compressionLoaded) {

			compressionLoaded = true;

			final CompressionLevel[] compressionLevels = { CompressionLevel.LIGHT_COMPRESSION,
				CompressionLevel.MEDIUM_COMPRESSION, CompressionLevel.HEAVY_COMPRESSION,
				CompressionLevel.DYNAMIC_COMPRESSION };
			final String[] keySuffix = { "lightClass", "mediumClass", "heavyClass", "dynamicClass" };

			for (int i = 0; i < compressionLevels.length; i++) {

				final String key = "channel.compression." + keySuffix[i];
				final String libraryClass = GlobalConfiguration.getString(key, null);
				if (libraryClass == null) {
					LOG.warn("No library class for compression Level " + compressionLevels[i] + " configured");
					continue;
				}

				LOG.debug("Trying to load compression library " + libraryClass);
				final CompressionLibrary compressionLibrary = initCompressionLibrary(libraryClass);
				if (compressionLibrary == null) {
					LOG.error("Cannot load " + libraryClass);
					continue;
				}

				compressionLibraries.put(compressionLevels[i], compressionLibrary);
			}
		}

		finished = false;
	}

	/**
	 * Returns the path to the native libraries or <code>null</code> if an error occurred.
	 * 
	 * @return the path to the native libraries or <code>null</code> if an error occurred
	 */
	private static String getNativeLibraryPath() {

		final ClassLoader cl = ClassLoader.getSystemClassLoader();
		if (cl == null) {
			LOG.error("Cannot find system class loader");
			return null;
		}

		final String classLocation = "eu/stratosphere/nephele/io/compression/library/zlib/ZlibLibrary.class"; // TODO:
		// Use
		// other
		// class
		// here

		final URL location = cl.getResource(classLocation);
		if (location == null) {
			LOG.error("Cannot determine location of CompressionLoader class");
			return null;
		}

		final String locationString = location.toString();
		// System.out.println("LOCATION: " + locationString);
		if (locationString.contains(".jar!")) { // Class if inside of a deployed jar file

			// Create and return path to native library cache
			final String pathName = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
				ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH)
				+ File.separator + NATIVELIBRARYCACHENAME;

			final File path = new File(pathName);
			if (!path.exists()) {
				if (!path.mkdir()) {
					LOG.error("Cannot create directory for native library cache.");
					return null;
				}
			}

			return pathName;

		} else {

			String result = "";

			int pos = locationString.indexOf(classLocation);
			if (pos < 0) {
				LOG.error("Cannot find extract native path from class location");
				return null;
			}

			result = locationString.substring(0, pos) + "META-INF/lib";

			// Strip the file:/ scheme, it confuses the class loader
			if (result.startsWith("file:")) {
				result = result.substring(5);
			}

			return result;
		}
	}

	/**
	 * Initialize the CompressionLoader, load all native compression libraries and prepare Compression-Decision-Model.
	 */
	/*
	 * public static void init2() {
	 * init();
	 * String compressionTrainingFile = GlobalConfiguration.getString("taskmanager.compression.trainingfile.default",
	 * "/home/akli/uni/diplom/nephele-new/nephele/nephele-common/src/native/default_compression_training_data.dat");
	 * String compressionDecisionModel =
	 * GlobalConfiguration.getString("taskmanager.compression.decisionmodel.classname",
	 * "eu.stratosphere.nephele.io.compression.dynamic.TestModel");
	 * if(GlobalConfiguration.getBoolean(ENABLE_THROUGHPUT_ANALYZER_KEY, false)) {
	 * final String address = GlobalConfiguration.getString(THROUGHPUT_ANALYZER_IPC_ADDRESS_KEY,
	 * DEFAULT_THROUGHPUT_ANALYZER_IPC_ADDRESS);
	 * final int port = GlobalConfiguration.getInteger(THROUGHPUT_ANALYZER_IPC_PORT_KEY,
	 * DEFAULT_THROUGHPUT_ANALYZER_IPC_PORT);
	 * InetSocketAddress throughputAnalyzerAddress = new InetSocketAddress(address, port);
	 * //Try to create local stub for the ThroughputAnalyzer
	 * ThroughputAnalyzerProtocol throughput = null; //TODO: Fix me
	 * try {
	 * throughput = (ThroughputAnalyzerProtocol) RPC.getProxy(ThroughputAnalyzerProtocol.class,
	 * throughputAnalyzerAddress, NetUtils.getSocketFactory());
	 * } catch (IOException e) {
	 * LOG.error("Error during load of ThroughputAnalyzer");
	 * LOG.error(StringUtils.stringifyException(e));
	 * //System.exit(FAILURERETURNCODE);
	 * }
	 * throughputAnalyzer = throughput;
	 * LOG.info("ThroughputAnalyzer loaded!");
	 * }
	 * //initCompressionDecisionModel(compressionDecisionModel, compressionTrainingFile); //TODO: Fix me
	 * }
	 */

	/*
	 * public static void initCompressionDecisionModel(String modelName, String pathToTrainingFile){
	 * if (modelName.compareTo(IOWaitModel.class.getName()) == 0){
	 * if (instance != null && initWithProfiler(instance)){
	 * decisionModel = DecisionModel.IOWAIT_MODEL;
	 * LOG.info("CompressionLoader: Using IOWaitModel for Dynamic Compression.");
	 * }else{
	 * decisionModel = DecisionModel.TEST_MODEL;
	 * LOG.warn(
	 * "CompressionLoader: Could not load IOWaitModel for Dynamic Compression - Using TestModel with Lzo Compression.");
	 * }
	 * }else if (modelName.compareTo(ThroughputModel.class.getName()) == 0){
	 * if (throughputAnalyzer != null){
	 * decisionModel = DecisionModel.THROUGHPUT_MODEL;
	 * LOG.info("CompressionLoader: Using ThroughputModel for Dynamic Compression.");
	 * }else{
	 * decisionModel = DecisionModel.TEST_MODEL;
	 * LOG.warn(
	 * "CompressionLoader: Could not load ThroughputModel for Dynamic Compression - Using TestModel with Lzo Compression."
	 * );
	 * }
	 * }else if (modelName.compareTo(ExtendedThroughputModel.class.getName()) == 0){
	 * if (throughputAnalyzer != null){
	 * decisionModel = DecisionModel.EXTENDED_THROUGHPUT_MODEL;
	 * LOG.info("CompressionLoader: Using ExtendedThroughputModel for Dynamic Compression.");
	 * }else{
	 * decisionModel = DecisionModel.TEST_MODEL;
	 * LOG.warn(
	 * "CompressionLoader: Could not load ExtendedThroughputModel for Dynamic Compression - Using TestModel with Lzo Compression."
	 * );
	 * }
	 * }else if (modelName.compareTo(NumericModel.class.getName()) == 0){
	 * if (instance != null && initWithProfiler(instance)){
	 * decisionModel = DecisionModel.NUMERIC_MODEL;
	 * LOG.info("CompressionLoader: Using NumericModel for Dynamic Compression.");
	 * }else{
	 * decisionModel = DecisionModel.TEST_MODEL;
	 * LOG.warn(
	 * "CompressionLoader: Could not load NumericModel for Dynamic Compression - Using TestModel with Lzo Compression."
	 * );
	 * }
	 * }else if (modelName.compareTo(TrainedModel.class.getName()) == 0){
	 * if (pathToTrainingFile != null && initWithTrainingset(pathToTrainingFile)){
	 * decisionModel = DecisionModel.TRAINED_MODEL;
	 * LOG.info("CompressionLoader: Using TrainedModel for Dynamic Compression.");
	 * }else{
	 * decisionModel = DecisionModel.TEST_MODEL;
	 * LOG.warn(
	 * "CompressionLoader: Could not load TrainedModel for Dynamic Compression - Using TestModel with Lzo Compression."
	 * );
	 * }
	 * }else if (modelName.compareTo(ExtendedTrainedModel.class.getName()) == 0){
	 * if (pathToTrainingFile != null && initWithTrainingset(pathToTrainingFile) && throughputAnalyzer != null){
	 * decisionModel = DecisionModel.EXTENDED_TRAINED_MODEL;
	 * LOG.info("CompressionLoader: Using ExtendedTrainedModel for Dynamic Compression.");
	 * }else{
	 * decisionModel = DecisionModel.TEST_MODEL;
	 * LOG.warn(
	 * "CompressionLoader: Could not load ExtendedTrainedModel for Dynamic Compression - Using TestModel with Lzo Compression."
	 * );
	 * }
	 * }else{
	 * LOG.info("DecisionModel for Dynamic Compression not set or unknown ( " + modelName +
	 * " ) - Using TestModel with Lzo Compression.");
	 * decisionModel = DecisionModel.TEST_MODEL;
	 * }
	 * }
	 */

	@SuppressWarnings("unchecked")
	private static CompressionLibrary initCompressionLibrary(String libraryClass) {

		Class<? extends CompressionLibrary> compressionLibraryClass;
		try {
			compressionLibraryClass = (Class<? extends CompressionLibrary>) Class.forName(libraryClass);
		} catch (ClassNotFoundException e1) {
			LOG.error(e1);
			return null;
		}

		if (compressionLibraryClass == null) {
			LOG.error("Cannot load compression library " + libraryClass);
			return null;
		}

		Constructor<? extends CompressionLibrary> constructor;
		try {
			constructor = compressionLibraryClass.getConstructor(String.class);
		} catch (SecurityException e) {
			LOG.error(e);
			return null;
		} catch (NoSuchMethodException e) {
			LOG.error(e);
			return null;
		}
		if (constructor == null) {
			LOG.error("Cannot find matching constructor for class " + compressionLibraryClass.toString());
			return null;
		}

		CompressionLibrary compressionLibrary;

		try {
			compressionLibrary = constructor.newInstance(getNativeLibraryPath());
		} catch (IllegalArgumentException e) {
			LOG.error(e);
			return null;
		} catch (InstantiationException e) {
			LOG.error(e);
			return null;
		} catch (IllegalAccessException e) {
			LOG.error(e);
			return null;
		} catch (InvocationTargetException e) {
			LOG.error(e);
			return null;
		}

		return compressionLibrary;
	}

	/*
	 * private static boolean initWithTrainingset(String fileName){
	 * File f = new File(fileName);
	 * if (f.exists() && f.isFile()){
	 * FileInputStream fis;
	 * ObjectInputStream ois;
	 * try {
	 * fis = new FileInputStream(f);
	 * ois = new ObjectInputStream(fis);
	 * trainingSet = (TrainingResults) ois.readObject();
	 * ois.close();
	 * return true;
	 * } catch (FileNotFoundException e1) {
	 * // TODO Auto-generated catch block
	 * e1.printStackTrace();
	 * } catch (IOException e2) {
	 * // TODO Auto-generated catch block
	 * e2.printStackTrace();
	 * } catch (ClassNotFoundException e3) {
	 * // TODO Auto-generated catch block
	 * e3.printStackTrace();
	 * }
	 * }
	 * return false;
	 * }
	 */

	public static synchronized CompressionLibrary getCompressionLibraryByCompressionLevel(CompressionLevel level) {

		if (level == CompressionLevel.NO_COMPRESSION) {
			return null;
		}

		if (!compressionLoaded) {
			// Lazy initialization
			init();
		}

		final CompressionLibrary cl = compressionLibraries.get(level);
		if (cl == null) {
			LOG.error("Cannot find compression library for compression level " + level);
			return null;
		}

		return cl;
	}

	public static synchronized Compressor getCompressorByCompressionLevel(CompressionLevel level) {

		if (level == CompressionLevel.NO_COMPRESSION) {
			return null;
		}

		if (!compressionLoaded) {
			// Lazy initialization
			init();
		}

		try {

			final CompressionLibrary cl = compressionLibraries.get(level);
			if (cl == null) {
				LOG.error("Cannot find compression library for compression level " + level);
				return null;
			}

			return cl.getCompressor();

		} catch (CompressionException e) {
			LOG.error("Cannot load native compressor: " + StringUtils.stringifyException(e));
			return null;
		}
	}

	public static synchronized Decompressor getDecompressorByCompressionLevel(CompressionLevel level) {

		if (level == CompressionLevel.NO_COMPRESSION) {
			return null;
		}

		if (!compressionLoaded) {
			// Lazy initialization
			init();
		}

		if (finished) {
			LOG.error("CompressionLoader already finished. Unable to construct more decompressors");
			return null;
		}

		try {

			final CompressionLibrary cl = compressionLibraries.get(level);
			if (cl == null) {
				LOG.error("Cannot find compression library for compression level " + level);
				return null;
			}

			return cl.getDecompressor();

		} catch (CompressionException e) {
			LOG.error("Cannot load native decompressor: " + StringUtils.stringifyException(e));
			return null;
		}
	}

	/*
	 * public static boolean isThroughputAnalyzerLoaded(){
	 * return throughputAnalyzer != null;
	 * }
	 */

	public static synchronized int getUncompressedBufferSize(int compressedBufferSize, CompressionLevel cl) {

		final CompressionLibrary c = compressionLibraries.get(cl);
		if (c == null) {
			LOG.error("Cannot find compression library for compression level " + cl);
			return compressedBufferSize;
		}

		return c.getUncompressedBufferSize(compressedBufferSize);
	}

	public static synchronized int getCompressedBufferSize(int uncompressedBufferSize, CompressionLevel cl) {
		switch (cl) {
		case HEAVY_COMPRESSION:
			/*
			 * Calculate size of compressed data buffer according to
			 * http://gpwiki.org/index.php/LZO
			 */
			// TODO check that for LZMA
			return uncompressedBufferSize + ((uncompressedBufferSize / 1024) * 16);
		case MEDIUM_COMPRESSION:
			/*
			 * Calculate size of compressed data buffer according to
			 * zlib manual
			 * http://www.zlib.net/zlib_tech.html
			 */
			return uncompressedBufferSize + (int) ((uncompressedBufferSize / 100) * 0.04) + 6;
		case DYNAMIC_COMPRESSION:
		case LIGHT_COMPRESSION:
			/*
			 * Calculate size of compressed data buffer according to
			 * LZO Manual http://www.oberhumer.com/opensource/lzo/lzofaq.php
			 */
			return uncompressedBufferSize + (uncompressedBufferSize / 16) + 64 + 3;
		default:
			return uncompressedBufferSize;
		}
	}

	/*
	 * public static void reportDatapackageSend(int compressorID, int dataID, int bytes, int uncompressedBytes, int
	 * uncompressedBufferSize, int compressionLevel){
	 * if (throughputAnalyzer != null)
	 * throughputAnalyzer.reportDatapackageSend(new IntegerRecord(compressorID), new IntegerRecord(dataID), new
	 * IntegerRecord(bytes), new IntegerRecord(uncompressedBytes), new IntegerRecord(uncompressedBufferSize), new
	 * IntegerRecord(compressionLevel));
	 * }
	 * public static void reportDatapackageReceive(int dataID){
	 * if (throughputAnalyzer != null)
	 * throughputAnalyzer.reportDatapackageReceive(new IntegerRecord(dataID));
	 * }
	 * public static ThroughputAnalyzerResult getAverageCommunicationTimeForCompressor(int compressorID){
	 * if (throughputAnalyzer != null)
	 * return throughputAnalyzer.getAverageCommunicationTimeForCompressor(new IntegerRecord(compressorID));
	 * else{
	 * return new ThroughputAnalyzerResult();
	 * }
	 * }
	 * public static double getCurrentBandwidthInBytesPerNS(ChannelType type){
	 * switch(type){
	 * case FILE:
	 * return 0.042; //0.084Byte/ns = 83886.08Byte/ms = 83886080Byte/s = 80MByte/s (assumed average read and write
	 * performance is 80MByte/s - result divided by 2 because we have to write and read)
	 * case NETWORK:
	 * InternalInstanceProfilingDataCompression pc =
	 * profiler.generateProfilingDataCompression(System.currentTimeMillis());
	 * if (pc.getGoodput() == 0){
	 * LOG.warn("Could not determine speed of network connection. Using 100MBit/s" );
	 * return 0.0131*0.8; //0.0131Byte/ns = 13107.2Byte/ms = 13107200Byte/s = 100MBit/s (more than 80% are not
	 * realistic)
	 * }else
	 * return pc.getGoodput()/1000000000; //getGoodput gives speed per second in bytes, we need per nanosecond
	 * //return 0.131*0.8; //0.131Byte/ns = 131072Byte/ms = 131072000Byte/s = 1000MBit/s (more than 80% are not
	 * realistic)
	 * case INMEMORY:
	 * return 1;
	 * }
	 * LOG.error("Unknown Channel Type! " + type);
	 * return 1;
	 * }
	 */

}
