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
}
