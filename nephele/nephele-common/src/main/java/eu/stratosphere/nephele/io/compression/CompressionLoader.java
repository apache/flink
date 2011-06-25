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
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.util.StringUtils;

public class CompressionLoader {

	private static final Log LOG = LogFactory.getLog(CompressionLoader.class);

	private static final Map<CompressionLevel, AbstractCompressionLibrary> compressionLibraries = new HashMap<CompressionLevel, AbstractCompressionLibrary>();

	private static final String NATIVELIBRARYCACHENAME = "nativeLibraryCache";

	public static synchronized void init(final CompressionLevel compressionLevel) {

		// Return immediately, if implementation for compression level has already been loaded
		if (compressionLibraries.containsKey(compressionLevel)) {
			return;
		}

		String keySuffix = null;
		switch (compressionLevel) {
		case LIGHT_COMPRESSION:
			keySuffix = "lightClass";
			break;
		case MEDIUM_COMPRESSION:
			keySuffix = "mediumClass";
			break;
		case HEAVY_COMPRESSION:
			keySuffix = "heavyClass";
			break;
		case DYNAMIC_COMPRESSION:
			keySuffix = "dynamicClass";
			break;
		}

		if (keySuffix == null) {
			throw new RuntimeException("Cannot find keySuffix for compression level " + compressionLevel);
		}

		final String key = "channel.compression." + keySuffix;
		final String libraryClass = GlobalConfiguration.getString(key, null);
		if (libraryClass == null) {
			throw new RuntimeException("No library class for compression Level " + compressionLevel + " configured");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Trying to load compression library " + libraryClass);
		}

		final AbstractCompressionLibrary compressionLibrary = initCompressionLibrary(libraryClass);
		if (compressionLibrary == null) {
			throw new RuntimeException("Cannot load " + libraryClass);
		}

		compressionLibraries.put(compressionLevel, compressionLibrary);
	}

	/**
	 * Returns the path to the native libraries or <code>null</code> if an error occurred.
	 * 
	 * @param libraryClass
	 *        the name of this compression library's wrapper class including full package name
	 * @return the path to the native libraries or <code>null</code> if an error occurred
	 */
	private static String getNativeLibraryPath(final String libraryClass) {

		final ClassLoader cl = ClassLoader.getSystemClassLoader();
		if (cl == null) {
			LOG.error("Cannot find system class loader");
			return null;
		}

		final String classLocation = libraryClass.replace('.', '/') + ".class";
		if (LOG.isDebugEnabled()) {
			LOG.debug("Class location is " + classLocation);
		}

		final URL location = cl.getResource(classLocation);
		if (location == null) {
			LOG.error("Cannot determine location of CompressionLoader class");
			return null;
		}

		final String locationString = location.toString();
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
	private static AbstractCompressionLibrary initCompressionLibrary(String libraryClass) {

		Class<? extends AbstractCompressionLibrary> compressionLibraryClass;
		try {
			compressionLibraryClass = (Class<? extends AbstractCompressionLibrary>) Class.forName(libraryClass);
		} catch (ClassNotFoundException e1) {
			LOG.error(e1);
			return null;
		}

		if (compressionLibraryClass == null) {
			LOG.error("Cannot load compression library " + libraryClass);
			return null;
		}

		Constructor<? extends AbstractCompressionLibrary> constructor;
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

		AbstractCompressionLibrary compressionLibrary;

		try {
			compressionLibrary = constructor.newInstance(getNativeLibraryPath(libraryClass));
		} catch (IllegalArgumentException e) {
			LOG.error(StringUtils.stringifyException(e));
			return null;
		} catch (InstantiationException e) {
			LOG.error(StringUtils.stringifyException(e));
			return null;
		} catch (IllegalAccessException e) {
			LOG.error(StringUtils.stringifyException(e));
			return null;
		} catch (InvocationTargetException e) {
			LOG.error(StringUtils.stringifyException(e));
			return null;
		}

		return compressionLibrary;
	}

	public static synchronized AbstractCompressionLibrary getCompressionLibraryByCompressionLevel(CompressionLevel level) {

		if (level == CompressionLevel.NO_COMPRESSION) {
			return null;
		}

		init(level);

		final AbstractCompressionLibrary cl = compressionLibraries.get(level);
		if (cl == null) {
			LOG.error("Cannot find compression library for compression level " + level);
			return null;
		}

		return cl;
	}

	public static synchronized Compressor getCompressorByCompressionLevel(final CompressionLevel level,
			final AbstractByteBufferedOutputChannel<?> outputChannel) {

		if (level == CompressionLevel.NO_COMPRESSION) {
			return null;
		}

		init(level);

		try {

			final AbstractCompressionLibrary cl = compressionLibraries.get(level);
			if (cl == null) {
				LOG.error("Cannot find compression library for compression level " + level);
				return null;
			}

			return cl.getCompressor(outputChannel);

		} catch (CompressionException e) {
			LOG.error("Cannot load native compressor: " + StringUtils.stringifyException(e));
			return null;
		}
	}

	public static synchronized Decompressor getDecompressorByCompressionLevel(final CompressionLevel level,
			final AbstractByteBufferedInputChannel<?> inputChannel) {

		if (level == CompressionLevel.NO_COMPRESSION) {
			return null;
		}

		init(level);

		try {

			final AbstractCompressionLibrary cl = compressionLibraries.get(level);
			if (cl == null) {
				LOG.error("Cannot find compression library for compression level " + level);
				return null;
			}

			return cl.getDecompressor(inputChannel);

		} catch (CompressionException e) {
			LOG.error("Cannot load native decompressor: " + StringUtils.stringifyException(e));
			return null;
		}
	}

	public static synchronized int getUncompressedBufferSize(int compressedBufferSize, CompressionLevel cl) {

		final AbstractCompressionLibrary c = compressionLibraries.get(cl);
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
