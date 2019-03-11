/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.compression;

import org.apache.flink.configuration.IllegalConfigurationException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Each compression codec has a implementation of {@link BlockCompressionFactory}
 * to create compressors and decompressors.
 */
public interface BlockCompressionFactory {

	BlockCompressor getCompressor();

	BlockDecompressor getDecompressor();

	/**
	 * Name of {@link BlockCompressionFactory}.
	 */
	enum CompressionFactoryName {
		LZ4
	}

	/**
	 * Creates {@link BlockCompressionFactory} according to the configuration.
	 * @param compressionFactoryName supported compression codecs or user-defined class name inherited from
	 *                               {@link BlockCompressionFactory}.
	 */
	static BlockCompressionFactory createBlockCompressionFactory(String compressionFactoryName) {

		checkNotNull(compressionFactoryName);

		CompressionFactoryName compressionName;
		try {
			compressionName = CompressionFactoryName.valueOf(compressionFactoryName.toUpperCase());
		} catch (IllegalArgumentException e) {
			compressionName = null;
		}

		BlockCompressionFactory blockCompressionFactory = null;
		if (compressionName != null) {
			switch (compressionName) {
				case LZ4:
					blockCompressionFactory = new Lz4BlockCompressionFactory();
					break;
				default:
					throw new IllegalStateException("Unknown CompressionMethod " + compressionName);
			}
		} else {
			Object factoryObj;
			try {
				factoryObj = Class.forName(compressionFactoryName).newInstance();
			} catch (ClassNotFoundException e) {
				throw new IllegalConfigurationException("Cannot load class " + compressionFactoryName, e);
			} catch (Exception e) {
				throw new IllegalConfigurationException("Cannot create object for class " + compressionFactoryName, e);
			}
			if (factoryObj instanceof BlockCompressionFactory) {
				blockCompressionFactory = (BlockCompressionFactory) factoryObj;
			} else {
				throw new IllegalArgumentException("CompressionFactoryName should inherit from" +
						" interface BlockCompressionFactory, or use the default compression codec.");
			}
		}

		checkNotNull(blockCompressionFactory);
		return blockCompressionFactory;
	}
}
