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

package org.apache.flink.api.common.io.blockcompression;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A helper class to get specified {@link BlockCompressionFactory}.
 */
public class BlockCompressionFactoryLoader {

	public enum CompressionMethod {
		LZ4, BZIP2, GZIP
	}

	/**
	 * Creates {@link BlockCompressionFactory} according to the configuration.
	 * @param compressionFactoryName supported compression codecs or user-defined class name inherited from
	 *                               {@link BlockCompressionFactory}.
	 * @param configuration configurations for compression factory.
	 * @return
	 */
	public static BlockCompressionFactory createBlockCompressionFactory(
		String compressionFactoryName, Configuration configuration) {

		checkNotNull(compressionFactoryName);
		checkNotNull(configuration);

		CompressionMethod compressionMethod;
		try {
			compressionMethod = CompressionMethod.valueOf(compressionFactoryName.toUpperCase());
		} catch (IllegalArgumentException e) {
			compressionMethod = null;
		}

		BlockCompressionFactory blockCompressionFactory = null;
		if (compressionMethod != null) {
			switch (compressionMethod) {
				case LZ4:
					blockCompressionFactory = new Lz4BlockCompressionFactory();
					break;
				case BZIP2:
					blockCompressionFactory = new Bzip2BlockCompressionFactory();
					break;
				case GZIP:
					blockCompressionFactory = new GzipBlockCompressionFactory();
					break;
				default:
					throw new IllegalStateException("Unknown CompressionMethod " + compressionMethod);
			}
		} else {
			Object factoryObj = null;
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
				throw new IllegalArgumentException(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_COMPRESSION_CODEC.key() +
					" should inherit from interface BlockCompressionFactory, or use the default compression codec.");
			}
		}

		checkNotNull(blockCompressionFactory);
		blockCompressionFactory.setConfiguration(configuration);
		return blockCompressionFactory;
	}
}
