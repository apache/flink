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

/**
 * Implementation of {@link BlockCompressionFactory} for Lz4 codec.
 */
public class Lz4BlockCompressionFactory implements BlockCompressionFactory {


	/**
	 * We put two integers before each compressed block, the first integer represents the compressed
	 * length of the block, and the second one represents the original length of the block.
	 */
	static final int HEADER_LENGTH = 8;

	@Override
	public BlockCompressor getCompressor() {
		return new Lz4BlockCompressor();
	}

	@Override
	public BlockDecompressor getDecompressor() {
		return new Lz4BlockDecompressor();
	}
}
