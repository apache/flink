/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.io;

/**
 * Set of supported compression types.
 */
public enum CompressionTypes implements CompressionType {

	/**
	 * No compression when decorating the stream.
	 */
	NONE((byte) 0x0){
		@Override
		public StreamCompressionDecorator getStreamCompressionDecorator() {
			return UncompressedStreamCompressionDecorator.INSTANCE;
		}
	},

	/**
	 * Use Snappy algorithm when decorating the stream.
	 */
	SNAPPY((byte) 0x1){
		@Override
		public StreamCompressionDecorator getStreamCompressionDecorator() {
			return SnappyStreamCompressionDecorator.INSTANCE;
		}
	},

	/**
	 * Use LZ4 algorithm when decorating the stream.
	 */
	LZ4((byte) 0x2){
		@Override
		public StreamCompressionDecorator getStreamCompressionDecorator() {
			return LZ4StreamCompressionDecorator.INSTANCE;
		}
	};

	CompressionTypes(final byte value) {
		value_ = value;
	}

	/**
	 * Get the CompressionType enumeration value by passing the byte identifier to this method.
	 *
	 * @param byteIdentifier of CompressionType.
	 *
	 * @return CompressionType instance.
	 *
	 * @throws IllegalArgumentException If CompressionType cannot be found for the
	 *   provided byteIdentifier
	 */
	public static CompressionType getCompressionType(byte byteIdentifier) {
		for (final CompressionType compressionType : CompressionTypes.values()) {
			if (compressionType.getValue() == byteIdentifier) {
				return compressionType;
			}
		}

		throw new IllegalArgumentException(
			"Illegal value provided for CompressionType.");
	}

	/**
	 * <p>Returns the byte value of the enumerations value.</p>
	 *
	 * @return byte representation
	 */
	@Override
	public byte getValue() {
		return value_;
	}

	private final byte value_;

}
