/**
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

package org.apache.flink.streaming.connectors.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

import java.io.IOException;

/**
 * A base class that compresses the input element and write them to the filesystem. Default serialization is to
 * write events separates by newline.
 * Extends the class and override write() to make custom writing
 */
public class CompressionStringWriter<T> extends StreamWriterBase<T> implements Writer<T>{
	private static final long serialVersionUID = 1L;

	// The codec name is the codec class name from org.apache.hadoop.io.compress.
	// Candidate: GzipCodec, Lz4Codec, SnappyCodec, BZip2Codec
	private String codecName;

	private String separator;

	public String getCodecName() {
		return codecName;
	}

	public String getSeparator() {
		return separator;
	}

	private transient CompressionOutputStream compressedOutputStream;

	public CompressionStringWriter(String codecName, String separator) {
		this.codecName = codecName;
		this.separator = separator;
	}

	public CompressionStringWriter(String codecName) {
		this(codecName, System.lineSeparator());
	}

	protected CompressionStringWriter(CompressionStringWriter<T> other) {
		super(other);
		this.codecName = other.codecName;
		this.separator = other.separator;
	}

	@Override
	public void open(FileSystem fs, Path path) throws IOException {
		super.open(fs, path);
		Configuration conf = fs.getConf();
		CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
		CompressionCodec codec = codecFactory.getCodecByName(codecName);
		if (codec == null) {
			throw new RuntimeException("Codec " + codecName + " not found");
		}
		Compressor compressor = CodecPool.getCompressor(codec, conf);
		compressedOutputStream = codec.createOutputStream(getStream(), compressor);

	}

	@Override
	public void close() throws IOException {
		if (compressedOutputStream != null) {
			compressedOutputStream.close();
			compressedOutputStream = null;
		} else {
			super.close();
		}
	}

	@Override
	public void write(Object element) throws IOException {
		getStream();
		compressedOutputStream.write(element.toString().getBytes());
		compressedOutputStream.write(this.separator.getBytes());
	}

	@Override
	public CompressionStringWriter<T> duplicate() {
		return new CompressionStringWriter<>(this);
	}
}
