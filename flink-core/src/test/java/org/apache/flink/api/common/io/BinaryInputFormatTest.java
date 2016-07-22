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


package org.apache.flink.api.common.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.types.Record;
import org.junit.Assert;
import org.junit.Test;

public class BinaryInputFormatTest {
	
	private static final class MyBinaryInputFormat extends BinaryInputFormat<Record> {

		private static final long serialVersionUID = 1L;

		@Override
		protected Record deserialize(Record record, DataInputView dataInput) {
			return record;
		}
	}
	
	@Test
	public void testCreateInputSplitsWithOneFile() throws IOException {
		// create temporary file with 3 blocks
		final File tempFile = File.createTempFile("binary_input_format_test", "tmp");
		tempFile.deleteOnExit();
		final int blockInfoSize = new BlockInfo().getInfoSize();
		final int blockSize = blockInfoSize + 8;
		final int numBlocks = 3;
		FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
		for(int i = 0; i < blockSize * numBlocks; i++) {
			fileOutputStream.write(new byte[]{1});
		}
		fileOutputStream.close();

		final Configuration config = new Configuration();
		config.setLong("input.block_size", blockSize + 10);

		final BinaryInputFormat<Record> inputFormat = new MyBinaryInputFormat();
		inputFormat.setFilePath(tempFile.toURI().toString());
		inputFormat.setBlockSize(blockSize);
		
		inputFormat.configure(config);
		
		FileInputSplit[] inputSplits = inputFormat.createInputSplits(numBlocks);
		
		Assert.assertEquals("Returns requested numbers of splits.", numBlocks, inputSplits.length);
		Assert.assertEquals("1. split has block size length.", blockSize, inputSplits[0].getLength());
		Assert.assertEquals("2. split has block size length.", blockSize, inputSplits[1].getLength());
		Assert.assertEquals("3. split has block size length.", blockSize, inputSplits[2].getLength());
	}
	
}
