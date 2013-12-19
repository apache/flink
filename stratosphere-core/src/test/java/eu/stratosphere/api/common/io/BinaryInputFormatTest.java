/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.common.io;

import java.io.DataInput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.api.common.io.BinaryInputFormat;
import eu.stratosphere.api.common.io.BlockInfo;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.LogUtils;

public class BinaryInputFormatTest {
	
	private static final class MyBinaryInputFormat extends BinaryInputFormat<Record> {

		private static final long serialVersionUID = 1L;

		@Override
		protected void deserialize(Record record, DataInput dataInput) throws IOException {}
	}
	
	@BeforeClass
	public static void initialize() {
		LogUtils.initializeDefaultConsoleLogger(Level.WARN);
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
		for(int i = 0; i < blockSize * numBlocks; i++)
			fileOutputStream.write(new byte[]{1});
		fileOutputStream.close();

		final Configuration config = new Configuration();
		config.setLong(BinaryInputFormat.BLOCK_SIZE_PARAMETER_KEY, blockSize);
		
		final BinaryInputFormat<Record> inputFormat = new MyBinaryInputFormat();
		inputFormat.setFilePath(tempFile.toURI().toString());
		
		inputFormat.configure(config);
		
		FileInputSplit[] inputSplits = inputFormat.createInputSplits(numBlocks);
		
		Assert.assertEquals("Returns requested numbers of splits.", numBlocks, inputSplits.length);
		Assert.assertEquals("1. split has block size length.", blockSize, inputSplits[0].getLength());
		Assert.assertEquals("2. split has block size length.", blockSize, inputSplits[1].getLength());
		Assert.assertEquals("3. split has block size length.", blockSize, inputSplits[2].getLength());
	}
	
}
