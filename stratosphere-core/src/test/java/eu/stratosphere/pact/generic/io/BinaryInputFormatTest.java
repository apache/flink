package eu.stratosphere.pact.generic.io;

import java.io.DataInput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.LogUtils;

public class BinaryInputFormatTest {
	
	private static final class MyBinaryInputFormat extends BinaryInputFormat<PactRecord> {

		private static final long serialVersionUID = 1L;

		@Override
		protected void deserialize(PactRecord record, DataInput dataInput) throws IOException {}
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
		
		final BinaryInputFormat<PactRecord> inputFormat = new MyBinaryInputFormat();
		inputFormat.setFilePath(tempFile.toURI().toString());
		
		inputFormat.configure(config);
		
		FileInputSplit[] inputSplits = inputFormat.createInputSplits(numBlocks);
		
		Assert.assertEquals("Returns requested numbers of splits.", numBlocks, inputSplits.length);
		Assert.assertEquals("1. split has block size length.", blockSize, inputSplits[0].getLength());
		Assert.assertEquals("2. split has block size length.", blockSize, inputSplits[1].getLength());
		Assert.assertEquals("3. split has block size length.", blockSize, inputSplits[2].getLength());
	}
	
}
