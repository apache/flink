package eu.stratosphere.pact.common.io;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.pact.common.type.PactRecord;

public class TextInputFormatTest {
	
	/**
	 * The TextInputFormat seems to fail reading more than one record. I guess its
	 * an off by one error.
	 * 
	 * The easiest workaround is to setParameter(TextInputFormat.CHARSET_NAME, "ASCII");
	 * @throws IOException
	 */
	@Test
	public void testPositionBug() throws IOException {
		// create input file
		File tempFile = File.createTempFile("TextInputFormatTest", null);
		tempFile.setWritable(true);
		PrintStream ps = new  PrintStream(tempFile);
		ps.println("First line");
		ps.println("Second line");
		ps.close();
		tempFile.deleteOnExit();
		
		TextInputFormat inputFormat = new TextInputFormat();
		Configuration parameters = new Configuration();
		parameters.setString(FileInputFormat.FILE_PARAMETER_KEY, "file://"+tempFile.getAbsolutePath() ); 
		inputFormat.configure(parameters);
		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		assertTrue("expected at least one input split", splits.length >= 1);
		inputFormat.open(splits[0]);
		PactRecord r = new PactRecord();
		assertTrue("Expecting first record here",inputFormat.nextRecord(r ));
		try {
			assertTrue("Expecting second record here",inputFormat.nextRecord(r ));
		} catch(IllegalArgumentException iae) {
			iae.printStackTrace();
			fail("TextInputFormat is unable to read the file");
		}
		assertFalse("The input file is over", inputFormat.nextRecord(r));
		
	}
}
