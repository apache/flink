package eu.stratosphere.streaming.util;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

public class TestDataUtilTest {

	@Test
	public void testDownload() throws FileNotFoundException, IOException {
		String fileToDownload = "hamlet.txt";
		String expectedFile = "hamletTestExpectation.txt";

		deleteFile(TestDataUtil.testDataDir + fileToDownload);
		
		TestDataUtil.download(fileToDownload);

		assertTrue(compareFile(TestDataUtil.testDataDir + expectedFile, TestDataUtil.testDataDir
				+ fileToDownload));
	}

	public void deleteFile(String fileLocation) throws IOException{
		Path path = Paths.get(fileLocation);
		if(Files.exists(path))
			Files.delete(path);
	}

	public boolean compareFile(String file1, String file2) throws FileNotFoundException,
			IOException {

		BufferedReader myInput1 = new BufferedReader(new InputStreamReader(new FileInputStream(file1)));
		BufferedReader myInput2 = new BufferedReader(new InputStreamReader(new FileInputStream(file2)));

		String line1, line2;
		while ((line1 = myInput1.readLine()) != null && (line2 = myInput2.readLine()) != null) {
			if (!line1.equals(line2))
				return false;
		}
		return true;
	}
}