package eu.stratosphere.streaming.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;

public class TestDataUtil {
	
	public static final String testDataDir = "src/test/resources/testdata/";
	public static final String testRepoUrl = "info.ilab.sztaki.hu/~mbalassi/flink-streaming/testdata/";
	public static final String testChekSumDir = "src/test/resources/testdata_checksum/";
	
	public static void downloadIfNotExists(String fileName) {
		
		File file = new File(testDataDir + fileName);
		File checkFile = new File(testChekSumDir + fileName + ".md5");
		String checkSumDesired = new String();
		String checkSumActaul = new String();

		try {
			FileReader fileReader = new FileReader(checkFile);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			checkSumDesired = bufferedReader.readLine();
			bufferedReader.close();
			fileReader.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		if (file.exists()) {
			System.out.println(fileName + " already exists");
			try {
				checkSumActaul = DigestUtils.md5Hex(FileUtils.readFileToByteArray(file));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (!checkSumActaul.equals(checkSumDesired)) {
				download(fileName);
			}

		} else {
			download(fileName);
		}

	}

	public static void download(String fileName) {
		System.out.println("downloading " + fileName);
		try {
			String myCommand = "wget -O " + testDataDir + fileName + " " + testRepoUrl + fileName;
			System.out.println(myCommand);
			Runtime.getRuntime().exec(myCommand);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
