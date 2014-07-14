package eu.stratosphere.streaming.util;

import java.io.IOException;
import java.io.File;

public class TestDataUtil {

	public static void downloadIfNotExists(String fileName) {
		String testDataDir = "";
		File file = new File(testDataDir + fileName);
		String testRepoUrl = "info.ilab.sztaki.hu/~mbalassi/flink-streaming/testdata/";

		if (file.exists()){
			System.out.println(fileName +" already exists");
		} else {
			System.out.println("downloading " + fileName);
			try {
				String myCommand = "wget -O " + testDataDir	+ fileName + " " + testRepoUrl + fileName;
				System.out.println(myCommand);
				Runtime.getRuntime().exec(myCommand);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
