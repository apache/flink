/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.testutils;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

public class TestFileUtils {
	
	private static final String FILE_PREFIX = "pact_test_";
	
	private static final String FILE_SUFFIX = ".tmp";

	public static String createTempFile(long bytes) throws IOException {
		File f = File.createTempFile(FILE_PREFIX, FILE_SUFFIX);
		f.deleteOnExit();
		
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(f));
		try { 
			for (; bytes > 0; bytes--) {
				out.write(0);
			}
		} finally {
			out.close();
		}
		return f.toURI().toString();
	}
	
	public static String createTempFile(String contents) throws IOException {
		File f = File.createTempFile(FILE_PREFIX, FILE_SUFFIX);
		f.deleteOnExit();
		
		BufferedWriter out = new BufferedWriter(new FileWriter(f));
		try { 
			out.write(contents);
		} finally {
			out.close();
		}
		return f.toURI().toString();
	}
	
	// ------------------------------------------------------------------------

	public static String createTempFileDir(long ... bytes) throws IOException {
		File tempDir = new File(System.getProperty("java.io.tmpdir"));
		File f = null;
		do {
			f = new File(tempDir, randomFileName());
		} while (f.exists());
		f.mkdirs();
		f.deleteOnExit();
		
		for (long l : bytes) {
			File child = new File(f, randomFileName());
			child.deleteOnExit();
		
			BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(child));
			try { 
				for (; l > 0; l--) {
					out.write(0);
				}
			} finally {
				out.close();
			}
		}
		return f.toURI().toString();
	}
	
	public static String createTempFileDir(String ... contents) throws IOException {
		File tempDir = new File(System.getProperty("java.io.tmpdir"));
		File f = null;
		do {
			f = new File(tempDir, randomFileName());
		} while (f.exists());
		f.mkdirs();
		f.deleteOnExit();
		
		for (String s : contents) {
			File child = new File(f, randomFileName());
			child.deleteOnExit();
		
			BufferedWriter out = new BufferedWriter(new FileWriter(child));
			try { 
				out.write(s);
			} finally {
				out.close();
			}
		}
		return f.toURI().toString();
	}
	
	public static String randomFileName() {
		return FILE_PREFIX + ((int) (Math.random() * Integer.MAX_VALUE)) + FILE_SUFFIX;
	}

	// ------------------------------------------------------------------------
	
	private TestFileUtils() {}
}
