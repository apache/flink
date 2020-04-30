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

package org.apache.flink.testutils;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

public class TestFileUtils {
	
	private static final String FILE_PREFIX = "flink_test_";
	
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
	
	public static String createTempFileInDirectory(String dir, String contents) throws IOException {
		File f;
		do {
			f = new File(dir + "/" + randomFileName());
		} while (f.exists());
		f.getParentFile().mkdirs();
		f.createNewFile();
		f.deleteOnExit();

		try (BufferedWriter out = new BufferedWriter(new FileWriter(f))) {
			out.write(contents);
		}
		return f.toURI().toString();
	}

	public static String createTempFileInDirectory(String dir, long bytes) throws IOException {
		File f;
		do {
			f = new File(dir + "/" + randomFileName());
		} while (f.exists());
		f.getParentFile().mkdirs();
		f.createNewFile();
		f.deleteOnExit();

		try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(f))) {
			for (; bytes > 0; bytes--) {
				out.write(0);
			}
		}
		return f.toURI().toString();
	}

	public static String createTempFile(String contents) throws IOException {
		File f = File.createTempFile(FILE_PREFIX, FILE_SUFFIX);
		f.deleteOnExit();

		try (BufferedWriter out = new BufferedWriter(new FileWriter(f))) {
			out.write(contents);
		}
		return f.toURI().toString();
	}
	
	// ------------------------------------------------------------------------

	public static String createTempFileDir(File tempDir, long ... bytes) throws IOException {
		File f = null;
		do {
			f = new File(tempDir, randomFileName());
		} while (f.exists());
		f.mkdirs();
		f.deleteOnExit();
		
		for (long l : bytes) {
			File child = new File(f, randomFileName());
			child.deleteOnExit();

			try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(child))) {
				for (; l > 0; l--) {
					out.write(0);
				}
			}
		}
		return f.toURI().toString();
	}
	
	public static String createTempFileDir(File tempDir, String ... contents) throws IOException {
		return createTempFileDirExtension(tempDir, FILE_SUFFIX, contents);
	}
	
	public static String createTempFileDirExtension(File tempDir, String fileExtension, String ... contents ) throws IOException {
		File f = null;
		do {
			f = new File(tempDir, randomFileName(FILE_SUFFIX));
		} while (f.exists());
		f.mkdirs();
		f.deleteOnExit();
		
		for (String s : contents) {
			File child = new File(f, randomFileName(fileExtension));
			child.deleteOnExit();

			try (BufferedWriter out = new BufferedWriter(new FileWriter(child))) {
				out.write(s);
			}
		}
		return f.toURI().toString();
	}
	
	public static String randomFileName() {
		return randomFileName(FILE_SUFFIX);
	}
	public static String randomFileName(String fileSuffix) {
		return FILE_PREFIX + ((int) (Math.random() * Integer.MAX_VALUE)) + fileSuffix;
	}

	// ------------------------------------------------------------------------
	
	private TestFileUtils() {}
}
