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

package org.apache.flink.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

/**
 * This is a utility class to deal with temporary files.
 */
public final class FileUtils {

	/**
	 * The alphabet to construct the random part of the filename from.
	 */
	private static final char[] ALPHABET = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b', 'c', 'd',
		'e', 'f' };

	/**
	 * The length of the random part of the filename.
	 */
	private static final int LENGTH = 12;

	

	/**
	 * Constructs a random filename with the given prefix and
	 * a random part generated from hex characters.
	 * 
	 * @param prefix
	 *        the prefix to the filename to be constructed
	 * @return the generated random filename with the given prefix
	 */
	public static String getRandomFilename(final String prefix) {

		final StringBuilder stringBuilder = new StringBuilder(prefix);

		for (int i = 0; i < LENGTH; i++) {
			stringBuilder.append(ALPHABET[(int) Math.floor(Math.random() * (double) ALPHABET.length)]);
		}

		return stringBuilder.toString();
	}
	
	// ------------------------------------------------------------------------
	//  Simple reading and writing of files
	// ------------------------------------------------------------------------
	
	public static String readFile(File file, String charsetName) throws IOException {
		byte[] bytes = Files.readAllBytes(file.toPath());
		return new String(bytes, charsetName);
	}

	public static String readFileUtf8(File file) throws IOException {
		return readFile(file, "UTF-8");
	}

	public static void writeFile(File file, String contents, String encoding) throws IOException {
		byte[] bytes = contents.getBytes(encoding);
		Files.write(file.toPath(), bytes, StandardOpenOption.WRITE);
	}
	
	public static void writeFileUtf8(File file, String contents) throws IOException {
		writeFile(file, contents, "UTF-8");
	}
	
	// ------------------------------------------------------------------------

	/**
	 * Private default constructor to avoid instantiation.
	 */
	private FileUtils() {}
}
