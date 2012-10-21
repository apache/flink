/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.util;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This is a utility class to deal with temporary files.
 * 
 * @author warneke
 */
public final class FileUtils {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(FileUtils.class);

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
	 * Empty private constructor to avoid instantiation.
	 */
	private FileUtils() {
	}

	/**
	 * Constructs a random filename with the given prefix and a random part generated from hex characters.
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

	/**
	 * Silently deletes the given file, i.e. any exception is dropped and logged on a debug level.
	 * 
	 * @param file
	 *        the file to delete, possibly <code>null</code>
	 */
	public static void deleteSilently(final File file) {

		if (file == null) {
			return;
		}

		try {
			file.delete();
		} catch (SecurityException se) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(StringUtils.stringifyException(se));
			}
		}
	}
}
