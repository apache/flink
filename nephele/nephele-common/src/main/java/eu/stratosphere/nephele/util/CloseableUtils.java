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

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class is a utility class to deal with {@link Closeable} objects.
 * 
 * @author warneke
 */
public class CloseableUtils {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(CloseableUtils.class);

	/**
	 * private constructor to prevent instantiation.
	 */
	private CloseableUtils() {
	}

	/**
	 * Silently closes the given {@link Closeable} object, i.e. a potential {@link IOException} that occurs while
	 * calling the close method is silently dropped.
	 * 
	 * @param closeable
	 *        the object to close, possibly <code>null</code>
	 */
	public static void closeSilently(final Closeable closeable) {

		if (closeable == null) {
			return;
		}

		try {
			closeable.close();
		} catch (IOException ioe) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(StringUtils.stringifyException(ioe));
			}
		}
	}
}
