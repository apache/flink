/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions.utils;

import org.apache.flink.table.runtime.functions.ThreadLocalCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parse url function util.
 */
public class ParseUrlUtils {
	public static final Logger LOG = LoggerFactory.getLogger(ParseUrlUtils.class);
	public static ThreadLocalCache urlCache =
			new ThreadLocalCache<String, URL>(64) {
				public URL getNewInstance(String url) {
					try {
						return new URL(url);
					} catch (MalformedURLException e) {
						throw new RuntimeException(e);
					}
				}
			};
	public static String parseUrl(String urlStr, String partToExtract) {
		if (urlStr == null || partToExtract == null) {
			return null;
		}

		URL url;
		try {
			url = (URL) urlCache.get(urlStr);
		} catch (Exception e) {
			LOG.error("parse URL error: " + urlStr, e);
			return null;
		}
		if ("HOST".equals(partToExtract)) {
			return url.getHost();
		}
		if ("PATH".equals(partToExtract)) {
			return url.getPath();
		}
		if ("QUERY".equals(partToExtract)) {
			return url.getQuery();
		}
		if ("REF".equals(partToExtract)) {
			return url.getRef();
		}
		if ("PROTOCOL".equals(partToExtract)) {
			return url.getProtocol();
		}
		if ("FILE".equals(partToExtract)) {
			return url.getFile();
		}
		if ("AUTHORITY".equals(partToExtract)) {
			return url.getAuthority();
		}
		if ("USERINFO".equals(partToExtract)) {
			return url.getUserInfo();
		}

		return null;
	}

	public static String parseUrl(String urlStr, String partToExtract, String key) {
		if (!"QUERY".equals(partToExtract)) {
			return null;
		}

		String query = parseUrl(urlStr, partToExtract);
		if (query == null) {
			return null;
		}

		Pattern p = Pattern.compile("(&|^)" + Pattern.quote(key) + "=([^&]*)");
		Matcher m = p.matcher(query);
		if (m.find()) {
			return m.group(2);
		}
		return null;
	}
}
