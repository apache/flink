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

package org.apache.flink.table.runtime.functions.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Md5 function util.
 */
public class Md5Utils {
	public static final Logger LOG = LoggerFactory.getLogger(Md5Utils.class);
	private static final char[] HEX_CHAR = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

	private static String toHexString(byte[] b) {
		StringBuilder sb = new StringBuilder(b.length * 2);

		for (int i = 0; i < b.length; ++i) {
			sb.append(HEX_CHAR[(b[i] & 240) >>> 4]);
			sb.append(HEX_CHAR[b[i] & 15]);
		}

		return sb.toString();
	}

	public static String md5sum(byte[] b) {
		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			md5.update(b, 0, b.length);
			return toHexString(md5.digest());
		} catch (NoSuchAlgorithmException var3) {
			LOG.error("No such algorithm: MD5", var3);
			return "";
		}
	}

	public static String md5sum(String str) {
		if (str != null && !"".equals(str)) {
			Object b = null;

			byte[] b1;
			try {
				b1 = str.getBytes("UTF-8");
			} catch (UnsupportedEncodingException var5) {
				LOG.error("Unsupported encoding: UTF-8", var5);
				return null;
			}

			try {
				MessageDigest md5 = MessageDigest.getInstance("MD5");
				md5.update(b1, 0, b1.length);
				return toHexString(md5.digest());
			} catch (NoSuchAlgorithmException var4) {
				LOG.error("No such algorithm: MD5", var4);
				return null;
			}

		} else {
			return "";
		}
	}
}
