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

package org.apache.flink.runtime.util;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for {@link java.lang.management.ManagementFactory}.
 */
public final class JvmUtils {

	/**
	 * Returns the thread info for all live threads with stack trace and synchronization information.
	 *
	 * @return the thread dump stream of current JVM
	 */
	public static InputStream threadDumpStream() {
		ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();

		List<InputStream> streams = Arrays
			.stream(threadMxBean.dumpAllThreads(true, true))
			.map((v) -> v.toString().getBytes(StandardCharsets.UTF_8))
			.map(ByteArrayInputStream::new)
			.collect(Collectors.toList());

		return new SequenceInputStream(Collections.enumeration(streams));
	}

	/**
	 * Private default constructor to avoid instantiation.
	 */
	private JvmUtils() {}

}
