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

package org.apache.flink.kubernetes.utils;

import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Utils for retrying to execute.
 */
public class RetryUtils {

	private static final Logger LOG = LoggerFactory.getLogger(RetryUtils.class);

	public static <T> Object run(Supplier<T> function, int retryTimes, long retryIntervalMs)
		throws ResourceManagerException {
		try {
			return function.get();
		} catch (Exception e) {
			LOG.error("Run failed, will be retried {} times with interval {} ms.",
				retryTimes, retryIntervalMs, e);
			return retry(function, retryTimes, retryIntervalMs);
		}
	}

	public static <T> Object retry(Supplier<T> function, int retryCount, long retryIntervalMs)
		throws ResourceManagerException {
		while (0 < retryCount) {
			try {
				return function.get();
			} catch (Exception e) {
				retryCount--;
				if (retryCount == 0) {
					LOG.error("Retry failed and reached the maximum retried times.");
					throw new ResourceManagerException(e);
				}
				LOG.error("Retry failed, will retry {} times with interval {} ms.", retryCount, retryIntervalMs, e);
				try {
					Thread.sleep(retryIntervalMs);
				} catch (InterruptedException e1) {
					throw new ResourceManagerException(e1);
				}
			}
		}
		return null;
	}

}
