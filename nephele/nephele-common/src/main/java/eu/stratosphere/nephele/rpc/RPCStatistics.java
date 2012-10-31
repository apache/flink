/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.rpc;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.esotericsoftware.minlog.Log;

/**
 * The RPCStatistics class collects statistics on the operation of the RPC service, e.g. packet loss, request retries,
 * etc.
 * <p>
 * This class is thread safe.
 * 
 * @author warneke
 */
public class RPCStatistics {

	/**
	 * The base timeout interval in milliseconds.
	 */
	private static final int BASE_TIMEOUT = 100;

	/**
	 * Auxiliary class to hold the statistics data for a particular number of packets.
	 * <p>
	 * This class is thread-safe.
	 * 
	 * @author warneke
	 */
	private static final class RPCStatisticsData {

		/**
		 * The number of packets this object has been created for.
		 */
		private final int numberOfPackets;

		/**
		 * The minimum number of retries reported in the current collection interval.
		 */
		private final AtomicInteger minRetries = new AtomicInteger(Integer.MAX_VALUE);

		/**
		 * The method name for which the minimum number of retries has been reported.
		 */
		private volatile String minMethodName = null;

		/**
		 * The maximum number of retries reported in the current collection interval.
		 */
		private final AtomicInteger maxRetries = new AtomicInteger(Integer.MIN_VALUE);

		/**
		 * The method name for which the maximum number of retries has been reported.
		 */
		private volatile String maxMethodName = null;

		/**
		 * The number of reported requests in the current collection interval.
		 */
		private final AtomicInteger requestCounter = new AtomicInteger(0);

		/**
		 * The sum of required retries in the current collection interval.
		 */
		private final AtomicInteger sumOfRetries = new AtomicInteger(0);

		/**
		 * Creates a new RPC statistics data object.
		 * 
		 * @param numberOfPackets
		 *        the number of packets this object is created for
		 */
		private RPCStatisticsData(final int numberOfPackets) {
			this.numberOfPackets = numberOfPackets;
		}

		/**
		 * Reports the number of required retries for an RPC call.
		 * 
		 * @param methodName
		 *        the name of the remote method that has been called
		 * @param requiredRetries
		 *        the number of required retries
		 */
		private final void reportNumberOfRequiredRetries(final String methodName, final int requiredRetries) {

			this.requestCounter.incrementAndGet();
			this.sumOfRetries.addAndGet(requiredRetries);
			testAndSetMin(methodName, requiredRetries);
			testAndSetMax(methodName, requiredRetries);
		}

		/**
		 * Sets the minimum number of required retries in a thread-safe way.
		 * 
		 * @param methodName
		 *        the method name
		 * @param requiredRetries
		 *        the number of required retries
		 */
		private void testAndSetMin(final String methodName, final int requiredRetries) {

			while (true) {

				final int val = this.minRetries.get();
				if (requiredRetries < val) {
					if (!this.minRetries.compareAndSet(val, requiredRetries)) {
						continue;
					}

					this.minMethodName = methodName;
				}

				break;
			}
		}

		/**
		 * Sets the maximum number of required retries in a thread-safe way.
		 * 
		 * @param methodName
		 *        the method name
		 * @param requiredRetries
		 *        the number of required retries
		 */
		private void testAndSetMax(final String methodName, final int requiredRetries) {

			while (true) {

				final int val = this.maxRetries.get();
				if (requiredRetries > val) {
					if (!this.maxRetries.compareAndSet(val, requiredRetries)) {
						continue;
					}

					this.maxMethodName = methodName;
				}

				break;
			}
		}

		/**
		 * Processes and logs the collected statistics data.
		 */
		private void processCollectedData() {

			if (Log.DEBUG) {

				final int numberOfRequests = this.requestCounter.get();
				if (numberOfRequests == 0) {
					return;
				}

				final float avg = (float) this.sumOfRetries.get() / (float) numberOfRequests;

				final StringBuilder sb = new StringBuilder();
				sb.append(this.numberOfPackets);
				sb.append("\t: ");
				sb.append(avg);
				sb.append(" (min ");
				sb.append(this.minMethodName);
				sb.append(' ');
				sb.append(this.minRetries.get());
				sb.append(", max ");
				sb.append(this.maxMethodName);
				sb.append(' ');
				sb.append(this.maxRetries.get());
				sb.append(')');

				Log.debug(sb.toString());
			}
		}
	}

	/**
	 * Map storing the collected statistics data by the number of packets the request consisted of.
	 */
	private final ConcurrentMap<Integer, RPCStatisticsData> statisticsData = new ConcurrentHashMap<Integer, RPCStatisticsData>();

	/**
	 * Calculates the timeout in milliseconds for a request consisting of the given number of packets and the given
	 * retry.
	 * 
	 * @param numberOfPackets
	 *        the number of packets the request consists of
	 * @param retry
	 *        the value of the requests retry counter
	 * @return the calculated timeout in milliseconds
	 */
	int calculateTimeout(int numberOfPackets, int retry) {

		return BASE_TIMEOUT;
	}

	/**
	 * Reports the number of required retries for a successful RPC call to the statistics module.
	 * 
	 * @param methodName
	 *        the name of the remote method that has been invoked as part of the RPC call
	 * @param numberOfPackets
	 *        the number of packets the request consisted of
	 * @param requiredRetries
	 *        the number of required retries before the call's response has been received
	 */
	void reportSuccessfulCall(final String methodName, final int numberOfPackets, final int requiredRetries) {

		final Integer key = Integer.valueOf(numberOfPackets);
		RPCStatisticsData data = this.statisticsData.get(key);
		if (data == null) {
			data = new RPCStatisticsData(numberOfPackets);
			final RPCStatisticsData oldValue = this.statisticsData.putIfAbsent(key, data);
			if (oldValue != null) {
				data = oldValue;
			}
		}

		data.reportNumberOfRequiredRetries(methodName, requiredRetries);
	}

	/**
	 * Triggers the periodic processing the of the collected data.
	 */
	void processCollectedData() {

		final Iterator<RPCStatisticsData> it = this.statisticsData.values().iterator();
		while (it.hasNext()) {
			final RPCStatisticsData data = it.next();
			it.remove();
			data.processCollectedData();
		}
	}
}
