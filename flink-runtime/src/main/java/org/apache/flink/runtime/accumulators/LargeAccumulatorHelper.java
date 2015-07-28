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

package org.apache.flink.runtime.accumulators;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.util.SerializedValue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LargeAccumulatorHelper {

	/**
	 * Puts the blobs of the large accumulators on the BlobCache.
	 * @param blobServerAddress the address of the server to the blobCache.
	 * @param accumulators the accumulators to be stored in the cache.
	 * @return the name of each accumulator with the BlobKey that identifies its blob in the BlobCache.
	 * */
	public static Map<String, List<BlobKey>> storeAccumulatorsToBlobCache(InetSocketAddress blobServerAddress,
																	Map<String, Accumulator<?, ?>> accumulators) throws IOException {
		if (blobServerAddress == null) {
			throw new RuntimeException("Undefined Blob Server Address.");
		}
		if (accumulators.isEmpty()) {
			return Collections.emptyMap();
		}

		Map<String, SerializedValue<Object>> serializedAccumulators = serializeAccumulators(accumulators);
		return storeSerializedAccumulatorsToBlobCache(blobServerAddress, serializedAccumulators);
	}

	/**
	 * Puts the blobs of the large accumulators on the BlobCache.
	 * @param blobServerAddress the address of the server to the blobCache.
	 * @param accumulators the accumulators to be stored in the cache.
	 * @return the name of each accumulator with the BlobKey that identifies its blob in the BlobCache.
	 * */
	public static Map<String, List<BlobKey>> storeSerializedAccumulatorsToBlobCache(InetSocketAddress blobServerAddress,
																		Map<String, SerializedValue<Object>> accumulators) throws IOException {
		if (blobServerAddress == null) {
			throw new RuntimeException("Undefined Blob Server Address.");
		}
		if (accumulators.isEmpty()) {
			return Collections.emptyMap();
		}

		Map<String, List<BlobKey>> keys = new HashMap<String, List<BlobKey>>();

		BlobClient bc = null;
		try {
			bc = new BlobClient(blobServerAddress);

			for (Map.Entry<String, SerializedValue<Object>> entry : accumulators.entrySet()) {

				String accumulatorName = entry.getKey();
				byte[] accumulatorPayload = entry.getValue().getSerializedData();
				if(accumulatorPayload != null) {
					BlobKey blobKey = bc.put(accumulatorPayload);
					List<BlobKey> accKeys = keys.get(accumulatorName);
					if (accKeys == null) {
						accKeys = new ArrayList<BlobKey>();
					}
					accKeys.add(blobKey);
					keys.put(accumulatorName, accKeys);
				}
			}
		} catch (IOException e) {
			throw new IOException("Failed to send oversized accumulators to the BlobCache: ", e);
		} finally {
			try {
				if(bc != null) {
					bc.close();
				}
			} catch (IOException e) {
				throw new IOException("Failed to close BlobClient: ", e);
			}

		}
		return keys;
	}

	private static Map<String, SerializedValue<Object>> serializeAccumulators(Map<String, Accumulator<?, ?>> accumulators) throws IOException {
		if (accumulators.isEmpty()) {
			return Collections.emptyMap();
		}

		Map<String, SerializedValue<Object>> serializedAccumulators =
				new HashMap<String, SerializedValue<Object>>();
		try {
			for (Map.Entry<String, Accumulator<?, ?>> entry : accumulators.entrySet()) {
				String accumulatorName = entry.getKey();
				Accumulator<?, ?> accumulator = entry.getValue();
				serializedAccumulators.put(accumulatorName, new SerializedValue<Object>(accumulator));
			}
		} catch (IOException e) {
			throw new IOException("Failed to serialize accumulators.", e);
		}
		return serializedAccumulators;
	}
}
