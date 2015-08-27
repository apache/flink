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
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobServerProtocol;
import org.apache.flink.runtime.util.SerializedValue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains general methods that help at handling oversized accumulators
 * that are stored and fetched from the BlobCache, instead of being sent directly
 * through akka and stored in memory.
 * */
public class LargeAccumulatorHelper {

	/**
	 * When the result of the job contains oversized (i.e. bigger than
	 * {@link org.apache.flink.runtime.akka.AkkaUtils#getLargeAccumulatorThreshold(
			org.apache.flink.configuration.Configuration)} bytes)
	 * accumulators then these are put in the BlobCache for the client to fetch and merge.
	 * This method serializes and stores the large accumulators in the BlobCache.
	 *
	 * @param blobServerAddress the address of the server to the blobCache.
	 * @param accumulators      the accumulators to be stored in the cache.
	 * @return the name of each accumulator with the associated BlobKey that identifies
	 *         its blob in the BlobCache.
	 */
	public static Map<String, List<BlobKey>> storeAccumulatorsToBlobCache(
			InetSocketAddress blobServerAddress,
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
	 * When the result of the job contains oversized (i.e. bigger than
	 * {@link org.apache.flink.runtime.akka.AkkaUtils#getLargeAccumulatorThreshold(
	         org.apache.flink.configuration.Configuration)} bytes)
	 * accumulators then these are put in the BlobCache for the client to fetch and merge.
	 * This method stores the blobs of the large accumulators in the BlobCache. Contrary to
	 * {@link LargeAccumulatorHelper#storeAccumulatorsToBlobCache(InetSocketAddress, Map)}, this
	 * method assumes that accumulators are already serialized.
	 * @param blobServerAddress the address of the server to the blobCache.
	 * @param accumulators      a map with the names and the (serialized) accumulators to be
	 *                          stored in the BlobCache.
	 * @return the name of each accumulator with the associated BlobKey that identifies
	 * its blob in the BlobCache.
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
				if (accumulatorPayload != null) {
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
				if (bc != null) {
					bc.close();
				}
			} catch (IOException e) {
				throw new IOException("Failed to close BlobClient: ", e);
			}

		}
		return keys;
	}

	/**
	 * When the result of the job contains oversized (i.e. bigger than
	 * {@link org.apache.flink.runtime.akka.AkkaUtils#getLargeAccumulatorThreshold(
	         org.apache.flink.configuration.Configuration)} bytes)
	 * accumulators then these are put in the BlobCache for the client to fetch and merge.
	 * This method gets, deserializes, and merges these oversized user-defined accumulators.
	 *
	 * @param blobServerAddress the address that the BlobCache is listening to.
	 * @param keys the blob keys to fetch.
	 * @param loader the classloader used to deserialize the accumulators fetched.
	 * @return the accumulators, grouped by name.
	 * */
	public static Map<String, Accumulator<?, ?>> getDeserializeAndMergeAccumulatorsFromBlobCache(
			InetSocketAddress blobServerAddress, Map<String, List<BlobKey>> keys, ClassLoader loader)
			throws IOException, ClassNotFoundException {

		if (blobServerAddress == null) {
			throw new RuntimeException("Undefined Blob Server Address.");
		}

		if (keys.isEmpty()) {
			return Collections.emptyMap();
		}

		Map<String, Accumulator<?, ?>> accumulators =
				new HashMap<String, Accumulator<?, ?>>();

		Map<String, List<SerializedValue<Object>>> accumulatorBlobs =
				getSerializedAccumulatorsFromBlobCache(blobServerAddress, keys);

		for (String accumulatorName : accumulatorBlobs.keySet()) {
			Accumulator<?, ?> existing = accumulators.get(accumulatorName);

			for (SerializedValue<Object> acc : accumulatorBlobs.get(accumulatorName)) {
				Accumulator<?, ?> accumulator = (Accumulator) acc.deserializeValue(loader);
				if(existing == null) {
					existing = accumulator;
					accumulators.put(accumulatorName, existing);
				} else {
					AccumulatorHelper.mergeAccumulators(accumulatorName, existing, accumulator);
				}
			}
		}
		return accumulators;
	}

	/**
	 * When the result of the job contains oversized (i.e. bigger than
	 * {@link org.apache.flink.runtime.akka.AkkaUtils#getLargeAccumulatorThreshold(
			org.apache.flink.configuration.Configuration)} bytes)
	 * accumulators then these are put in the BlobCache for the client to fetch and merge.
	 * This methos gets the user-defined accumulators from the BlobCache and returns them in
	 * serialized form. Contrary to
	 * {@link LargeAccumulatorHelper#getDeserializeAndMergeAccumulatorsFromBlobCache(
	     InetSocketAddress, Map, ClassLoader)}, this method does nothing more than fetching
	 * the serialized data.
	 *
	 * @param blobServerAddress the address that the BlobCache is listening to.
	 * @param keys the blob keys to fetch.
	 * @return the accumulators, grouped by name.
	 */
	public static Map<String, List<SerializedValue<Object>>> getSerializedAccumulatorsFromBlobCache(
			InetSocketAddress blobServerAddress, Map<String, List<BlobKey>> keys) throws IOException {

		if (blobServerAddress == null) {
			throw new RuntimeException("Undefined Blob Server Address.");
		}

		if (keys.isEmpty()) {
			return Collections.emptyMap();
		}

		Map<String, List<SerializedValue<Object>>> accumulatorBlobs =
				new HashMap<String, List<SerializedValue<Object>>>();

		BlobClient bc = new BlobClient(blobServerAddress);

		final byte[] buf = new byte[BlobServerProtocol.BUFFER_SIZE];
		for (String accName : keys.keySet()) {
			List<BlobKey> accBlobKeys = keys.get(accName);
			List<SerializedValue<Object>> accBlobs = new ArrayList<SerializedValue<Object>>();

			for (BlobKey bk : accBlobKeys) {
				InputStream is = bc.get(bk);
				ByteArrayOutputStream os = new ByteArrayOutputStream();
				while (true) {
					final int read = is.read(buf);
					if (read < 0) {
						break;
					}
					os.write(buf, 0, read);
				}
				os.flush();
				byte[] blob = os.toByteArray();
				accBlobs.add(new SerializedValue<Object>(blob));
				is.close();
				os.close();

				// after getting them, clean up and delete the blobs from the BlobCache.
				bc.delete(bk);
			}
			accumulatorBlobs.put(accName, accBlobs);
		}
		bc.close();
		return accumulatorBlobs;
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
