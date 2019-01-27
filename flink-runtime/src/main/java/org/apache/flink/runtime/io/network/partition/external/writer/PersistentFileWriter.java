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

package org.apache.flink.runtime.io.network.partition.external.writer;

import org.apache.flink.runtime.io.network.partition.external.PersistentFileType;
import org.apache.flink.runtime.io.network.partition.external.PartitionIndex;

import java.io.IOException;
import java.util.List;

/**
 * Writes records to the persistent files which are transported by the shuffle service.
 */
public interface PersistentFileWriter<T> {

	/**
	 * Adds a record in the given partition.
	 */
	void add(T record, int targetPartition) throws IOException, InterruptedException;

	/**
	 * Adds a record in the given partitions.
	 */
	void add(T record, int[] targetPartitions) throws IOException, InterruptedException;

	/**
	 * Notifies all the records have been written.
	 */
	void finish() throws IOException, InterruptedException;

	/**
	 * Acquires the partition indices of written files.
	 */
	List<List<PartitionIndex>> generatePartitionIndices() throws IOException, InterruptedException;

	/**
	 * Releases all the resources.
	 */
	void clear() throws IOException;

	/**
	 * Gets the type of the files.
	 */
	PersistentFileType getExternalFileType();
}
