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

package org.apache.flink.runtime.io.disk;

import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel.Enumerator;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel.ID;

import java.io.File;

/**
 * The manager used for creating/getting file IO channels based on config temp dirs.
 */
public interface FileChannelManager extends AutoCloseable {

	/**
	 * Creates an ID identifying an underlying file channel and returns it.
	 */
	ID createChannel();

	/**
	 * Creates an enumerator for channels that logically belong together and returns it.
	 */
	Enumerator createChannelEnumerator();

	/**
	 * Gets all the files corresponding to the config temp dirs.
	 */
	File[] getPaths();
}
