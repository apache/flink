/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.rocksdb.CompactionStyle;
import org.rocksdb.Options;

/**
 * The {@code PredefinedOptions} are configuration settings for the {@link RocksDBStateBackend}. 
 * The various pre-defined choices are configurations that have been empirically
 * determined to be beneficial for performance under different settings.
 * 
 * <p>Some of these settings are based on experiments by the Flink community, some follow
 * guides from the RocksDB project.
 */
public enum PredefinedOptions {

	/**
	 * Default options for all settings.
	 */
	DEFAULT {
		
		@Override
		public Options createOptions() {
			return new Options();
		}
	},

	/**
	 * Pre-defined options for regular spinning hard disks.
	 * 
	 * <p>This constant configures RocksDB with some options that lead empirically
	 * to better performance when the machines executing the system use
	 * regular spinning hard disks. The following options are set:
	 * <ul>
	 *     <li>Optimized level-style compactions</li>
	 * </ul>
	 */
	SPINNING_DISK_OPTIMIZED {

		@Override
		public Options createOptions() {
			return new Options()
					.setCompactionStyle(CompactionStyle.LEVEL)
					.optimizeLevelStyleCompaction();
		}
	},

	/**
	 * Pre-defined options for Flash SSDs.
	 *
	 * <p>This constant configures RocksDB with some options that lead empirically
	 * to better performance when the machines executing the system use SSDs.
	 * The following options are set:
	 * <ul>
	 *     <li>none</li>
	 * </ul>
	 */
	FLASH_SSD_OPTIMIZED {

		@Override
		public Options createOptions() {
			return new Options();
		}
	};
	
	// ------------------------------------------------------------------------

	/**
	 * Creates the options for this pre-defined setting.
	 * 
	 * @return The pre-defined options object. 
	 */
	public abstract Options createOptions();
}
