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

package eu.stratosphere.nephele.io.compression;

/**
 * This enumeration contains the possible compression levels which can be assigned
 * to a byte-buffered channel.
 * 
 * @author warneke
 */
public enum CompressionLevel {

	/**
	 * No compression is applied to the channel.
	 */
	NO_COMPRESSION,

	/**
	 * Light compression is applied to the channel, reasonable when CPU load is fairly high.
	 */
	LIGHT_COMPRESSION,

	/**
	 * Medium compression is applied to the channel, good trade-off between CPU and I/O load.
	 */
	MEDIUM_COMPRESSION,

	/**
	 * Heavy compression is applied to the channel, this may cause significant computational overhead.
	 */
	HEAVY_COMPRESSION,

	/**
	 * Dynamic compression is applied to the channel, the compression rate is switched according to the current CPU
	 * load.
	 */
	DYNAMIC_COMPRESSION
}
