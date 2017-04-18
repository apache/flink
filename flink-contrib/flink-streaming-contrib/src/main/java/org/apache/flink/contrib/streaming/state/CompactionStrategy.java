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

package org.apache.flink.contrib.streaming.state;

import java.io.Serializable;

public class CompactionStrategy implements Serializable {

	private static final long serialVersionUID = 1L;

	public final boolean enabled;
	public final int frequency;
	public final boolean compactFromBeginning;

	public CompactionStrategy(int frequency, boolean compactFromBeginning) {
		if (frequency > 0) {
			this.enabled = true;
			this.frequency = frequency;
			this.compactFromBeginning = compactFromBeginning;
		} else {
			this.enabled = false;
			this.frequency = 0;
			this.compactFromBeginning = false;
		}
	}

	public static CompactionStrategy disabled() {
		return new CompactionStrategy(-1, false);
	}
}
