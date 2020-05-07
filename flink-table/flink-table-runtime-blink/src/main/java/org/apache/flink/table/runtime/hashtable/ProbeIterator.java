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

package org.apache.flink.table.runtime.hashtable;

import org.apache.flink.runtime.io.disk.ChannelReaderInputViewIterator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;

import java.io.IOException;

/**
 * Probe iterator from probe or spilled partition.
 */
public final class ProbeIterator {

	private ChannelReaderInputViewIterator<BinaryRowData> source;

	private RowData instance;
	private BinaryRowData reuse;

	public ProbeIterator(BinaryRowData instance) {
		this.instance = instance;
	}

	public void set(ChannelReaderInputViewIterator<BinaryRowData> source) {
		this.source = source;
	}

	public void setReuse(BinaryRowData reuse) {
		this.reuse = reuse;
	}

	public BinaryRowData next() throws IOException {
		BinaryRowData retVal = this.source.next(reuse);
		if (retVal != null) {
			this.instance = retVal;
			return retVal;
		} else {
			return null;
		}
	}

	public RowData current() {
		return this.instance;
	}

	public void setInstance(RowData instance) {
		this.instance = instance;
	}

	public boolean hasSource() {
		return source != null;
	}
}
