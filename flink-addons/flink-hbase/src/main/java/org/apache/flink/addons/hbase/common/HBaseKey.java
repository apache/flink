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


package org.apache.flink.addons.hbase.common;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Key;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Simple wrapper to encapsulate an HBase h{@link ImmutableBytesWritable} as a Key
 */
public class HBaseKey implements Key<HBaseKey> {

	private static final long serialVersionUID = 1L;

	private ImmutableBytesWritable writable;
	

	public HBaseKey() {
		this.writable = new ImmutableBytesWritable();
	}
	

	public HBaseKey(ImmutableBytesWritable writable) {
		this.writable = writable;
	}
	
	
	public ImmutableBytesWritable getWritable() {
		return writable;
	}

	public void setWritable(ImmutableBytesWritable writable) {
		this.writable = writable;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void write(DataOutputView out) throws IOException {
		this.writable.write(out);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.writable.readFields(in);
	}

	@Override
	public int hashCode() {
		return this.writable.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj.getClass() == HBaseKey.class) {
			return this.writable.equals(((HBaseKey) obj).writable);
		} else {
			return false;
		}
	}
	
	@Override
	public int compareTo(HBaseKey other) {
		return this.writable.compareTo(other.writable);
	}
}
