/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.common.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import eu.stratosphere.pact.common.type.Key;

/**
 * Simple wrapper to encapsulate an HBase h{@link ImmutableBytesWritable} as a Key
 */
public class HBaseKey implements Key {

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
	public void write(DataOutput out) throws IOException {
		this.writable.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
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
	public int compareTo(Key other) {
		if (other.getClass() == HBaseKey.class) {
			return this.writable.compareTo(((HBaseKey) other).writable);
		} else {
			throw new IllegalArgumentException("Compare between HBase key and " + 
					other.getClass().getName() + " is not defined.");
		}
	}
}
