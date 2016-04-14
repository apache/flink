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

package org.apache.flink.api.java.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.io.InputSplit;

/**
 * A range input split provides information about a particular range of keys, each with a min and a max value. 
 */
@Public
public class RangeInputSplit implements InputSplit{

	private static final long serialVersionUID = 4310893817447171721L;
	
	private long min;
	private long max;
	
	/** The number of the split. */
	private final int splitNumber;
	
	public RangeInputSplit(int splitNumber, long min, long max){
		this.min = min;
		this.max = max;
		this.splitNumber = splitNumber;
	}

	@Override
	public int getSplitNumber() {
		return splitNumber;
	}

	public long getMin() {
		return min;
	}

	public long getMax() {
		return max;
	}


}
