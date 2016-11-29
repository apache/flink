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
package org.apache.flink.runtime.executiongraph;

import org.apache.flink.metrics.Meter;

import java.io.Serializable;

/**
 * An instance of this class represents a snapshot of the io-related metrics of a single task.
 */
public class IOMetrics implements Serializable {
	private static final long serialVersionUID = -7208093607556457183L;
	private final long numRecordsIn;
	private final long numRecordsOut;

	private final double numRecordsInPerSecond;
	private final double numRecordsOutPerSecond;

	private final long numBytesInLocal;
	private final long numBytesInRemote;
	private final long numBytesOut;

	private final double numBytesInLocalPerSecond;
	private final double numBytesInRemotePerSecond;
	private final double numBytesOutPerSecond;

	public IOMetrics(Meter recordsIn, Meter recordsOut, Meter bytesLocalIn, Meter bytesRemoteIn, Meter bytesOut) {
		this.numRecordsIn = recordsIn.getCount();
		this.numRecordsInPerSecond = recordsIn.getRate();
		this.numRecordsOut = recordsOut.getCount();
		this.numRecordsOutPerSecond = recordsOut.getRate();
		this.numBytesInLocal = bytesLocalIn.getCount();
		this.numBytesInLocalPerSecond = bytesLocalIn.getRate();
		this.numBytesInRemote = bytesRemoteIn.getCount();
		this.numBytesInRemotePerSecond = bytesRemoteIn.getRate();
		this.numBytesOut = bytesOut.getCount();
		this.numBytesOutPerSecond = bytesOut.getRate();
	}

	public long getNumRecordsIn() {
		return numRecordsIn;
	}

	public long getNumRecordsOut() {
		return numRecordsOut;
	}

	public long getNumBytesInLocal() {
		return numBytesInLocal;
	}

	public long getNumBytesInRemote() {
		return numBytesInRemote;
	}

	public long getNumBytesInTotal() {
		return numBytesInLocal + numBytesInRemote;
	}

	public long getNumBytesOut() {
		return numBytesOut;
	}

	public double getNumRecordsInPerSecond() {
		return numRecordsInPerSecond;
	}

	public double getNumRecordsOutPerSecond() {
		return numRecordsOutPerSecond;
	}

	public double getNumBytesInLocalPerSecond() {
		return numBytesInLocalPerSecond;
	}

	public double getNumBytesInRemotePerSecond() {
		return numBytesInRemotePerSecond;
	}

	public double getNumBytesOutPerSecond() {
		return numBytesOutPerSecond;
	}
}
