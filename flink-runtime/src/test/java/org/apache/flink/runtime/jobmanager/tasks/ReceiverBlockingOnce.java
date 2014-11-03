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

package org.apache.flink.runtime.jobmanager.tasks;

import org.apache.flink.runtime.io.network.api.RecordReader;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.types.IntegerRecord;

public final class ReceiverBlockingOnce extends AbstractInvokable {
	
	private static boolean shouldBlock = true;

	private RecordReader<IntegerRecord> reader;
	
	@Override
	public void registerInputOutput() {
		reader = new RecordReader<IntegerRecord>(this, IntegerRecord.class);
	}

	@Override
	public void invoke() throws Exception {
		if (shouldBlock) {
			
			Object o = new Object();
			synchronized (o) {
				o.wait();
			}
		}
		
		while (reader.next() != null);
	}
	
	public static void setShouldNotBlock() {
		shouldBlock = false;
	}
}
