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


package org.apache.flink.addons.hbase;

import java.util.Random;

import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.java.record.operators.GenericDataSink;

/**
 * A sink for writing to HBase
 */
public class HBaseDataSink extends GenericDataSink {
	
	private static final int IDENTIFYIER_LEN = 16;
	
	public HBaseDataSink(GenericTableOutputFormat f, Operator input, String name) {
		super(f, input, name);
		
		// generate a random unique identifier string
		final Random rnd = new Random();
		final StringBuilder bld = new StringBuilder();
		for (int i = 0; i < IDENTIFYIER_LEN; i++) {
			bld.append((char) (rnd.nextInt(26) + 'a'));
		}
		
		setParameter(GenericTableOutputFormat.JT_ID_KEY, bld.toString());
		setParameter(GenericTableOutputFormat.JOB_ID_KEY, rnd.nextInt());
	}
}
