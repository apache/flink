/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.operator;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.Comparator;

public class StreamRecordComparator<IN> implements Comparator<StreamRecord<IN>>, Serializable {
	private static final long serialVersionUID = 1581054988433915305L;

	@Override
	public int compare(StreamRecord<IN> o1, StreamRecord<IN> o2) {
		if (o1.getTimestamp() < o2.getTimestamp()) {
			return -1;
		} else if (o1.getTimestamp() > o2.getTimestamp()) {
			return 1;
		} else {
			return 0;
		}
	}
}
