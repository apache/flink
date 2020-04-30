/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Abstract class for formatting the output of the writeAsText and writeAsCsv
 * functions.
 *
 * @param <IN>
 *            Input tuple type
 *
 * @deprecated Please use the {@link org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink StreamingFileSink}
 * for writing to files from a streaming program.
 */
@PublicEvolving
@Deprecated
public abstract class WriteFormat<IN> implements Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * Writes the contents of tupleList to the file specified by path.
	 *
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param tupleList
	 *            is the list of tuples to be written
	 */
	protected abstract void write(String path, ArrayList<IN> tupleList);

}
