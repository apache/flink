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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Wrapper of hadoop Configuration to make it serializable.
 */
public class SerializableConfiguration implements Serializable {

	private static final long serialVersionUID = 1L;

	private transient Configuration configuration;

	public SerializableConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		configuration.write(out);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		if (configuration == null) {
			configuration = new Configuration();
		}

		configuration.readFields(in);
	}
}
