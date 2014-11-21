/**
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

package org.apache.flink.mesos;

import com.google.protobuf.ByteString;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * This is a class responsible for the mesos configuration. It adds two methods to
 * convert the configuration to a ByteString and to rebuild it from a ProtoBuf object.
 * This is necessary because data is send from the ExecutorInfo to the Executors only via
 * ProtoBuf.
 */
public class MesosConfiguration  extends Configuration {

	private final Logger LOG = LoggerFactory.getLogger(MesosConfiguration.class);

	/**
	 * Output this config as ProtoBuf ByteString
	 * @return ByteString representation of this config
	 */
	public ByteString toByteString() {
		HashMap<String, Object> data = getConfigData();
		FlinkProtos.Configuration.Builder builder = FlinkProtos.Configuration.newBuilder();
		for (HashMap.Entry<String, Object> entry: data.entrySet()) {
			builder.addValues(FlinkProtos.Configuration.Triplet.newBuilder().setKey(entry.getKey()).setValue(entry.getValue().toString()).setType(entry.getValue().getClass().getName()));
		}
		return builder.build().toByteString();
	}

	/**
	 * Fill this configuration with data from a ProtoBuf object
	 * @param config ProtoBuf config object
	 */
	public void fromProtos(FlinkProtos.Configuration config) {
		if (config == null) {
			return;
		}
		if (config.getValuesList() == null) {
			return;
		}
		for (FlinkProtos.Configuration.Triplet triplet: config.getValuesList()) {

			String type = triplet.getType();

			if (type == null) {
				continue;
			}
			/*
			Currently only the types String, Double, Integer and Boolean are supported. This covers most
			of the configuration data.
			 */
			if (type.equals("java.lang.String")) {
				this.setString(triplet.getKey(), triplet.getValue());
			} else if (type.equals("java.lang.Double")) {
				this.setDouble(triplet.getKey(), Double.valueOf(triplet.getValue()));
			} else if (type.equals("java.lang.Integer")) {
				this.setInteger(triplet.getKey(), Integer.valueOf(triplet.getValue()));
			} else if (type.equals("java.lang.Boolean")) {
				this.setBoolean(triplet.getKey(), Boolean.valueOf(triplet.getValue()));
			}
		}

	}
}
