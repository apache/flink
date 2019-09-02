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

package streaming.connectors.filesystem;

import org.apache.flink.table.descriptors.Bucket;
import org.apache.flink.table.descriptors.BucketValidator;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorTestBase;
import org.apache.flink.table.descriptors.DescriptorValidator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class BucketTest extends DescriptorTestBase {

	@Override
	protected List<Descriptor> descriptors() {
		final Descriptor fileRawDesc =
			new Bucket().basePath("file:///tmp/flink-data/json")
				.rowFormat()
				.dateFormat("yyyy-MM-dd-HHmm");

		final Descriptor fileBultDesc =
			new Bucket().basePath("hdfs://localhost/tmp/flink-data/avro")
				.bultFormat()
				.dateFormat("yyyy-MM-dd");

		return Arrays.asList(fileRawDesc, fileBultDesc);
	}

	@Override
	protected List<Map<String, String>> properties() {
		final Map<String, String> fileRawDesc = new HashMap<>();
		fileRawDesc.put("connector.property-version", "1");
		fileRawDesc.put("connector.type", "bucket");
		fileRawDesc.put("connector.basepath", "file:///tmp/flink-data/json");
		fileRawDesc.put("connector.date.format", "yyyy-MM-dd-HHmm");
		fileRawDesc.put("connector.format.type", "row");

		final Map<String, String> fileBultDesc = new HashMap<>();
		fileBultDesc.put("connector.property-version", "1");
		fileBultDesc.put("connector.type", "bucket");
		fileBultDesc.put("connector.basepath", "hdfs://localhost/tmp/flink-data/avro");
		fileBultDesc.put("connector.date.format", "yyyy-MM-dd");
		fileBultDesc.put("connector.format.type", "bult");

		return Arrays.asList(fileRawDesc, fileBultDesc);
	}

	@Override
	protected DescriptorValidator validator() {
		return new BucketValidator();
	}
}
