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

package org.apache.flink.tests.util.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import org.junit.Rule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link MinioS3Resource}.
 */
public class MinioS3ResourceTest {
	@Rule
	public S3Resource minioServer = new MinioS3Resource();

	@Test
	public void testWriteReadDelete() {
		final AmazonS3 client = minioServer.getClient();

		client.putObject(minioServer.getTestBucket(), "testFile", "testContent");
		assertThat(client.getObjectAsString(minioServer.getTestBucket(), "testFile")).isEqualTo("testContent");

		client.deleteObject(minioServer.getTestBucket(), "testFile");
		assertThatThrownBy(() -> client.getObject(minioServer.getTestBucket(), "testFile"))
			.isInstanceOf(AmazonServiceException.class);
	}
}
