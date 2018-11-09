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

package org.apache.flink.streaming.tests.util.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CSVOutput;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.SelectObjectContentEvent;
import com.amazonaws.services.s3.model.SelectObjectContentEventStream;
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.amazonaws.util.IOUtils.copy;

class S3QueryUtil {
	/** Run SQL query over non-compressed CSV file saved in s3 object. */
	static String queryFile(
			AmazonS3 s3client, String bucket, String s3file, @SuppressWarnings("SameParameterValue") String query) {
		SelectObjectContentRequest request = generateBaseCSVRequest(bucket, s3file, query);
		final AtomicBoolean isResultComplete = new AtomicBoolean(false);
		String res;
		try (SelectObjectContentResult result = s3client.selectObjectContent(request);
			SelectObjectContentEventStream payload = result.getPayload();
			ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			InputStream resultInputStream = payload.getRecordsInputStream(
				new SelectObjectContentEventVisitor() {
					@Override
					public void visit(SelectObjectContentEvent.EndEvent event) {
						isResultComplete.set(true);
					}
				}
			);
			copy(resultInputStream, out);
			res = out.toString().trim();
		} catch (Throwable e) {
			System.out.println("SQL query failure");
			throw new RuntimeException("SQL query failure", e);
		}
		/*
		 * The End Event indicates all matching records have been transmitted.
		 * If the End Event is not received, the results may be incomplete.
		 */
		if (!isResultComplete.get()) {
			throw new RuntimeException("S3 Select request was incomplete as End Event was not received.");
		}
		return res;
	}

	private static SelectObjectContentRequest generateBaseCSVRequest(String bucket, String key, String query) {
		SelectObjectContentRequest request = new SelectObjectContentRequest();
		request.setBucketName(bucket);
		request.setKey(key);
		request.setExpression(query);
		request.setExpressionType(ExpressionType.SQL);

		InputSerialization inputSerialization = new InputSerialization();
		inputSerialization.setCsv(new CSVInput());
		inputSerialization.setCompressionType(CompressionType.NONE);
		request.setInputSerialization(inputSerialization);

		OutputSerialization outputSerialization = new OutputSerialization();
		outputSerialization.setCsv(new CSVOutput());
		request.setOutputSerialization(outputSerialization);

		return request;
	}
}
