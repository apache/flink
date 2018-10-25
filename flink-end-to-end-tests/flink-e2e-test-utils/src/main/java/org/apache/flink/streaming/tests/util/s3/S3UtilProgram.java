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

import org.apache.flink.api.java.utils.ParameterTool;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.KeyFilter;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * S3 utilities.
 *
 * <p>Usage: java -jar S3UtilProgram.jar args.
 *
 * <p>Note: {@code S3UtilProgram.Action.lineNumber*} actions are applicable only
 * to valid non-compressed CSV comma separated files.
 *
 * <p>Program parameters:
 * <ul>
 *     <li>action (string, required): Action to perform, see {@link S3UtilProgram.Action}.</li>
 *     <li>bucket (string, required): Bucket where s3 objects reside.</li>
 *     <li>s3file (string, required for single object actions): s3 object key.</li>
 *     <li>s3prefix (string, required for actions over objects grouped by key prefix): s3 key prefix.</li>
 *     <li>s3filePrefix (string, optional for downloadByPrefix): s3 file name prefix w/o directory to filter files by name.</li>
 *     <li>localFile (string, required for single file actions): local file path.</li>
 *     <li>localFolder (string, required for actions over folders): local folder path.</li>
 *     <li>parallelism (int, default 10): parallelism for parallelizable actions (e.g. {@code lineNumberByPrefix}).</li>
 * </ul>
 */
class S3UtilProgram {
	enum Action {
		listByPrefix,
		downloadFile,
		downloadByPrefix,
		deleteFile,
		deleteByPrefix,
		lineNumberFile,
		lineNumberByPrefix
	}

	private static final Map<Action, Consumer<ParameterTool>> handlers;
	static {
		Map<Action, Consumer<ParameterTool>> handlersMutable = new HashMap<>();
		handlersMutable.put(Action.listByPrefix, S3UtilProgram::listByPrefix);
		handlersMutable.put(Action.downloadFile, S3UtilProgram::downloadFile);
		handlersMutable.put(Action.downloadByPrefix, S3UtilProgram::downloadByPrefix);
		handlersMutable.put(Action.deleteFile, S3UtilProgram::deleteFile);
		handlersMutable.put(Action.deleteByPrefix, S3UtilProgram::deleteByPrefix);
		handlersMutable.put(Action.lineNumberFile, S3UtilProgram::lineNumberFile);
		handlersMutable.put(Action.lineNumberByPrefix, S3UtilProgram::lineNumberByPrefix);
		handlers = Collections.unmodifiableMap(handlersMutable);
	}

	private static final String countQuery = "select count(*) from s3object";

	public static void main(String[] args) {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final Action action = Action.valueOf(params.getRequired("action"));
		handlers.get(action).accept(params);
	}

	private static void listByPrefix(ParameterTool params) {
		final String bucket = params.getRequired("bucket");
		final String s3prefix = params.getRequired("s3prefix");
		listByPrefix(bucket, s3prefix).forEach(System.out::println);
	}

	private static List<String> listByPrefix(final String bucket, final String s3prefix) {
		return AmazonS3ClientBuilder.defaultClient().listObjects(bucket, s3prefix).getObjectSummaries()
			.stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
	}

	private static void downloadFile(ParameterTool params) {
		final String bucket = params.getRequired("bucket");
		final String s3file = params.getRequired("s3file");
		final String localFile = params.getRequired("localFile");
		TransferManager tx = TransferManagerBuilder.defaultTransferManager();
		try {
			tx.download(bucket, s3file, new File(localFile)).waitForCompletion();
		} catch (InterruptedException e) {
			System.out.println("Transfer interrupted");
		} finally {
			tx.shutdownNow();
		}
	}

	private static void downloadByPrefix(ParameterTool params) {
		final String bucket = params.getRequired("bucket");
		final String s3prefix = params.getRequired("s3prefix");
		final String localFolder = params.getRequired("localFolder");
		final String s3filePrefix = params.get("s3filePrefix", "");
		TransferManager tx = TransferManagerBuilder.defaultTransferManager();
		try {
			tx.downloadDirectory(
				bucket, s3prefix, new File(localFolder),
				getDownloadByPrefixKeyFilter(s3filePrefix)).waitForCompletion();
		} catch (InterruptedException e) {
			System.out.println("Transfer interrupted");
		} finally {
			tx.shutdownNow();
		}
	}

	private static KeyFilter getDownloadByPrefixKeyFilter(String s3filePrefix) {
		if (s3filePrefix.isEmpty()) {
			return KeyFilter.INCLUDE_ALL;
		} else {
			return objectSummary -> {
				String[] parts = objectSummary.getKey().split("/");
				String fileName = parts[parts.length - 1];
				return fileName.startsWith(s3filePrefix);
			};
		}
	}

	private static void deleteFile(ParameterTool params) {
		final String bucket = params.getRequired("bucket");
		final String s3file = params.getRequired("s3file");
		AmazonS3ClientBuilder.defaultClient().deleteObject(bucket, s3file);
	}

	private static void deleteByPrefix(ParameterTool params) {
		final String bucket = params.getRequired("bucket");
		final String s3prefix = params.getRequired("s3prefix");
		String[] keys = listByPrefix(bucket, s3prefix).toArray(new String[] {});
		if (keys.length > 0) {
			DeleteObjectsRequest request = new DeleteObjectsRequest(bucket).withKeys(keys);
			AmazonS3ClientBuilder.defaultClient().deleteObjects(request);
		}
	}

	private static void lineNumberFile(ParameterTool params) {
		final String bucket = params.getRequired("bucket");
		final String s3file = params.getRequired("s3file");
		AmazonS3 s3client = AmazonS3ClientBuilder.defaultClient();
		System.out.print(S3QueryUtil.queryFile(s3client, bucket, s3file, countQuery));
		s3client.shutdown();
	}

	private static void lineNumberByPrefix(ParameterTool params) {
		final String bucket = params.getRequired("bucket");
		final String s3prefix = params.getRequired("s3prefix");
		int parallelism = params.getInt("parallelism", 10);
		List<String> files = listByPrefix(bucket, s3prefix);
		ExecutorService executor = Executors.newFixedThreadPool(parallelism);
		List<CompletableFuture<Integer>> results = new ArrayList<>();
		AmazonS3 s3client = AmazonS3ClientBuilder.defaultClient();
		files.forEach(file -> {
			CompletableFuture<Integer> result = new CompletableFuture<>();
			executor.execute(() ->
				result.complete(Integer.parseInt(S3QueryUtil.queryFile(s3client, bucket, file, countQuery))));
			results.add(result);
		});
		int count = 0;
		for (CompletableFuture<Integer> result : results) {
			try {
				count += result.get();
			} catch (Throwable e) {
				System.out.println("Failed count lines");
				e.printStackTrace();
			}
		}
		executor.shutdownNow();
		s3client.shutdown();
		System.out.print(count);
	}
}
