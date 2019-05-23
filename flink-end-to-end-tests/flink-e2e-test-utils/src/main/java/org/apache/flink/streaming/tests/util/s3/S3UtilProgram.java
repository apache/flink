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
import java.util.function.Predicate;
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
 *     <li>s3filePrefix (string, optional for downloadByFullPathAndFileNamePrefix or numberOfLinesInFilesWithFullAndNamePrefix):
 *     s3 file name prefix w/o directory to filter files by name.</li>
 *     <li>localFile (string, required for single file actions): local file path.</li>
 *     <li>localFolder (string, required for actions over folders): local folder path.</li>
 *     <li>parallelism (int, default 10): parallelism for parallelizable actions
 *     (e.g. {@code numberOfLinesInFilesWithFullAndNamePrefix}).</li>
 * </ul>
 */
class S3UtilProgram {
	enum Action {
		listByFullPathPrefix,
		downloadFile,
		downloadByFullPathAndFileNamePrefix,
		deleteFile,
		deleteByFullPathPrefix,
		numberOfLinesInFile,
		numberOfLinesInFilesWithFullAndNamePrefix
	}

	private static final Map<Action, Consumer<ParameterTool>> handlers;
	static {
		Map<Action, Consumer<ParameterTool>> handlersMutable = new HashMap<>();
		handlersMutable.put(Action.listByFullPathPrefix, S3UtilProgram::listByFullPathPrefix);
		handlersMutable.put(Action.downloadFile, S3UtilProgram::downloadFile);
		handlersMutable.put(Action.downloadByFullPathAndFileNamePrefix,
			S3UtilProgram::downloadByFullPathAndFileNamePrefix);
		handlersMutable.put(Action.deleteFile, S3UtilProgram::deleteFile);
		handlersMutable.put(Action.deleteByFullPathPrefix, S3UtilProgram::deleteByFullPathPrefix);
		handlersMutable.put(Action.numberOfLinesInFile, S3UtilProgram::numberOfLinesInFile);
		handlersMutable.put(Action.numberOfLinesInFilesWithFullAndNamePrefix,
			S3UtilProgram::numberOfLinesInFilesWithFullAndNamePrefix);
		handlers = Collections.unmodifiableMap(handlersMutable);
	}

	private static final String countQuery = "select count(*) from s3object";

	public static void main(String[] args) {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final Action action = Action.valueOf(params.getRequired("action"));
		handlers.get(action).accept(params);
	}

	private static void listByFullPathPrefix(ParameterTool params) {
		final String bucket = params.getRequired("bucket");
		final String s3prefix = params.getRequired("s3prefix");
		listByFullPathPrefix(bucket, s3prefix).forEach(System.out::println);
	}

	private static List<String> listByFullPathPrefix(final String bucket, final String s3prefix) {
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

	private static void downloadByFullPathAndFileNamePrefix(ParameterTool params) {
		final String bucket = params.getRequired("bucket");
		final String s3prefix = params.getRequired("s3prefix");
		final String localFolder = params.getRequired("localFolder");
		final String s3filePrefix = params.get("s3filePrefix", "");
		TransferManager tx = TransferManagerBuilder.defaultTransferManager();
		Predicate<String> keyPredicate = getKeyFilterByFileNamePrefix(s3filePrefix);
		KeyFilter keyFilter = s3filePrefix.isEmpty() ? KeyFilter.INCLUDE_ALL :
			objectSummary -> keyPredicate.test(objectSummary.getKey());
		try {
			tx.downloadDirectory(bucket, s3prefix, new File(localFolder), keyFilter).waitForCompletion();
		} catch (InterruptedException e) {
			System.out.println("Transfer interrupted");
		} finally {
			tx.shutdownNow();
		}
	}

	private static Predicate<String> getKeyFilterByFileNamePrefix(String s3filePrefix) {
		if (s3filePrefix.isEmpty()) {
			return key -> true;
		} else {
			return key -> {
				String[] parts = key.split("/");
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

	private static void deleteByFullPathPrefix(ParameterTool params) {
		final String bucket = params.getRequired("bucket");
		final String s3prefix = params.getRequired("s3prefix");
		String[] keys = listByFullPathPrefix(bucket, s3prefix).toArray(new String[] {});
		if (keys.length > 0) {
			DeleteObjectsRequest request = new DeleteObjectsRequest(bucket).withKeys(keys);
			AmazonS3ClientBuilder.defaultClient().deleteObjects(request);
		}
	}

	private static void numberOfLinesInFile(ParameterTool params) {
		final String bucket = params.getRequired("bucket");
		final String s3file = params.getRequired("s3file");
		AmazonS3 s3client = AmazonS3ClientBuilder.defaultClient();
		System.out.print(S3QueryUtil.queryFile(s3client, bucket, s3file, countQuery));
		s3client.shutdown();
	}

	private static void numberOfLinesInFilesWithFullAndNamePrefix(ParameterTool params) {
		final String bucket = params.getRequired("bucket");
		final String s3prefix = params.getRequired("s3prefix");
		final String s3filePrefix = params.get("s3filePrefix", "");
		int parallelism = params.getInt("parallelism", 10);

		List<String> files = listByFullPathPrefix(bucket, s3prefix);

		ExecutorService executor = Executors.newFixedThreadPool(parallelism);
		AmazonS3 s3client = AmazonS3ClientBuilder.defaultClient();
		List<CompletableFuture<Integer>> requests =
			submitLineCountingRequestsForFilesAsync(executor, s3client, bucket, files, s3filePrefix);
		int count = waitAndComputeTotalLineCountResult(requests);

		executor.shutdownNow();
		s3client.shutdown();
		System.out.print(count);
	}

	private static List<CompletableFuture<Integer>> submitLineCountingRequestsForFilesAsync(
			ExecutorService executor, AmazonS3 s3client, String bucket, List<String> files, String s3filePrefix) {
		List<CompletableFuture<Integer>> requests = new ArrayList<>();
		Predicate<String> keyPredicate = getKeyFilterByFileNamePrefix(s3filePrefix);
		files.forEach(file -> {
			if (keyPredicate.test(file)) {
				CompletableFuture<Integer> result = new CompletableFuture<>();
				executor.execute(() ->
					result.complete(Integer.parseInt(S3QueryUtil.queryFile(s3client, bucket, file, countQuery))));
				requests.add(result);
			}
		});
		return requests;
	}

	private static int waitAndComputeTotalLineCountResult(List<CompletableFuture<Integer>> requests) {
		int count = 0;
		for (CompletableFuture<Integer> result : requests) {
			try {
				count += result.get();
			} catch (Throwable e) {
				System.out.println("Failed count lines");
				e.printStackTrace();
			}
		}
		return count;
	}
}
