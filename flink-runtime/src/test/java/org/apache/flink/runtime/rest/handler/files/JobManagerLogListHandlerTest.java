package org.apache.flink.runtime.rest.handler.files;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobManagerLogListHeaders;
import org.apache.flink.runtime.rest.messages.LogListInfo;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link JobManagerLogListHandler}.
 */
public class JobManagerLogListHandlerTest extends TestLogger {

	@Test
	public void handleRequest() throws IOException, RestHandlerException, HandlerRequestException, ExecutionException, InterruptedException {

		File logDir = createLogFiles();
		JobManagerLogListHandler jobManagerLogListHandler = new JobManagerLogListHandler(
			() -> new CompletableFuture<>(),
			RpcUtils.INF_TIMEOUT,
			Collections.EMPTY_MAP,
			JobManagerLogListHeaders.getInstance(),
			logDir);
		HandlerRequest handlerRequest = new HandlerRequest(
			EmptyRequestBody.getInstance(),
			JobManagerLogListHeaders.getInstance().getUnresolvedMessageParameters(),
			Collections.emptyMap(),
			Collections.emptyMap());
		CompletableFuture<LogListInfo> logListInfoCompletableFuture = jobManagerLogListHandler.handleRequest(handlerRequest, null);
		assertTrue(logListInfoCompletableFuture.get().equals(new LogListInfo(logDir.list())));
	}

	private File createLogFiles() throws IOException {
		File logDir = File.createTempFile("TestBaseUtils-logdir", null);
		assertTrue("Unable to delete temp file", logDir.delete());
		assertTrue("Unable to create temp directory", logDir.mkdir());
		String fileContent = "this is a test";
		File logFile1 = new File(logDir, "jobmanager.log");
		try (FileOutputStream fileOutputStream = new FileOutputStream(logFile1)) {
			fileOutputStream.write(fileContent.getBytes());
		}
		File logFile2 = new File(logDir, "jobmanager_2019-01-18.log");
		try (FileOutputStream fileOutputStream = new FileOutputStream(logFile2)) {
			fileOutputStream.write(fileContent.getBytes());
		}
		File logFile3 = new File(logDir, "jobmanager_2019-01-19.log");
		try (FileOutputStream fileOutputStream = new FileOutputStream(logFile3)) {
			fileOutputStream.write(fileContent.getBytes());
		}
		return logDir;
	}
}
