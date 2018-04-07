/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.http;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.MethodNotSupportedException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.HttpConnectionFactory;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link HttpSink}.
 */
public class HttpSinkTest {

	private static final int HTTP_PORT = 30445;
	private static HttpServer httpServer;

	private static CollectingHttpRequestHandler<String> requestHandler =
		new CollectingHttpRequestHandler<>(new SimpleStringSchema());

	public static HttpServer createHttpServer(int listenPort, HttpRequestHandler requestHandler)
		throws IOException {

		SocketConfig socketConfig = SocketConfig.custom()
			.setSoTimeout(15)
			.setTcpNoDelay(true)
			.setSoReuseAddress(true)
			.build();

		final HttpServer server = ServerBootstrap.bootstrap()
			.setListenerPort(listenPort)
			.setLocalAddress(InetAddress.getLoopbackAddress())
			//.setHttpProcessor(httpProcessor)
			.setSocketConfig(socketConfig)
			.registerHandler("*", requestHandler)
			//.setResponseFactory(new DefaultHttpResponseFactory())
			.create();

		server.start();

		return server;
	}

	@BeforeClass
	public static void before() throws Exception {
		httpServer = createHttpServer(HTTP_PORT, requestHandler);
	}

	@AfterClass
	public static void after() {
		httpServer.shutdown(10, TimeUnit.MILLISECONDS);
	}

	@Test
	public void testSimpleHttpSinkBasicCtor() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();

		Set<String> stringData = getStringData();
		DataStream<String> input = env.fromCollection(stringData);

		input.addSink(
			new HttpSink<>(new URI("http://localhost:" + HTTP_PORT),
				new SimpleStringSchema()
			))
			.name("SimpleHTTP")
			.setParallelism(1);

		env.execute("[HttpSink] testHttpSinkBasic");

		assertEquals(stringData, requestHandler.getCollectedData());
	}

	@Test
	public void testSimpleHttpSinkExtendedCtor() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();

		Set<String> stringData = getStringData();
		DataStream<String> input = env.fromCollection(stringData);

		executeHttpSinkJob(env, input, HTTP_PORT);

		assertEquals(stringData, requestHandler.getCollectedData());
	}

	@Test(expected = JobExecutionException.class)
	public void testSimpleHttpSinkExceptionOnConnectionError() throws Exception {
		final int notListenPort = 44004;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();

		Set<String> stringData = getStringData();
		DataStream<String> input = env.fromCollection(stringData);

		try {
			executeHttpSinkJob(env, input, notListenPort);
		} catch (Exception e)  {
			assertTrue(e.getCause() instanceof ConnectException);
			throw e;
		}
	}

	// ---- Utils

	private void executeHttpSinkJob(StreamExecutionEnvironment env, DataStream<String> input, int listenPort)
		throws Exception {
		input.addSink(
			new HttpSink<>(new URI("http://localhost:" + listenPort),
				new SimpleStringSchema(),
				new MyHttpBuilder(),
				new MyResponseHandlerBuilder()
			))
			.name("SimpleHTTP")
			.setParallelism(1);

		env.execute("[HttpSink] testHttpSinkBasic");
	}

	private Set<String> getStringData() {
		Set<String> dataSet = new HashSet<>();
		for (int i = 0; i < 10; i++) {
			dataSet.add("Testing entry " + i);
		}

		return dataSet;
	}

	private DataStream<String> getStringDataStream(StreamExecutionEnvironment env) {

		Set<String> stringData = getStringData();
		return env.fromCollection(stringData);
	}

	private static class MyHttpBuilder implements HttpBuilder {
		private static final long serialVersionUID = 4622936756L;

		@Override
		public CloseableHttpClient buildClient() {
			HttpClientBuilder client = HttpClients.custom();
			// Create a connection manager with custom configuration.
			HttpConnectionFactory<HttpRoute, ManagedHttpClientConnection> httpConnectionFactory =
				new ManagedHttpClientConnectionFactory();

			PoolingHttpClientConnectionManager connManager =
				new PoolingHttpClientConnectionManager(httpConnectionFactory);

			return client
				.setConnectionManager(connManager)
				.build();

		}
	}

	private static class MyResponseHandlerBuilder implements ResponseHandlerBuilder<String> {
		@Override
		public ResponseHandler<String> buildHandler() {
			return new ResponseHandler<String>() {
				@Override
				public String handleResponse(
					final HttpResponse response) throws IOException {
					int status = response.getStatusLine().getStatusCode();
					if (status >= 200 && status < 300) {
						HttpEntity entity = response.getEntity();
						return entity != null ? EntityUtils.toString(entity) : null;
					} else {
						throw new ClientProtocolException("Unexpected response status: " + status);
					}
				}
			};
		}
	}

	private static class CollectingHttpRequestHandler<T> implements HttpRequestHandler {
		private static final Logger LOG = LoggerFactory.getLogger(CollectingHttpRequestHandler.class);

		private Set<T> collectedData = new HashSet<>();
		private DeserializationSchema<T> deserializationSchema;

		public CollectingHttpRequestHandler(DeserializationSchema<T> deserializationSchema) {
			this.deserializationSchema = deserializationSchema;
		}

		@Override
		public void handle(HttpRequest request, HttpResponse response, HttpContext context)
			throws HttpException, IOException {

			String method = request.getRequestLine().getMethod().toUpperCase(Locale.ROOT);
			if (!method.equals("POST")) {
				throw new MethodNotSupportedException(method + " method not supported");
			}

			if (request instanceof HttpEntityEnclosingRequest) {
				HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
				byte[] entityContent = EntityUtils.toByteArray(entity);

				T data = deserializationSchema.deserialize(entityContent);

				LOG.debug("Incoming entity content (bytes): " + entityContent.length);
				LOG.debug(data.toString());

				collectedData.add(data);
			}
		}

		public Set<T> getCollectedData() {
			return Collections.unmodifiableSet(collectedData);
		}
	}
}
