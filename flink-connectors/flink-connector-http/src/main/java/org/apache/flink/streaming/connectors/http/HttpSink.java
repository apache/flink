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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.URI;


/**
 * Http sink that initiates an HTTP POST to the configured HTTP server
 * for each element
 *
 * <p>The sink uses {@link org.apache.http.client.HttpClient} internally to perform HTTP requests.
 * The sink will fail if it can't connect to the URI specified in the constructor.
 *
 *<p>The constructor receives an HttpBuilder and an ResponseHandlerBuilder so that Http client can be fully configured</p>
 *
 * @param <T> Type of the elements handled by this sink
 */
public class HttpSink<T> extends RichSinkFunction<T> {

	private static final Logger LOG = LoggerFactory.getLogger(HttpSink.class);

	private transient CloseableHttpClient httpClient;
	private transient ResponseHandler responseHandler;

	private final HttpBuilder httpBuilder;
	private ResponseHandlerBuilder responseHandlerBuilder;

	private URI destURI;
	private SerializationSchema<T> serializationSchema;

	/**
	 * Creates a new {@code HttpSink} that connects to a URL using an http client.
	 *
	 * @param destURI HTTP URI to which client will send data
	 * @param serializationSchema Serialiation schema that will be used to serialize values
	 */
	public HttpSink(URI destURI, SerializationSchema<T> serializationSchema) {
		this(destURI,
			serializationSchema,
			new DefaultHttpBuilder(),
			new DefaultResponseHandlerBuilder());
	}

	/**
	 * Creates a new {@code HttpSink} that connects to a URL using an http client.
	 *
	 * @param destURI HTTP URI to which client will send data
	 * @param serializationSchema Serialization schema that will be used to serialize values
	 * @param httpBuilder Builder used to customize the HttpClient
	 * @param responseHandlerBuilder Builder used to customize HttpClient response handler
	 */
	public <V> HttpSink(URI destURI,
						SerializationSchema<T> serializationSchema,
						HttpBuilder httpBuilder,
						ResponseHandlerBuilder<V> responseHandlerBuilder) {
		this.destURI = destURI;
		this.serializationSchema = serializationSchema;
		ClosureCleaner.ensureSerializable(serializationSchema);

		this.httpBuilder = httpBuilder;
		ClosureCleaner.clean(httpBuilder, true);

		this.responseHandlerBuilder = responseHandlerBuilder;
		ClosureCleaner.clean(responseHandlerBuilder, true);
	}

	@Override
	public void open(Configuration parameters) {
		httpClient = httpBuilder.buildClient();
		responseHandler = responseHandlerBuilder.buildHandler();
	}

	@Override
	public void close() throws Exception {
		if (httpClient != null) {
			httpClient.close();
		}
	}

	@Override
	public void invoke(T value, Context context) throws Exception {
		byte[] serializedValue = serializationSchema.serialize(value);
		HttpPost httpPost = new HttpPost(destURI);
		ByteArrayEntity httpEntity = new ByteArrayEntity(serializedValue);

		httpPost.setEntity(httpEntity);

		try {
			LOG.debug("Executing HTTP POST with data " + value);
			httpClient.execute(httpPost, responseHandler);
		}
		catch (HttpResponseException e1) {
			LOG.error("HTTP Response exception", e1);
			throw e1;
		}
		catch (ConnectException e2) {
			LOG.error("HTTP Connection exception", e2);
			throw e2;
		}
		catch (Exception e3) {
			LOG.error("HTTP generic exception", e3);
			throw e3;
		}
	}

	/**
	 * A default HttpBuilder that creates a default Http client.
	 */
	public static class DefaultHttpBuilder implements HttpBuilder {
		@Override
		public CloseableHttpClient buildClient() {
			return HttpClients.createDefault();
		}
	}

	/**
	 * A default response handler that creates a {@link BasicResponseHandler}.
	 */
	public static class DefaultResponseHandlerBuilder implements ResponseHandlerBuilder {
		@Override
		public ResponseHandler buildHandler() {
			return new BasicResponseHandler();
		}
	}

}
