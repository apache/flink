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

package org.apache.flink.runtime.webmonitor.handlers;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.webmonitor.files.MimeTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utilities to extract a redirect address.
 *
 * <p>This is necessary at the moment, because many execution graph structures are not serializable.
 * The proper solution here is to have these serializable and transparently work with the leading
 * job manager instead of redirecting.
 */
public class HandlerRedirectUtils {

	private static final Logger LOG = LoggerFactory.getLogger(HandlerRedirectUtils.class);

	/** Pattern to extract the host from an remote Akka URL */
	private final static Pattern LeaderAddressHostPattern = Pattern.compile("^.+@(.+):([0-9]+)/user/.+$");

	public static String getRedirectAddress(
			String localJobManagerAddress,
			Tuple2<ActorGateway, Integer> leader) throws Exception {

		final String leaderAddress = leader._1().path();
		final int webMonitorPort = leader._2();

		final String jobManagerName = localJobManagerAddress.substring(localJobManagerAddress.lastIndexOf("/") + 1);

		if (!localJobManagerAddress.equals(leaderAddress) &&
			!leaderAddress.equals(JobManager.getLocalJobManagerAkkaURL(Option.apply(jobManagerName)))) {
			// We are not the leader and need to redirect
			Matcher matcher = LeaderAddressHostPattern.matcher(leaderAddress);

			if (matcher.matches()) {
				String redirectAddress = String.format("%s:%d", matcher.group(1), webMonitorPort);
				return redirectAddress;
			}
			else {
				LOG.warn("Unexpected leader address pattern {}. Cannot extract host.", leaderAddress);
			}
		}

		return null;
	}

	public static HttpResponse getRedirectResponse(String redirectAddress, String path) throws Exception {
		checkNotNull(redirectAddress, "Redirect address");
		checkNotNull(path, "Path");

		String newLocation = String.format("http://%s%s", redirectAddress, path);

		HttpResponse redirectResponse = new DefaultFullHttpResponse(
				HttpVersion.HTTP_1_1, HttpResponseStatus.TEMPORARY_REDIRECT);
		redirectResponse.headers().set(HttpHeaders.Names.LOCATION, newLocation);
		redirectResponse.headers().set(HttpHeaders.Names.CONTENT_ENCODING, "utf-8");
		redirectResponse.headers().set(HttpHeaders.Names.CONTENT_LENGTH, 0);

		return redirectResponse;
	}

	public static HttpResponse getUnavailableResponse() throws UnsupportedEncodingException {
		String result = "Service temporarily unavailable due to an ongoing leader election. Please refresh.";
		byte[] bytes = result.getBytes(Charset.forName("UTF-8"));

		HttpResponse unavailableResponse = new DefaultFullHttpResponse(
				HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE, Unpooled.wrappedBuffer(bytes));

		unavailableResponse.headers().set(HttpHeaders.Names.CONTENT_ENCODING, "utf-8");
		unavailableResponse.headers().set(HttpHeaders.Names.CONTENT_LENGTH, bytes.length);
		unavailableResponse.headers().set(HttpHeaders.Names.CONTENT_TYPE, MimeTypes.getMimeTypeForExtension("txt"));

		return unavailableResponse;
	}
}
