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

package org.apache.flink.streaming.connectors.wikiedits;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * This class is a SourceFunction that reads {@link WikipediaEditEvent} instances from the IRC channel
 * <code>#en.wikipedia</code>.
 */
public class WikipediaEditsSource extends RichSourceFunction<WikipediaEditEvent> {

	/** Hostname of the server to connect to. */
	public static final String DEFAULT_HOST = "irc.wikimedia.org";

	/** Port of the server to connect to. */
	public static final int DEFAULT_PORT = 6667;

	/** IRC channel to join. */
	public static final String DEFAULT_CHANNEL = "#en.wikipedia";

	private final String host;
	private final int port;
	private final String channel;

	private volatile boolean isRunning = true;

	/**
	 * Creates a source reading {@link WikipediaEditEvent} instances from the
	 * IRC channel <code>#en.wikipedia</code>.
	 *
	 * <p>This creates a separate Thread for the IRC connection.
	 */
	public WikipediaEditsSource() {
		this(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_CHANNEL);
	}

	/**
	 * Creates a source reading {@link WikipediaEditEvent} instances from the
	 * specified IRC channel.
	 *
	 * <p>In most cases, you want to use the default {@link #WikipediaEditsSource}
	 * constructor. This constructor is meant to be used only if there is a
	 * problem with the default constructor.
	 *
	 * @param host    The IRC server to connect to.
	 * @param port    The port of the IRC server to connect to.
	 * @param channel The channel to join. Messages not matching the expected
	 *                format will be ignored.
	 */
	public WikipediaEditsSource(String host, int port, String channel) {
		this.host = host;
		this.port = port;
		this.channel = Objects.requireNonNull(channel);
	}

	@Override
	public void run(SourceContext<WikipediaEditEvent> ctx) throws Exception {
		try (WikipediaEditEventIrcStream ircStream = new WikipediaEditEventIrcStream(host, port)) {
			// Open connection and join channel
			ircStream.connect();
			ircStream.join(channel);

			try {
				while (isRunning) {
					// Query for the next edit event
					WikipediaEditEvent edit = ircStream.getEdits().poll(100, TimeUnit.MILLISECONDS);

					if (edit != null) {
						ctx.collect(edit);
					}
				}
			} finally {
				ircStream.leave(channel);
			}
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}
}
