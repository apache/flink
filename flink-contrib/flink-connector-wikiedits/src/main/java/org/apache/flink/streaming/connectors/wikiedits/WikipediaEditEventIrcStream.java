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

import org.schwering.irc.lib.IRCConnection;
import org.schwering.irc.lib.IRCEventListener;
import org.schwering.irc.lib.IRCModeParser;
import org.schwering.irc.lib.IRCUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

class WikipediaEditEventIrcStream implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(WikipediaEditEventIrcStream.class);

	/** Bounded queue of edit events from the channel. */
	private final BlockingQueue<WikipediaEditEvent> edits =
			new ArrayBlockingQueue<>(128);

	/** IRC connection (NOTE: this is a separate Thread). */
	private IRCConnection conn;

	WikipediaEditEventIrcStream(String host, int port) {
		final String nick = "flink-bot-" + UUID.randomUUID().toString();
		this.conn = new IRCConnection(host, new int[] { port}, "", nick, nick, nick);
		conn.addIRCEventListener(new WikipediaIrcChannelListener(edits));
		conn.setEncoding("UTF-8");
		conn.setPong(true);
		conn.setColors(false);
		conn.setDaemon(true);
		conn.setName("WikipediaEditEventIrcStreamThread");
	}

	BlockingQueue<WikipediaEditEvent> getEdits() {
		return edits;
	}

	void connect() throws IOException {
		if (!conn.isConnected()) {
			conn.connect();
		}
	}

	void join(String channel) {
		Objects.requireNonNull(channel, "channel");
		conn.send("JOIN " + channel);
	}

	void leave(String channel) {
		conn.send("PART " + channel);
	}

	@Override
	public void close() throws Exception {
		if (conn != null && conn.isConnected()) {
			conn.doQuit();
			conn.close();
			conn.join(5 * 1000);
		}
	}

	// ------------------------------------------------------------------------
	// IRC channel listener
	// ------------------------------------------------------------------------

	private static class WikipediaIrcChannelListener implements IRCEventListener {

		private final BlockingQueue<WikipediaEditEvent> edits;

		WikipediaIrcChannelListener(BlockingQueue<WikipediaEditEvent> edits) {
			this.edits = Objects.requireNonNull(edits, "edits");
		}

		@Override
		public void onPrivmsg(String target, IRCUser user, String msg) {
			LOG.debug("[{}] {}: {}.", target, user.getNick(), msg);

			WikipediaEditEvent event = WikipediaEditEvent.fromRawEvent(
					System.currentTimeMillis(),
					target,
					msg);

			if (event != null) {
				if (!edits.offer(event)) {
					LOG.debug("Dropping message, because of full queue.");
				}
			}
		}

		@Override
		public void onRegistered() {
			LOG.debug("Connected.");
		}

		@Override
		public void onDisconnected() {
			LOG.debug("Disconnected.");
		}

		@Override
		public void onError(String msg) {
			LOG.error("Error: '{}'.", msg);
		}

		@Override
		public void onError(int num, String msg) {
			LOG.error("Error #{}: '{}'.", num, msg);
		}

		@Override
		public void onInvite(String chan, IRCUser user, String passiveNick) {
			LOG.debug("[{}]: {} invites {}.", chan, user.getNick(), passiveNick);
		}

		@Override
		public void onJoin(String chan, IRCUser user) {
			LOG.debug("[{}]: {} joins.", chan, user.getNick());
		}

		@Override
		public void onKick(String chan, IRCUser user, String passiveNick, String msg) {
			LOG.debug("[{}]: {} kicks {}.", chan, user.getNick(), passiveNick);
		}

		@Override
		public void onMode(String chan, IRCUser user, IRCModeParser modeParser) {
			LOG.debug("[{}]: mode '{}'.", chan, modeParser.getLine());
		}

		@Override
		public void onMode(IRCUser user, String passiveNick, String mode) {
			LOG.debug("{} sets modes {} ({}).", user.getNick(), mode, passiveNick);
		}

		@Override
		public void onNick(IRCUser user, String newNick) {
			LOG.debug("{} is now known as {}.", user.getNick(), newNick);
		}

		@Override
		public void onNotice(String target, IRCUser user, String msg) {
			LOG.debug("[{}] {} (notice): {}.", target, user.getNick(), msg);
		}

		@Override
		public void onPart(String chan, IRCUser user, String msg) {
			LOG.debug("[{}] {} parts {}.", chan, user.getNick(), msg);
		}

		@Override
		public void onPing(String ping) {
		}

		@Override
		public void onQuit(IRCUser user, String msg) {
			LOG.debug("Quit: {}.", user.getNick());
		}

		@Override
		public void onReply(int num, String value, String msg) {
			LOG.debug("Reply #{}: {} {}.", num, value, msg);
		}

		@Override
		public void onTopic(String chan, IRCUser user, String topic) {
			LOG.debug("[{}] {} changes topic into {}.", chan, user.getNick(), topic);
		}

		@Override
		public void unknown(String prefix, String command, String middle, String trailing) {
			LOG.warn("UNKNOWN: " + prefix + " " + command + " " + middle + " " + trailing);
		}
	}
}
