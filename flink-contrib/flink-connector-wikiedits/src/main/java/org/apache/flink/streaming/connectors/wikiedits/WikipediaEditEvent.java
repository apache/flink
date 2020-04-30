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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Instances of this class represent edits made on Wikipedia.
 */
public class WikipediaEditEvent {

	// Metadata
	private final long timestamp;
	private final String channel;

	// Edit attributes
	private final String title;
	private final String diffUrl;
	private final String user;
	private final int byteDiff;
	private final String summary;
	private final int flags;

	public WikipediaEditEvent(
			long timestamp,
			String channel,
			String title,
			String diffUrl,
			String user,
			int byteDiff,
			String summary,
			boolean isMinor,
			boolean isNew,
			boolean isUnpatrolled,
			boolean isBotEdit,
			boolean isSpecial,
			boolean isTalk) {

		if (channel == null || title == null || diffUrl == null ||
				user == null || summary == null) {
			throw new NullPointerException();
		}

		this.timestamp = timestamp;
		this.channel = channel;

		this.title = title;
		this.diffUrl = diffUrl;
		this.user = user;
		this.byteDiff = byteDiff;
		this.summary = summary;
		this.flags = getFlags(
				isMinor,
				isNew,
				isUnpatrolled,
				isBotEdit,
				isSpecial,
				isTalk);
	}

	/**
	 * Returns the timestamp when this event arrived at the source.
	 *
	 * @return The timestamp assigned at the source.
	 */
	public long getTimestamp() {
		return timestamp;
	}

	public String getChannel() {
		return channel;
	}

	public String getTitle() {
		return title;
	}

	public String getDiffUrl() {
		return diffUrl;
	}

	public String getUser() {
		return user;
	}

	public int getByteDiff() {
		return byteDiff;
	}

	public String getSummary() {
		return summary;
	}

	public boolean isMinor() {
		return (flags & IS_MINOR) > 0;
	}

	public boolean isNew() {
		return (flags & IS_NEW) > 0;
	}

	public boolean isUnpatrolled() {
		return (flags & IS_UNPATROLLED) > 0;
	}

	public boolean isBotEdit() {
		return (flags & IS_BOT_EDIT) > 0;
	}

	public boolean isSpecial() {
		return (flags & IS_SPECIAL) > 0;
	}

	public boolean isTalk() {
		return (flags & IS_TALK) > 0;
	}

	@Override
	public String toString() {
		return "WikipediaEditEvent{" +
				"timestamp=" + timestamp +
				", channel='" + channel + '\'' +
				", title='" + title + '\'' +
				", diffUrl='" + diffUrl + '\'' +
				", user='" + user + '\'' +
				", byteDiff=" + byteDiff +
				", summary='" + summary + '\'' +
				", flags=" + flags +
				'}';
	}

	// - Flags ----------------------------------------------------------------

	private static final byte IS_MINOR = 0B000001;
	private static final byte IS_NEW = 0B000010;
	private static final byte IS_UNPATROLLED = 0B000100;
	private static final byte IS_BOT_EDIT = 0B001000;
	private static final byte IS_SPECIAL = 0B010000;
	private static final byte IS_TALK = 0B100000;

	private byte getFlags(
			boolean isMinor,
			boolean isNew,
			boolean isUnpatrolled,
			boolean isBotEdit,
			boolean isSpecial,
			boolean isTalk) {

		byte flag = 0;
		flag |= isMinor ? IS_MINOR : flag;
		flag |= isNew ? IS_NEW : flag;
		flag |= isUnpatrolled ? IS_UNPATROLLED : flag;
		flag |= isBotEdit ? IS_BOT_EDIT : flag;
		flag |= isSpecial ? IS_SPECIAL : flag;
		flag |= isTalk ? IS_TALK : flag;

		return flag;
	}

	// - Parser ---------------------------------------------------------------

	/** Expected pattern of raw events. */
	private static final Pattern p = Pattern.compile("\\[\\[(.*)\\]\\]\\s(.*)\\s(.*)\\s\\*\\s(.*)\\s\\*\\s\\(\\+?(.\\d*)\\)\\s(.*)");

	public static WikipediaEditEvent fromRawEvent(
			long timestamp,
			String channel,
			String rawEvent) {
		final Matcher m = p.matcher(rawEvent);

		if (m.find() && m.groupCount() == 6) {
			String title = m.group(1);
			String flags = m.group(2);
			String diffUrl = m.group(3);
			String user = m.group(4);
			int byteDiff = Integer.parseInt(m.group(5));
			String summary = m.group(6);

			boolean isMinor = flags.contains("M");
			boolean isNew = flags.contains("N");
			boolean isUnpatrolled = flags.contains("!");
			boolean isBotEdit = flags.contains("B");
			boolean isSpecial = title.startsWith("Special:");
			boolean isTalk = title.startsWith("Talk:");

			return new WikipediaEditEvent(
					timestamp,
					channel,
					title,
					diffUrl,
					user,
					byteDiff,
					summary,
					isMinor,
					isNew,
					isUnpatrolled,
					isBotEdit,
					isSpecial,
					isTalk);
		}

		return null;
	}
}
