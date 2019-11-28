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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.handler.cluster.DashboardConfigHandler;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.ZonedDateTime;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.Objects;

/**
 * Response of the {@link DashboardConfigHandler} containing general configuration
 * values such as the time zone and the refresh interval.
 */
public class DashboardConfiguration implements ResponseBody {

	public static final String FIELD_NAME_REFRESH_INTERVAL = "refresh-interval";
	public static final String FIELD_NAME_TIMEZONE_OFFSET = "timezone-offset";
	public static final String FIELD_NAME_TIMEZONE_NAME = "timezone-name";
	public static final String FIELD_NAME_FLINK_VERSION = "flink-version";
	public static final String FIELD_NAME_FLINK_REVISION = "flink-revision";
	public static final String FIELD_NAME_FLINK_FEATURES = "features";

	public static final String FIELD_NAME_FEATURE_WEB_SUBMIT = "web-submit";

	@JsonProperty(FIELD_NAME_REFRESH_INTERVAL)
	private final long refreshInterval;

	@JsonProperty(FIELD_NAME_TIMEZONE_NAME)
	private final String timeZoneName;

	@JsonProperty(FIELD_NAME_TIMEZONE_OFFSET)
	private final int timeZoneOffset;

	@JsonProperty(FIELD_NAME_FLINK_VERSION)
	private final String flinkVersion;

	@JsonProperty(FIELD_NAME_FLINK_REVISION)
	private final String flinkRevision;

	@JsonProperty(FIELD_NAME_FLINK_FEATURES)
	private final Features features;

	@JsonCreator
	public DashboardConfiguration(
			@JsonProperty(FIELD_NAME_REFRESH_INTERVAL) long refreshInterval,
			@JsonProperty(FIELD_NAME_TIMEZONE_NAME) String timeZoneName,
			@JsonProperty(FIELD_NAME_TIMEZONE_OFFSET) int timeZoneOffset,
			@JsonProperty(FIELD_NAME_FLINK_VERSION) String flinkVersion,
			@JsonProperty(FIELD_NAME_FLINK_REVISION) String flinkRevision,
			@JsonProperty(FIELD_NAME_FLINK_FEATURES) Features features) {
		this.refreshInterval = refreshInterval;
		this.timeZoneName = Preconditions.checkNotNull(timeZoneName);
		this.timeZoneOffset = timeZoneOffset;
		this.flinkVersion = Preconditions.checkNotNull(flinkVersion);
		this.flinkRevision = Preconditions.checkNotNull(flinkRevision);
		this.features = features;
	}

	@JsonIgnore
	public long getRefreshInterval() {
		return refreshInterval;
	}

	@JsonIgnore
	public int getTimeZoneOffset() {
		return timeZoneOffset;
	}

	@JsonIgnore
	public String getTimeZoneName() {
		return timeZoneName;
	}

	@JsonIgnore
	public String getFlinkVersion() {
		return flinkVersion;
	}

	@JsonIgnore
	public String getFlinkRevision() {
		return flinkRevision;
	}

	@JsonIgnore
	public Features getFeatures() {
		return features;
	}

	/**
	 * Collection of features that are enabled/disabled.
	 */
	public static final class Features {

		@JsonProperty(FIELD_NAME_FEATURE_WEB_SUBMIT)
		private final boolean webSubmitEnabled;

		@JsonCreator
		public Features(@JsonProperty(FIELD_NAME_FEATURE_WEB_SUBMIT) boolean webSubmitEnabled) {
			this.webSubmitEnabled = webSubmitEnabled;
		}

		@JsonIgnore
		public boolean isWebSubmitEnabled() {
			return webSubmitEnabled;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Features features = (Features) o;
			return webSubmitEnabled == features.webSubmitEnabled;
		}

		@Override
		public int hashCode() {
			return Objects.hash(webSubmitEnabled);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		DashboardConfiguration that = (DashboardConfiguration) o;
		return refreshInterval == that.refreshInterval &&
			timeZoneOffset == that.timeZoneOffset &&
			Objects.equals(timeZoneName, that.timeZoneName) &&
			Objects.equals(flinkVersion, that.flinkVersion) &&
			Objects.equals(flinkRevision, that.flinkRevision) &&
			Objects.equals(features, that.features);
	}

	@Override
	public int hashCode() {
		return Objects.hash(refreshInterval, timeZoneName, timeZoneOffset, flinkVersion, flinkRevision, features);
	}

	public static DashboardConfiguration from(long refreshInterval, ZonedDateTime zonedDateTime, boolean webSubmitEnabled) {

		final String flinkVersion = EnvironmentInformation.getVersion();

		final EnvironmentInformation.RevisionInformation revision = EnvironmentInformation.getRevisionInformation();
		final String flinkRevision;

		if (revision != null) {
			flinkRevision = revision.commitId + " @ " + revision.commitDate;
		} else {
			flinkRevision = "unknown revision";
		}

		return new DashboardConfiguration(
			refreshInterval,
			zonedDateTime.getZone().getDisplayName(TextStyle.FULL, Locale.getDefault()),
			// convert zone date time into offset in order to not do the day light saving adaptions wrt the offset
			zonedDateTime.toOffsetDateTime().getOffset().getTotalSeconds() * 1000,
			flinkVersion,
			flinkRevision,
			new Features(webSubmitEnabled));
	}
}
