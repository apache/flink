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
package org.apache.flink.contrib.tweetinputformat.model.User;


import org.apache.flink.contrib.tweetinputformat.model.tweet.entities.Entities;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * {@link Users} can be anyone or anything. They {@link  org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet}
 * , follow, create lists, have a home_timeline, can be mentioned, and can be looked up in bulk.
 */
public class Users {


	private boolean contributors_enabled;

	private String created_at = "";

	private boolean default_profile;

	private boolean default_profile_image;

	private String description = "";

	private Entities entities;

	private long favourites_count;

	private boolean follow_request_sent;

	private boolean following;

	private long followers_count;

	private long friends_count;

	private boolean geo_enabled;

	private long id;

	private String id_str = "";

	private boolean is_translator;

	private String lang = "";

	private long listed_count;

	private String location = "";

	private String name = "";

	private boolean notifications;

	private String profile_background_color = "";

	private String profile_background_image_url = "";

	private String profile_background_image_url_https = "";

	private boolean profile_background_tile;

	private String profile_banner_url = "";

	private String profile_image_url = "";

	private String profile_image_url_https = "";

	private String profile_link_color = "";

	private String profile_sidebar_border_color = "";

	private String profile_sidebar_fill_color = "";

	private String profile_text_color = "";

	private boolean profile_use_background_image;

	private boolean protected_tweet;

	private String screen_name = "";

	private long statuses_count;

	private String time_zone = "";

	private String url = "";

	private long utc_offset;

	private boolean verified;

	public Users() {
		reset();
	}

	// to avoid FLINK KRYO serializer problem
	public void reset() {

		contributors_enabled = false;
		created_at = "";
		default_profile = false;
		default_profile_image = false;
		description = "";
		entities = new Entities();
		favourites_count = 0L;
		follow_request_sent = false;
		following = false;
		followers_count = 0L;
		friends_count = 0L;
		geo_enabled = false;
		id = 0L;
		id_str = "";
		is_translator = false;
		lang = "";
		listed_count = 0L;
		location = "";
		name = "";
		notifications = false;
		profile_background_color = "";
		profile_background_image_url = "";
		profile_background_image_url_https = "";
		profile_background_tile = false;
		profile_banner_url = "";
		profile_image_url = "";
		profile_image_url_https = "";
		profile_link_color = "";
		profile_sidebar_border_color = "";
		profile_sidebar_fill_color = "";
		profile_text_color = "";
		profile_use_background_image = false;
		protected_tweet = false;
		screen_name = "";
		statuses_count = 0L;
		time_zone = "";
		url = "";
		utc_offset = 0L;
		verified = false;

	}

	private String getUTCTime() {

		Date date = new Date();
		SimpleDateFormat format = new SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy");
		return format.format(date);

	}

	public boolean isContributors_enabled() {
		return contributors_enabled;
	}

	public void setContributors_enabled(boolean contributors_enabled) {
		this.contributors_enabled = contributors_enabled;
	}

	public String getCreated_at() {
		return created_at;
	}

	public void setCreated_at(String created_at) {
		this.created_at = created_at;
	}

	public boolean isDefault_profile() {
		return default_profile;
	}

	public void setDefault_profile(boolean default_profile) {
		this.default_profile = default_profile;
	}

	public boolean isDefault_profile_image() {
		return default_profile_image;
	}

	public void setDefault_profile_image(boolean default_profile_image) {
		this.default_profile_image = default_profile_image;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Entities getEntities() {
		return entities;
	}

	public void setEntities(Entities entities) {
		this.entities = entities;
	}

	public long getFavourites_count() {
		return favourites_count;
	}

	public void setFavourites_count(long favourites_count) {
		this.favourites_count = favourites_count;
	}

	public boolean isFollow_request_sent() {
		return follow_request_sent;
	}

	public void setFollow_request_sent(boolean follow_request_sent) {
		this.follow_request_sent = follow_request_sent;
	}

	public boolean isFollowing() {
		return following;
	}

	public void setFollowing(boolean following) {
		this.following = following;
	}

	public long getFollowers_count() {
		return followers_count;
	}

	public void setFollowers_count(long followers_count) {
		this.followers_count = followers_count;
	}

	public long getFriends_count() {
		return friends_count;
	}

	public void setFriends_count(long friends_count) {
		this.friends_count = friends_count;
	}

	public boolean isGeo_enabled() {
		return geo_enabled;
	}

	public void setGeo_enabled(boolean geo_enabled) {
		this.geo_enabled = geo_enabled;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getId_str() {
		return Long.toString(id);
	}

	public void setId_str(String id_str) {
		this.id_str = id_str;
	}

	public boolean isIs_translator() {
		return is_translator;
	}

	public void setIs_translator(boolean is_translator) {
		this.is_translator = is_translator;
	}

	public String getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}

	public long getListed_count() {
		return listed_count;
	}

	public void setListed_count(long listed_count) {
		this.listed_count = listed_count;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isNotifications() {
		return notifications;
	}

	public void setNotifications(boolean notifications) {
		this.notifications = notifications;
	}

	public String getProfile_background_color() {
		return profile_background_color;
	}

	public void setProfile_background_color(String profile_background_color) {
		this.profile_background_color = profile_background_color;
	}

	public String getProfile_background_image_url() {
		return profile_background_image_url;
	}

	public void setProfile_background_image_url(String profile_background_image_url) {
		this.profile_background_image_url = profile_background_image_url;
	}

	public String getProfile_background_image_url_https() {
		return profile_background_image_url_https;
	}

	public void setProfile_background_image_url_https(String profile_background_image_url_https) {
		this.profile_background_image_url_https = profile_background_image_url_https;
	}

	public boolean isProfile_background_tile() {
		return profile_background_tile;
	}

	public void setProfile_background_tile(boolean profile_background_tile) {
		this.profile_background_tile = profile_background_tile;
	}

	public String getProfile_banner_url() {
		return profile_banner_url;
	}

	public void setProfile_banner_url(String profile_banner_url) {
		this.profile_banner_url = profile_banner_url;
	}

	public String getProfile_image_url() {
		return profile_image_url;
	}

	public void setProfile_image_url(String profile_image_url) {
		this.profile_image_url = profile_image_url;
	}

	public String getProfile_image_url_https() {
		return profile_image_url_https;
	}

	public void setProfile_image_url_https(String profile_image_url_https) {
		this.profile_image_url_https = profile_image_url_https;
	}

	public String getProfile_link_color() {
		return profile_link_color;
	}

	public void setProfile_link_color(String profile_link_color) {
		this.profile_link_color = profile_link_color;
	}

	public String getProfile_sidebar_border_color() {
		return profile_sidebar_border_color;
	}

	public void setProfile_sidebar_border_color(String profile_sidebar_border_color) {
		this.profile_sidebar_border_color = profile_sidebar_border_color;
	}

	public String getProfile_sidebar_fill_color() {
		return profile_sidebar_fill_color;
	}

	public void setProfile_sidebar_fill_color(String profile_sidebar_fill_color) {
		this.profile_sidebar_fill_color = profile_sidebar_fill_color;
	}

	public String getProfile_text_color() {
		return profile_text_color;
	}

	public void setProfile_text_color(String profile_text_color) {
		this.profile_text_color = profile_text_color;
	}

	public boolean isProfile_use_background_image() {
		return profile_use_background_image;
	}

	public void setProfile_use_background_image(boolean profile_use_background_image) {
		this.profile_use_background_image = profile_use_background_image;
	}

	public boolean isProtected_tweet() {
		return protected_tweet;
	}

	public void setProtected_tweet(boolean protected_tweet) {
		this.protected_tweet = protected_tweet;
	}

	public String getScreen_name() {
		return screen_name;
	}

	public void setScreen_name(String screen_name) {
		this.screen_name = screen_name;
	}

	public long getStatuses_count() {
		return statuses_count;
	}

	public void setStatuses_count(long statuses_count) {
		this.statuses_count = statuses_count;
	}

	public String getTime_zone() {
		return time_zone;
	}

	public void setTime_zone(String time_zone) {
		this.time_zone = time_zone;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public long getUtc_offset() {
		return utc_offset;
	}

	public void setUtc_offset(long utc_offset) {
		this.utc_offset = utc_offset;
	}

	public boolean isVerified() {
		return verified;
	}

	public void setVerified(boolean verified) {
		this.verified = verified;
	}
}
