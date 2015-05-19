/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.twitter;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.httpclient.auth.Authentication;

/**
 * 
 * An extension of {@link TwitterSource} by filter parameters. This extension
 * enables to filter the twitter stream by user defined parameters.
 */
public class TwitterFilterSource extends TwitterSource {

	private static final Logger LOG = LoggerFactory
			.getLogger(TwitterFilterSource.class);

	private static final long serialVersionUID = 1L;

	private List<String> trackTerms = new LinkedList<String>();

	private List<String> languages = new LinkedList<String>();

	private List<Long> followings = new LinkedList<Long>();

	private List<Location> locations = new LinkedList<Location>();

	private Map<String, String> queryParameters = new HashMap<String, String>();

	private Map<String, String> postParameters = new HashMap<String, String>();

	public TwitterFilterSource(String authPath) {
		super(authPath);
	}

	@Override
	protected void initializeConnection() {

		if (LOG.isInfoEnabled()) {
			LOG.info("Initializing Twitter Streaming API connection");
		}
		queue = new LinkedBlockingQueue<String>(queueSize);

		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		configEndpoint(endpoint);
		endpoint.stallWarnings(false);

		Authentication auth = authenticate();

		initializeClient(endpoint, auth);

		if (LOG.isInfoEnabled()) {
			LOG.info("Twitter Streaming API connection established successfully");
		}
	}

	/**
	 * This function configures the streaming endpoint
	 * 
	 * @param endpoint
	 *            The streaming endpoint
	 */
	private void configEndpoint(StatusesFilterEndpoint endpoint) {
		if (!trackTerms.isEmpty()) {
			endpoint.trackTerms(trackTerms);
		}
		if (!languages.isEmpty()) {
			endpoint.languages(languages);
		}
		if (!followings.isEmpty()) {
			endpoint.followings(followings);
		}
		if (!locations.isEmpty()) {
			endpoint.locations(locations);
		}
		if (!queryParameters.isEmpty()) {
			for (Entry<String, String> entry : queryParameters.entrySet()) {
				endpoint.addQueryParameter(entry.getKey(), entry.getValue());
			}
		}
		if (!postParameters.isEmpty()) {
			for (Entry<String, String> entry : postParameters.entrySet()) {
				endpoint.addPostParameter(entry.getKey(), entry.getValue());
			}
		}
	}

	/**
	 * This function sets which term to track.
	 * 
	 * @param term
	 *            The term to track.
	 */
	public void trackTerm(String term) {
		this.trackTerms.add(term);
	}

	/**
	 * This function sets which terms to track.
	 * 
	 * @param terms
	 *            The terms to track.
	 */
	public void trackTerms(Collection<String> terms) {
		this.trackTerms.addAll(terms);
	}

	/**
	 * This function tells which terms are tracked.
	 */
	public List<String> getTrackTerms() {
		return this.trackTerms;
	}

	/**
	 * This function sets which language to filter.
	 * 
	 * @param language
	 *            The language to filter.
	 */
	public void filterLanguage(String language) {
		this.languages.add(language);
	}

	/**
	 * This function sets which languages to filter.
	 * 
	 * @param languages
	 *            The languages to filter.
	 */
	public void filterLanguages(Collection<String> languages) {
		this.languages.addAll(languages);
	}

	/**
	 * This function tells which languages are filtered.
	 */
	public List<String> getLanguages() {
		return this.languages;
	}

	/**
	 * This function sets which user to follow.
	 * 
	 * @param userID
	 *            The ID of the user to follow.
	 */
	public void filterFollowings(Long userID) {
		this.followings.add(userID);
	}

	/**
	 * This function sets which users to follow.
	 * 
	 * @param userIDs
	 *            The IDs of the users to follow.
	 */
	public void filterFollowings(Collection<Long> userIDs) {
		this.followings.addAll(userIDs);
	}

	/**
	 * This function tells which users are followed.
	 */
	public List<Long> getFollowings() {
		return this.followings;
	}

	/**
	 * This function sets which location to filter.
	 * 
	 * @param location
	 *            The location to filter.
	 */
	public void filterLocation(Location location) {
		this.locations.add(location);
	}

	/**
	 * This function sets which locations to filter.
	 * 
	 * @param locations
	 *            The locations to filter.
	 */
	public void filterLocations(Collection<Location> locations) {
		this.locations.addAll(locations);
	}

	/**
	 * This function tells which locations are filtered.
	 */
	public List<Location> getLocations() {
		return this.locations;
	}

	/**
	 * This function sets a query parameter.
	 * 
	 * @param parameter
	 *            The name of the query parameter.
	 * @param value
	 *            The value of the query parameter.
	 */
	public void addQueryParameter(String parameter, String value) {
		this.queryParameters.put(parameter, value);
	}

	/**
	 * This function sets query parameters.
	 * 
	 * @param queryParameters
	 *            The query parameters for the endpoint.
	 */
	public void addQueryParameters(Map<String, String> queryParameters) {
		this.queryParameters.putAll(queryParameters);
	}

	/**
	 * This function tells which query parameters are used by the endpoint.
	 */
	public Map<String, String> getQueryParameters() {
		return this.queryParameters;
	}

	/**
	 * This function sets a post parameter.
	 * 
	 * @param parameter
	 *            The name of the post parameter.
	 * @param value
	 *            The value of the post parameter.
	 */
	public void addPostParameter(String parameter, String value) {
		this.postParameters.put(parameter, value);
	}

	/**
	 * This function sets post parameters.
	 * 
	 * @param postParameters
	 *              The post parameters for the endpoint.
	 */
	public void addPostParameters(Map<String, String> postParameters) {
		this.postParameters.putAll(postParameters);
	}

	/**
	 * This function tells which post parameters are used by the endpoint.
	 */
	public Map<String, String> postParameters() {
		return this.postParameters;
	}
}
