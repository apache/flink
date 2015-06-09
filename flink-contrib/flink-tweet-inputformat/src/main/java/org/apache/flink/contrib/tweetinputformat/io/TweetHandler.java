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

package org.apache.flink.contrib.tweetinputformat.io;

import org.apache.flink.contrib.tweetinputformat.model.tweet.Contributors;
import org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet;
import org.apache.flink.contrib.tweetinputformat.model.tweet.entities.HashTags;
import org.json.simple.parser.ContentHandler;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class TweetHandler implements ContentHandler {

	private static final Logger logger = LoggerFactory.getLogger(TweetHandler.class);

	protected Tweet reuse;

	private int nesting = 0;

	private ObjectState objectState = ObjectState.TWEET;

	private EntryState entryState = EntryState.UNEXPECTED;

	private boolean sameHashTag = false;

	// to handle the coordinates special case of nesting primitive types
	private int coordinatesCounter = 0;

	private double coordinatesTemp = 0.0d;


	@Override
	public void startJSON() throws ParseException, IOException {
		sameHashTag = true;

	}

	@Override
	public void endJSON() throws ParseException, IOException {


	}

	@Override
	public boolean startObject() throws ParseException, IOException {

		nesting++;
		return true;
	}

	@Override
	public boolean endObject() throws ParseException, IOException {

		nesting--;

		if (this.nesting == 1) {
			this.objectState = ObjectState.TWEET;
		}

		// The handler in JSONParser checks for the "!contentHandler.endObject()", so we should
		// return false if its not the end of the object.
		return nesting > 0;
	}

	@Override
	public boolean startObjectEntry(String key) throws ParseException, IOException {

		if ((key.equals("contributors") || key.equals("user") || key.equals("geo") || key.equals("place") || key.equals("attributes") || key.equals("bounding_box"))) {
			objectState = ObjectState.valueOf(key.toUpperCase());
		} else if (key.equals("hashtags") && nesting == 2) {
			objectState = ObjectState.valueOf(key.toUpperCase());
		} else if (key.equals("coordinates") && (this.nesting == 1)) {
			objectState = ObjectState.valueOf(key.toUpperCase());
		} else {
			try {
				entryState = EntryState.valueOf(key.toUpperCase());
			} catch (IllegalArgumentException e) {

				logger.debug(e.getMessage());

			}
		}

		return true;
	}

	@Override
	public boolean endObjectEntry() throws ParseException, IOException {

		if (objectState == ObjectState.CONTRIBUTORS && nesting == 1) {
			objectState = ObjectState.TWEET;
		}

		return true;
	}

	@Override
	public boolean startArray() throws ParseException, IOException {

		return true;
	}

	@Override
	public boolean endArray() throws ParseException, IOException {

		if (objectState == ObjectState.COORDINATES) {
			coordinatesCounter = 0;
			coordinatesTemp = 0.0d;
		}


		// Some tweets have HashTags twice, this condition to read only one of them
		if (objectState == ObjectState.HASHTAGS && entryState == EntryState.INDICES && nesting == 2) {
			sameHashTag = false;
		}
		return true;
	}

	@Override
	public boolean primitive(Object value) throws ParseException, IOException {

		try {

			if (objectState == ObjectState.TWEET) {
				tweetObjectStatePrimitiveHandler(value);
			} else if (objectState == ObjectState.USER) {
				userObjectStatePrimitiveHandler(value);
			} else if (objectState == ObjectState.GEO) {

				return true;

			} else if (objectState == ObjectState.COORDINATES) {

				coordinatesObjectStatePrimitiveHandler(value);
			} else if (objectState == ObjectState.PLACE) {

				placeObjectStatePrimitiveHandler(value);
			} else if (objectState == ObjectState.GEO) {

				return true;

			} else if (objectState == ObjectState.ATTRIBUTES) {
				placeAttributesObjectStatePrimitiveHandler(value);
			} else if (objectState == ObjectState.CONTRIBUTORS) {
				contributorsObjectStatePrimitiveHandler(value);
			} else if (objectState == ObjectState.HASHTAGS && entryState == EntryState.TEXT && sameHashTag) {
				hashTagsObjectStatePrimitiveHandler(value);
			}
		} catch (Exception e) {
			logger.debug("Error in primitive type:  " + e.getMessage());
		}


		return true;
	}

	public void tweetObjectStatePrimitiveHandler(Object value) {

		switch (entryState) {
			case CREATED_AT:
				if (value != null) {
					reuse.setCreated_at((String) value);
				}
				break;
			case TEXT:
				if (value != null) {
					reuse.setText((String) value);
				}
				break;
			case ID:
				if (value != null) {
					reuse.setId((Long) value);
				}
				break;
			case ID_STR:
				if (value != null) {
					reuse.setId_str((String) value);
				}
				break;
			case SOURCE:
				if (value != null) {
					reuse.setSource((String) value);
				}
				break;
			case TRUNCATED:
				if (value != null) {
					reuse.setTruncated((Boolean) value);
				}
				break;
			case IN_REPLY_TO_STATUS_ID:
				if (value != null) {
					reuse.setIn_reply_to_status_id((Long) value);
				}
				break;
			case IN_REPLY_TO_STATUS_ID_STR:
				if (value != null) {
					reuse.setIn_reply_to_status_id_str((String) value);
				}
				break;
			case IN_REPLY_TO_USER_ID:
				if (value != null) {
					reuse.setIn_reply_to_user_id((Long) value);
				}
				break;
			case IN_REPLY_TO_USER_ID_STR:
				if (value != null) {
					reuse.setIn_reply_to_user_id_str((String) value);
				}
				break;
			case IN_REPLY_TO_SCREEN_NAME:
				if (value != null) {
					reuse.setIn_reply_to_screen_name((String) value);
				}
				break;
			case RETWEET_COUNT:
				if (value != null) {
					reuse.setRetweet_count((Long) value);
				}
				break;
			case FAVORITE_COUNT:
				if (value != null) {
					reuse.setFavorite_count((Long) value);
				}
				break;
			case FAVORITED:
				if (value != null) {
					reuse.setFavorited((Boolean) value);
				}
				break;
			case RETWEETED:
				if (value != null) {
					reuse.setRetweeted((Boolean) value);
				}
				break;
			case POSSIBLY_SENSITIVE:
				if (value != null) {
					reuse.setPossibly_sensitive((Boolean) value);
				}
				break;
			case FILTER_LEVEL:
				if (value != null) {
					reuse.setFilter_level((String) value);
				}
				break;
			case LANG:
				if (value != null) {
					reuse.setLang((String) value);
				}
				break;
		}
	}

	public void userObjectStatePrimitiveHandler(Object value) {

		switch (entryState) {
			case ID:
				if (value != null) {
					// handle format exception caused by wrong values in the "id" field in the
					// tweets.
					if (value instanceof String) {
						try {
							reuse.getUser().setId(Long.parseLong((String) value));
						} catch (NumberFormatException e) {
							reuse.getUser().setId(0L);
							logger.debug("This Tweet_ID is not a numeric type : " + (String) value);
						}
					} else {
						reuse.getUser().setId((Long) value);
					}
				}
				break;
			case ID_STR:
				if (value != null) {
					reuse.getUser().setId_str((String) value);
				}
				break;
			case NAME:
				if (value != null) {
					reuse.getUser().setName((String) value);
				}
				break;
			case SCREEN_NAME:
				if (value != null) {
					reuse.getUser().setScreen_name((String) value);
				}
				break;
			case LOCATION:
				if (value != null) {
					reuse.getUser().setLocation((String) value);
				}
				break;
			case URL:
				if (value != null) {
					reuse.getUser().setUrl((String) value);
				}
				break;
			case DESCRIPTION:
				if (value != null) {
					reuse.getUser().setDescription((String) value);
				}
				break;
			case PROTECTED:
				if (value != null) {
					reuse.getUser().setProtected_tweet((Boolean) value);
				}
				break;
			case VERIFIED:
				if (value != null) {
					reuse.getUser().setVerified((Boolean) value);
				}
				break;
			case FOLLOWERS_COUNT:
				if (value != null) {
					reuse.getUser().setFollowers_count((Long) value);
				}
				break;
			case FRIENDS_COUNT:
				if (value != null) {
					reuse.getUser().setFriends_count((Long) value);
				}
				break;
			case LISTED_COUNT:
				if (value != null) {
					reuse.getUser().setListed_count((Long) value);
				}
				break;
			case FAVOURITES_COUNT:
				if (value != null) {
					reuse.getUser().setFavourites_count((Long) value);
				}
				break;
			case STATUSES_COUNT:
				if (value != null) {
					reuse.getUser().setStatuses_count((Long) value);
				}
				break;
			case CREATED_AT:
				if (value != null) {
					reuse.getUser().setCreated_at((String) value);
				}
				break;
			case UTC_OFFSET:
				if (value != null) {
					reuse.getUser().setUtc_offset((Long) value);
				}
				break;
			case TIME_ZONE:
				if (value != null) {
					reuse.getUser().setTime_zone((String) value);
				}
				break;
			case GEO_ENABLED:
				if (value != null) {
					reuse.getUser().setGeo_enabled((Boolean) value);
				}
				break;
			case LANG:
				if (value != null) {
					reuse.getUser().setLang((String) value);
				}
				break;
			case CONTRIBUTORS_ENABLED:
				if (value != null) {
					reuse.getUser().setContributors_enabled((Boolean) value);
				}
				break;
			case IS_TRANSLATOR:
				if (value != null) {
					reuse.getUser().setIs_translator((Boolean) value);
				}
				break;
			case PROFILE_BACKGROUND_COLOR:
				if (value != null) {
					reuse.getUser().setProfile_background_color((String) value);
				}
				break;
			case PROFILE_BACKGROUND_IMAGE_URL:
				if (value != null) {
					reuse.getUser().setProfile_background_image_url((String) value);
				}
				break;
			case PROFILE_BACKGROUND_IMAGE_URL_HTTPS:
				if (value != null) {
					reuse.getUser().setProfile_background_image_url_https((String) value);
				}
				break;
			case PROFILE_BACKGROUND_TILE:
				if (value != null) {
					reuse.getUser().setProfile_background_tile((Boolean) value);
				}
				break;
			case PROFILE_LINK_COLOR:
				if (value != null) {
					reuse.getUser().setProfile_link_color((String) value);
				}
				break;
			case PROFILE_SIDEBAR_BORDER_COLOR:
				if (value != null) {
					reuse.getUser().setProfile_sidebar_border_color((String) value);
				}
				break;
			case PROFILE_SIDEBAR_FILL_COLOR:
				if (value != null) {
					reuse.getUser().setProfile_sidebar_fill_color((String) value);
				}
				break;
			case PROFILE_TEXT_COLOR:
				if (value != null) {
					reuse.getUser().setProfile_text_color((String) value);
				}
				break;
			case PROFILE_USE_BACKGROUND_IMAGE:
				if (value != null) {
					reuse.getUser().setProfile_use_background_image((Boolean) value);
				}
				break;
			case PROFILE_IMAGE_URL:
				if (value != null) {
					reuse.getUser().setProfile_image_url((String) value);
				}
				break;
			case PROFILE_IMAGE_URL_HTTPS:
				if (value != null) {
					reuse.getUser().setProfile_image_url_https((String) value);
				}
				break;
			case PROFILE_BANNER_URL:
				if (value != null) {
					reuse.getUser().setProfile_banner_url((String) value);
				}
				break;
			case DEFAULT_PROFILE:
				if (value != null) {
					reuse.getUser().setDefault_profile((Boolean) value);
				}
				break;
			case DEFAULT_PROFILE_IMAGE:
				if (value != null) {
					reuse.getUser().setDefault_profile_image((Boolean) value);
				}
				break;
			case FOLLOWING:
				if (value != null) {
					reuse.getUser().setFollowing((Boolean) value);
				}
				break;
			case FOLLOW_REQUEST_SENT:
				if (value != null) {
					reuse.getUser().setFollow_request_sent((Boolean) value);
				}
				break;
			case NOTIFICATIONS:
				if (value != null) {
					reuse.getUser().setNotifications((Boolean) value);
				}
				break;
		}
	}

	public void coordinatesObjectStatePrimitiveHandler(Object value) {

		switch (entryState) {
			case COORDINATES:
				if (value != null && this.coordinatesCounter == 0) {
					coordinatesTemp = (Double) value;
					this.coordinatesCounter++;
				} else if (value != null && this.coordinatesCounter == 1) {
					reuse.getCoordinates().setCoordinates(coordinatesTemp, (Double) value);
				} else {
					reuse.getCoordinates().setCoordinates(0.0d, 0.0d);
				}
				break;
		}

	}

	public void placeObjectStatePrimitiveHandler(Object value) {

		switch (entryState) {
			case ID:
				if (value != null) {
					reuse.getPlace().setId((String) value);
				}
				break;
			case URL:
				if (value != null) {
					reuse.getPlace().setUrl((String) value);
				}
				break;
			case PLACE_TYPE:
				if (value != null) {
					reuse.getPlace().setPlace_type((String) value);
				}
				break;
			case NAME:
				if (value != null) {
					reuse.getPlace().setName((String) value);
				}
				break;
			case FULL_NAME:
				if (value != null) {
					reuse.getPlace().setFull_name((String) value);
				}
				break;
			case COUNTRY_CODE:
				if (value != null) {
					reuse.getPlace().setCountry_code((String) value);
				}
				break;
			case COUNTRY:
				if (value != null) {
					reuse.getPlace().setCountry((String) value);
				}
				break;

			// Skipped BoundingBox -- Not Required


		}
	}

	public void placeAttributesObjectStatePrimitiveHandler(Object value) {

		switch (entryState) {
			case STREET_ADDRESS:
				if (value != null) {
					reuse.getPlace().getAttributes().setStreet_address((String) value);
				}
				break;
			case LOCALITY:
				if (value != null) {
					reuse.getPlace().getAttributes().setLocality((String) value);
				}
				break;
			case REGION:
				if (value != null) {
					reuse.getPlace().getAttributes().setRegion((String) value);
				}
				break;
			case ISO3:
				if (value != null) {
					reuse.getPlace().getAttributes().setIso3((String) value);
				}
				break;
			case POSTAL_CODE:
				if (value != null) {
					reuse.getPlace().getAttributes().setPostal_code((String) value);
				}
				break;
			case PHONE:
				if (value != null) {
					reuse.getPlace().getAttributes().setPhone((String) value);
				}
				break;
			case URL:
				if (value != null) {
					reuse.getPlace().getAttributes().setUrl((String) value);
				}
				break;
			case APP_ID:
				if (value != null) {
					reuse.getPlace().getAttributes().setAppId((String) value);
				}
				break;
			// Skipped BoundingBox -- Not Required

		}


	}

	public void contributorsObjectStatePrimitiveHandler(Object value) {

		// to handle the case of the null as contributors is an array in the Twitter documentation
		// && if it is not null we initialize the object and fill it with the data,
		if (value == null) {
			reuse.getContributors().add(new Contributors());
		} else {

			Contributors contributor = new Contributors();

			switch (entryState) {
				case ID:
					if (value != null) {
						contributor.setId((Long) value);
					}
					break;
				case ID_STR:
					if (value != null) {
						contributor.setId_str((String) value);
					}
					break;
				case TWEET_CONTRIBUTORS_SCREEN_NAME:
					if (value != null) {
						contributor.setScreenName((String) value);
					}
					break;
			}
			reuse.getContributors().add(contributor);

		}


	}

	public void hashTagsObjectStatePrimitiveHandler(Object value) {

		HashTags hashTag = new HashTags();

		if (value == null) {
			return;
		} else if (entryState == EntryState.TEXT && value != null) {
			hashTag.setText((String) value, false);
			reuse.getEntities().getHashtags().add(hashTag);
		}
	}

	private static enum ObjectState {
		TWEET,
		CONTRIBUTORS,
		USER,
		GEO,
		COORDINATES,
		PLACE,
		ATTRIBUTES,
		BOUNDING_BOX,
		HASHTAGS;

	}

	private static enum EntryState {
		TEXT,
		CREATED_AT,
		ID,
		ID_STR,
		SOURCE,
		TRUNCATED,
		IN_REPLY_TO_STATUS_ID,
		IN_REPLY_TO_STATUS_ID_STR,
		IN_REPLY_TO_USER_ID,
		IN_REPLY_TO_USER_ID_STR,
		IN_REPLY_TO_SCREEN_NAME,
		RETWEET_COUNT,
		FAVORITE_COUNT,
		FAVORITED,
		RETWEETED,
		POSSIBLY_SENSITIVE,
		FILTER_LEVEL,
		TWEET_CONTRIBUTORS_SCREEN_NAME,
		SCREEN_NAME,
		LOCATION,
		DESCRIPTION,
		PROTECTED,
		VERIFIED,
		FOLLOWERS_COUNT,
		FRIENDS_COUNT,
		LISTED_COUNT,
		FAVOURITES_COUNT,
		STATUSES_COUNT,
		UTC_OFFSET,
		TIME_ZONE,
		GEO_ENABLED,
		LANG,
		CONTRIBUTORS_ENABLED,
		IS_TRANSLATOR,
		PROFILE_BACKGROUND_COLOR,
		PROFILE_BACKGROUND_IMAGE_URL,
		PROFILE_BACKGROUND_IMAGE_URL_HTTPS,
		PROFILE_BACKGROUND_TILE,
		PROFILE_LINK_COLOR,
		PROFILE_SIDEBAR_BORDER_COLOR,
		PROFILE_SIDEBAR_FILL_COLOR,
		PROFILE_TEXT_COLOR,
		PROFILE_USE_BACKGROUND_IMAGE,
		PROFILE_IMAGE_URL,
		PROFILE_IMAGE_URL_HTTPS,
		PROFILE_BANNER_URL,
		DEFAULT_PROFILE,
		DEFAULT_PROFILE_IMAGE,
		FOLLOWING,
		FOLLOW_REQUEST_SENT,
		NOTIFICATIONS,
		TYPE,
		COORDINATES,
		PLACE_TYPE,
		NAME,
		FULL_NAME,
		COUNTRY_CODE,
		COUNTRY,
		BOUNDING_BOX,
		ATTRIBUTES,
		STREET_ADDRESS,
		LOCALITY,
		REGION,
		ISO3,
		POSTAL_CODE,
		PHONE,
		URL,
		ENTITIES,
		HASHTAGS,
		TRENDS,
		URLS,
		USER_MENTIONS,
		SYMBOLS,
		MEDIA,
		INDICES,
		MEDIA_URL,
		MEDIA_URL_HTTPS,
		DISPLAY_URL,
		EXPANDED_URL,
		SIZES,
		LARGE,
		W,
		H,
		RESIZE,
		SMALL,
		THUMB,
		MEDIUM,
		RETWEETED_STATUS,
		SOURCE_STATUS_ID,
		SOURCE_STATUS_ID_STR,
		SCOPES,
		FOLLOWERS,
		APP_ID,
		UNEXPECTED;
	}
}