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

package org.apache.flink.streaming.examples.twitter.util;

/** Example data looking like tweets, but not acquired from Twitter. */
public class TwitterExampleData {
    public static final String[] TEXTS =
            new String[] {
                "{\"created_at\":\"Mon Jan 1 00:00:00 +0000 1901\",\"id\":0,\"id_str\":\"000000000000000000\",\"text\":\"Apache Flink\",\"source\":null,\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":0,\"id_str\":\"0000000000\",\"name\":\"Apache Flink\",\"screen_name\":\"Apache Flink\",\"location\":\"Berlin\",\"protected\":false,\"verified\":false,\"followers_count\":999999,\"friends_count\":99999,\"listed_count\":999,\"favourites_count\":9999,\"statuses_count\":999,\"created_at\":\"Mon Jan 1 00:00:00 +0000 1901\",\"utc_offset\":7200,\"time_zone\":\"Amsterdam\",\"geo_enabled\":false,\"lang\":\"en\",\"entities\":{\"hashtags\":[{\"text\":\"example1\",\"indices\":[0,0]},{\"text\":\"tweet1\",\"indices\":[0,0]}]},\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"C6E2EE\",\"profile_background_tile\":false,\"profile_link_color\":\"1F98C7\",\"profile_sidebar_border_color\":\"FFFFFF\",\"profile_sidebar_fill_color\":\"252429\",\"profile_text_color\":\"666666\",\"profile_use_background_image\":true,\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null}",
                "{\"created_at\":\"Mon Jan 1 00:00:00 +0000 1901\",\"id\":1,\"id_str\":\"000000000000000000\",\"text\":\"Apache Flink\",\"source\":null,\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":0,\"id_str\":\"0000000000\",\"name\":\"Apache Flink\",\"screen_name\":\"Apache Flink\",\"location\":\"Berlin\",\"protected\":false,\"verified\":false,\"followers_count\":999999,\"friends_count\":99999,\"listed_count\":999,\"favourites_count\":9999,\"statuses_count\":999,\"created_at\":\"Mon Jan 1 00:00:00 +0000 1901\",\"utc_offset\":7200,\"time_zone\":\"Amsterdam\",\"geo_enabled\":false,\"lang\":\"en\",\"entities\":{\"hashtags\":[{\"text\":\"example2\",\"indices\":[0,0]},{\"text\":\"tweet2\",\"indices\":[0,0]}]},\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"C6E2EE\",\"profile_background_tile\":false,\"profile_link_color\":\"1F98C7\",\"profile_sidebar_border_color\":\"FFFFFF\",\"profile_sidebar_fill_color\":\"252429\",\"profile_text_color\":\"666666\",\"profile_use_background_image\":true,\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null}",
                "{\"created_at\":\"Mon Jan 1 00:00:00 +0000 1901\",\"id\":2,\"id_str\":\"000000000000000000\",\"text\":\"Apache Flink\",\"source\":null,\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":0,\"id_str\":\"0000000000\",\"name\":\"Apache Flink\",\"screen_name\":\"Apache Flink\",\"location\":\"Berlin\",\"protected\":false,\"verified\":false,\"followers_count\":999999,\"friends_count\":99999,\"listed_count\":999,\"favourites_count\":9999,\"statuses_count\":999,\"created_at\":\"Mon Jan 1 00:00:00 +0000 1901\",\"utc_offset\":7200,\"time_zone\":\"Amsterdam\",\"geo_enabled\":false,\"lang\":\"en\",\"entities\":{\"hashtags\":[{\"text\":\"example3\",\"indices\":[0,0]},{\"text\":\"tweet3\",\"indices\":[0,0]}]},\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"C6E2EE\",\"profile_background_tile\":false,\"profile_link_color\":\"1F98C7\",\"profile_sidebar_border_color\":\"FFFFFF\",\"profile_sidebar_fill_color\":\"252429\",\"profile_text_color\":\"666666\",\"profile_use_background_image\":true,\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null}",
            };

    public static final String STREAMING_COUNTS_AS_TUPLES =
            "(apache,1)\n"
                    + "(apache,2)\n"
                    + "(apache,3)\n"
                    + "(flink,1)\n"
                    + "(flink,2)\n"
                    + "(flink,3)\n";

    private TwitterExampleData() {}
}
