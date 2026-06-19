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

package org.apache.flink.runtime.webmonitor.testutils;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/** Collection of HTTP utils. */
public class HttpUtils {
    public static Tuple2<Integer, String> getFromHTTP(String url) throws Exception {
        URL u = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) u.openConnection();
        connection.setConnectTimeout(100000);
        connection.connect();
        InputStream is;
        if (connection.getResponseCode() >= 400) {
            // error!
            is = connection.getErrorStream();
        } else {
            is = connection.getInputStream();
        }

        return Tuple2.of(
                connection.getResponseCode(),
                IOUtils.toString(
                        is,
                        connection.getContentEncoding() != null
                                ? connection.getContentEncoding()
                                : "UTF-8"));
    }
}
