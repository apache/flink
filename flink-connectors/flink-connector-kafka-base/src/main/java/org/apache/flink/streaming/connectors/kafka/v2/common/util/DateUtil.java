/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * 	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.v2.common.util;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.commons.lang3.time.FastDateFormat;

import javax.annotation.Nonnull;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

/** Utils. **/
public class DateUtil {
	private static FastDateFormat dfTimeStamp = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
	private static ConcurrentHashMap<String, FastDateFormat> sdfCache = new ConcurrentHashMap<>();

	private static FastDateFormat getDateFormat(String timeZone, String format){
		String key = String.valueOf(timeZone) + String.valueOf(format);
		if (null == timeZone || timeZone.isEmpty()){
			return dfTimeStamp;
		}
		if (sdfCache.containsKey(key)){
			return sdfCache.get(key);
		} else {
			FastDateFormat sdf = FastDateFormat.getInstance(format, TimeZone.getTimeZone(timeZone));
			sdfCache.put(key, sdf);
			return sdf;
		}
	}

	public static String timeStamp2String(Timestamp value, String timeZone) {
		String res;
		if (null == timeZone || timeZone.isEmpty()) {
			res = dfTimeStamp.format(value);
			return res;
		} else {
			return timeStamp2String(value, timeZone, "yyyy-MM-dd HH:mm:ss");
		}
	}

	public static String timeStamp2String(Timestamp value, String timeZone, @Nonnull String format) {
		FastDateFormat sdf = getDateFormat(timeZone, format);
		return sdf.format(value);
	}

	public static String date2String(Date value, String timeZone) {
			return date2String(value, timeZone, "yyyy-MM-dd");
	}

	public static String date2String(Date value, String timeZone, @Nonnull String format) {
		FastDateFormat sdf = getDateFormat(timeZone, format);
		return sdf.format(value);
	}

	public static Long parseDateString(
			String formatString,
			String dateString,
			String timeZone) throws ParseException {
		FastDateFormat simpleDateFormat = getDateFormat(timeZone, formatString);
		return simpleDateFormat.parse(dateString).getTime();
	}

	public static boolean isTimeInRange(List<Tuple2<Long, Long>> rangeList, long time) {
		for (Tuple2<Long, Long> range : rangeList) {
			if (range.f0 <= time && time <= range.f1) {
				return true;
			}
		}
		return false;
	}
}
