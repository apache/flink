#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

angular.module('flinkApp')

.filter "amDurationFormatExtended", (angularMomentConfig) ->
  amDurationFormatExtendedFilter = (value, format, durationFormat) ->
    return ""  if typeof value is "undefined" or value is null

    moment.duration(value, format).format(durationFormat, { trim: false })

  amDurationFormatExtendedFilter.$stateful = angularMomentConfig.statefulFilters

  amDurationFormatExtendedFilter

.filter "humanizeDuration", ->
  (value, short) ->
    return "" if typeof value is "undefined" or value is null
    ms = value % 1000
    x = Math.floor(value / 1000)
    seconds = x % 60
    x = Math.floor(x / 60)
    minutes = x % 60
    x = Math.floor(x / 60)
    hours = x % 24
    x = Math.floor(x / 24)
    days = x
    if days == 0
      if hours == 0
        if minutes == 0
          if seconds == 0
            return ms + "ms"
          else
            return seconds + "s "
        else
          return minutes + "m " + seconds + "s"
      else
        if short then return hours + "h " + minutes + "m" else return hours + "h " + minutes + "m " + seconds + "s"
    else
      if short then return days + "d " + hours + "h" else return days + "d " + hours + "h " + minutes + "m " + seconds + "s"

.filter "limit", ->
  (text) ->
    if (text.length > 73)
      text = text.substring(0, 35) + "..." + text.substring(text.length - 35, text.length)
    text

.filter "humanizeText", ->
  (text) ->
    # TODO: extend... a lot
    if text then text.replace(/&gt;/g, ">").replace(/<br\/>/g,"") else ''

.filter "humanizeBytes", ->
  (bytes) ->
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"]
    converter = (value, power) ->
      base = Math.pow(1024, power)
      if value < base
        return (value / base).toFixed(2) + " " + units[power]
      else if value < base * 1000
        return (value / base).toPrecision(3) + " " + units[power]
      else
        return converter(value, power + 1)
    return "" if typeof bytes is "undefined" or bytes is null
    if bytes < 1000 then bytes + " B" else converter(bytes, 1)

.filter "toLocaleString", ->
  (text) -> text.toLocaleString()

.filter "toUpperCase", ->
  (text) -> text.toUpperCase()

.filter "percentage", ->
  (number) -> (number * 100).toFixed(0) + '%'

.filter "humanizeWatermark", (watermarksConfig) ->
  (value) ->
    if isNaN(value) || value <= watermarksConfig.noWatermark
      return 'No Watermark'
    else
      return value

.filter "increment", ->
  (number) ->
    parseInt(number) + 1

.filter "humanizeChartNumeric", ['humanizeBytesFilter', 'humanizeDurationFilter', (humanizeBytesFilter, humanizeDurationFilter)->
  (value, metric)->
    return_val = ''
    if value != null
      if /bytes/i.test(metric.id) && /persecond/i.test(metric.id)
        return_val = humanizeBytesFilter(value) + ' / s'
      else if /bytes/i.test(metric.id)
        return_val = humanizeBytesFilter(value)
      else if /persecond/i.test(metric.id)
        return_val = value + ' / s'
      else if /time/i.test(metric.id) || /latency/i.test(metric.id)
        return_val = humanizeDurationFilter(value, true)
      else
        return_val = value
    return return_val
]

.filter "humanizeChartNumericTitle", ['humanizeDurationFilter', (humanizeDurationFilter)->
  (value, metric)->
    return_val = ''
    if value != null
      if /bytes/i.test(metric.id) && /persecond/i.test(metric.id)
        return_val = value + ' Bytes / s'
      else if /bytes/i.test(metric.id)
        return_val = value + ' Bytes'
      else if /persecond/i.test(metric.id)
        return_val = value + ' / s'
      else if /time/i.test(metric.id) || /latency/i.test(metric.id)
        return_val = humanizeDurationFilter(value, false)
      else
        return_val = value
    return return_val
]

.filter "searchMetrics", ->
  (availableMetrics, query)->
    queryRegex = new RegExp(query, "gi")
    return (metric for metric in availableMetrics when metric.id.match(queryRegex))

