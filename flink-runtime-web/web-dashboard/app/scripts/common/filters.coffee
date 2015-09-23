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

.filter "humanizeText", ->
  (text) ->
    # TODO: extend... a lot
    if text then text.replace(/&gt;/g, ">").replace(/<br\/>/g,"") else ''

.filter "bytes", ->
  (bytes, precision) ->
    return "-"  if isNaN(parseFloat(bytes)) or not isFinite(bytes)
    precision = 1  if typeof precision is "undefined"
    units = [ "bytes", "kB", "MB", "GB", "TB", "PB" ]
    number = Math.floor(Math.log(bytes) / Math.log(1024))
    (bytes / Math.pow(1024, Math.floor(number))).toFixed(precision) + " " + units[number]
