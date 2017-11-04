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

.service 'TaskManagersService', ($http, flinkConfig, $q) ->
  @loadManagers = () ->
    deferred = $q.defer()

    $http.get(flinkConfig.jobServer + "taskmanagers")
    .success (data, status, headers, config) ->
      deferred.resolve(data['taskmanagers'])

    deferred.promise

  @

.service 'SingleTaskManagerService', ($http, flinkConfig, $q) ->
  @loadMetrics = (taskmanagerid) ->
    deferred = $q.defer()

    $http.get(flinkConfig.jobServer + "taskmanagers/" + taskmanagerid)
    .success (data, status, headers, config) ->
      deferred.resolve(data)

    deferred.promise

  @loadLogs = (taskmanagerid) ->
    deferred = $q.defer()

    $http.get(flinkConfig.jobServer + "taskmanagers/" + taskmanagerid + "/log")
    .success (data, status, headers, config) ->
      deferred.resolve(data)

    deferred.promise

  @loadStdout = (taskmanagerid) ->
    deferred = $q.defer()

    $http.get(flinkConfig.jobServer + "taskmanagers/" + taskmanagerid + "/stdout")
    .success (data, status, headers, config) ->
      deferred.resolve(data)

    deferred.promise

  @

