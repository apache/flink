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

.service 'JobSubmitService', ($http, flinkConfig, $q) ->

  @loadJarList = () ->
    deferred = $q.defer()

    $http.get(flinkConfig.jobServer + "jars/")
    .success (data, status, headers, config) ->
      deferred.resolve(data)

    deferred.promise

  @deleteJar = (id) ->
    deferred = $q.defer()

    $http.delete(flinkConfig.jobServer + "jars/" + encodeURIComponent(id))
    .success (data, status, headers, config) ->
       deferred.resolve(data)

    deferred.promise

  @getPlan = (id, args) ->
    deferred = $q.defer()

    $http.get(flinkConfig.jobServer + "jars/" + encodeURIComponent(id) + "/plan", {params: args})
    .success (data, status, headers, config) ->
      deferred.resolve(data)

    deferred.promise

  @runJob = (id, args) ->
    deferred = $q.defer()

    $http.post(flinkConfig.jobServer + "jars/" + encodeURIComponent(id) + "/run", {}, {params: args})
    .success (data, status, headers, config) ->
      deferred.resolve(data)
    .error (err) ->
      deferred.reject(err)

    deferred.promise

  @
