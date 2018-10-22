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

.controller 'JobManagerConfigController', ($scope, JobManagerConfigService) ->
  JobManagerConfigService.loadConfig().then (data) ->
    if !$scope.jobmanager?
      $scope.jobmanager = {}
    $scope.jobmanager['config'] = data

.controller 'JobManagerLogsController', ($scope, JobManagerLogsService) ->  
  JobManagerLogsService.loadLogList().then (data) ->
    if !$scope.jobmanager?
      $scope.jobmanager = {}
    $scope.jobmanager['loglist'] = data['loglist']
    $scope.filename = $scope.jobmanager['loglist'][0]

  JobManagerLogsService.loadLogs(0, 100 * 1024).then (data) ->
    if !$scope.jobmanager?
      $scope.jobmanager = {}
    $scope.jobmanager['log'] = data

  $scope.reloadData = () ->
    JobManagerLogsService.loadLogs($scope.start * 1024, $scope.size * 1024).then (data) ->
      $scope.jobmanager['log'] = data
    
  $scope.searchLog = () ->
    JobManagerLogsService.loadOtherLogs($scope.filename, $scope.start * 1024, $scope.size * 1024).then (data) ->
           $scope.jobmanager['log'] = data

  $scope.loadLogList = () ->
    JobManagerLogsService.loadLogList().then (data) ->
      $scope.jobmanager['loglist'] = data['loglist']

.controller 'JobManagerStdoutController', ($scope, JobManagerStdoutService) ->
  JobManagerStdoutService.loadStdout(0, 100 * 1024).then (data) ->
    if !$scope.jobmanager?
      $scope.jobmanager = {}
    $scope.jobmanager['stdout'] = data

  $scope.reloadData = () ->
    JobManagerStdoutService.loadStdout($scope.start * 1024, $scope.size * 1024).then (data) ->
      $scope.jobmanager['stdout'] = data

  $scope.searchStdout = () ->
    JobManagerStdoutService.loadStdout($scope.start * 1024, $scope.size * 1024).then (data) ->
      $scope.jobmanager['stdout'] = data