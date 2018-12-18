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

.controller 'AllTaskManagersController', ($scope, TaskManagersService, $interval, flinkConfig) ->
  TaskManagersService.loadManagers().then (data) ->
    $scope.managers = data

  refresh = $interval ->
    TaskManagersService.loadManagers().then (data) ->
      $scope.managers = data
  , flinkConfig["refresh-interval"]

  $scope.$on '$destroy', ->
    $interval.cancel(refresh)

.controller 'SingleTaskManagerController', ($scope, $stateParams, SingleTaskManagerService, $interval, flinkConfig) ->
  $scope.metrics = {}
  SingleTaskManagerService.loadMetrics($stateParams.taskmanagerid).then (data) ->
      $scope.metrics = data

    refresh = $interval ->
      SingleTaskManagerService.loadMetrics($stateParams.taskmanagerid).then (data) ->
        $scope.metrics = data
    , flinkConfig["refresh-interval"]

    $scope.$on '$destroy', ->
      $interval.cancel(refresh)

.controller 'SingleTaskManagerLogsController', ($scope, $stateParams, SingleTaskManagerService, $interval, flinkConfig) ->
  $scope.log = {}
  $scope.taskmanagerid = $stateParams.taskmanagerid
  SingleTaskManagerService.loadLogs($stateParams.taskmanagerid).then (data) ->
    $scope.log = data

  $scope.reloadData = () ->
    SingleTaskManagerService.loadLogs($stateParams.taskmanagerid).then (data) ->
      $scope.log = data

.controller 'SingleTaskManagerStdoutController', ($scope, $stateParams, SingleTaskManagerService, $interval, flinkConfig) ->
  $scope.stdout = {}
  $scope.taskmanagerid = $stateParams.taskmanagerid
  SingleTaskManagerService.loadStdout($stateParams.taskmanagerid).then (data) ->
    $scope.stdout = data

  $scope.reloadData = () ->
    SingleTaskManagerService.loadStdout($stateParams.taskmanagerid).then (data) ->
      $scope.stdout = data
