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

.controller 'RunningJobsController', ($scope, $state, $stateParams, JobsService) ->
  $scope.jobObserver = ->
    $scope.jobs = JobsService.getJobs('running')

  JobsService.registerObserver($scope.jobObserver)
  $scope.$on '$destroy', ->
    JobsService.unRegisterObserver($scope.jobObserver)

  $scope.jobObserver()

# --------------------------------------

.controller 'CompletedJobsController', ($scope, $state, $stateParams, JobsService) ->
  $scope.jobObserver = ->
    $scope.jobs = JobsService.getJobs('finished')

  JobsService.registerObserver($scope.jobObserver)
  $scope.$on '$destroy', ->
    JobsService.unRegisterObserver($scope.jobObserver)

  $scope.jobObserver()

# --------------------------------------

.controller 'SingleJobController', ($scope, $state, $stateParams, JobsService, $rootScope, flinkConfig, $interval) ->
  console.log 'SingleJobController'

  $scope.jobid = $stateParams.jobid
  $scope.job = null
  $scope.plan = null
  $scope.vertices = null
  $scope.jobCheckpointStats = null
  $scope.showHistory = false
  $scope.backPressureOperatorStats = {}

  JobsService.loadJob($stateParams.jobid).then (data) ->
    $scope.job = data
    $scope.plan = data.plan
    $scope.vertices = data.vertices

  refresher = $interval ->
    JobsService.loadJob($stateParams.jobid).then (data) ->
      $scope.job = data

      $scope.$broadcast 'reload'

  , flinkConfig["refresh-interval"]

  $scope.$on '$destroy', ->
    $scope.job = null
    $scope.plan = null
    $scope.vertices = null
    $scope.jobCheckpointStats = null
    $scope.backPressureOperatorStats = null

    $interval.cancel(refresher)

  $scope.cancelJob = (cancelEvent) ->
    angular.element(cancelEvent.currentTarget).removeClass("btn").removeClass("btn-default").html('Cancelling...')
    JobsService.cancelJob($stateParams.jobid).then (data) ->
      {}

  $scope.stopJob = (stopEvent) ->
    angular.element(stopEvent.currentTarget).removeClass("btn").removeClass("btn-default").html('Stopping...')
    JobsService.stopJob($stateParams.jobid).then (data) ->
      {}

  $scope.toggleHistory = ->
    $scope.showHistory = !$scope.showHistory

# --------------------------------------

.controller 'JobPlanController', ($scope, $state, $stateParams, JobsService) ->
  console.log 'JobPlanController'

  $scope.nodeid = null
  $scope.nodeUnfolded = false
  $scope.stateList = JobsService.stateList()

  $scope.changeNode = (nodeid) ->
    if nodeid != $scope.nodeid
      $scope.nodeid = nodeid
      $scope.vertex = null
      $scope.subtasks = null
      $scope.accumulators = null
      $scope.operatorCheckpointStats = null

      $scope.$broadcast 'reload'

    else
      $scope.nodeid = null
      $scope.nodeUnfolded = false
      $scope.vertex = null
      $scope.subtasks = null
      $scope.accumulators = null
      $scope.operatorCheckpointStats = null

  $scope.deactivateNode = ->
    $scope.nodeid = null
    $scope.nodeUnfolded = false
    $scope.vertex = null
    $scope.subtasks = null
    $scope.accumulators = null
    $scope.operatorCheckpointStats = null

  $scope.toggleFold = ->
    $scope.nodeUnfolded = !$scope.nodeUnfolded

# --------------------------------------

.controller 'JobPlanSubtasksController', ($scope, JobsService) ->
  console.log 'JobPlanSubtasksController'

  if $scope.nodeid and (!$scope.vertex or !$scope.vertex.st)
    JobsService.getSubtasks($scope.nodeid).then (data) ->
      $scope.subtasks = data

  $scope.$on 'reload', (event) ->
    console.log 'JobPlanSubtasksController'
    if $scope.nodeid
      JobsService.getSubtasks($scope.nodeid).then (data) ->
        $scope.subtasks = data

# --------------------------------------

.controller 'JobPlanTaskManagersController', ($scope, JobsService) ->
  console.log 'JobPlanTaskManagersController'

  if $scope.nodeid and (!$scope.vertex or !$scope.vertex.st)
    JobsService.getTaskManagers($scope.nodeid).then (data) ->
      $scope.taskmanagers = data

  $scope.$on 'reload', (event) ->
    console.log 'JobPlanTaskManagersController'
    if $scope.nodeid
      JobsService.getTaskManagers($scope.nodeid).then (data) ->
        $scope.taskmanagers = data

# --------------------------------------

.controller 'JobPlanAccumulatorsController', ($scope, JobsService) ->
  console.log 'JobPlanAccumulatorsController'

  if $scope.nodeid and (!$scope.vertex or !$scope.vertex.accumulators)
    JobsService.getAccumulators($scope.nodeid).then (data) ->
      $scope.accumulators = data.main
      $scope.subtaskAccumulators = data.subtasks

  $scope.$on 'reload', (event) ->
    console.log 'JobPlanAccumulatorsController'
    if $scope.nodeid
      JobsService.getAccumulators($scope.nodeid).then (data) ->
        $scope.accumulators = data.main
        $scope.subtaskAccumulators = data.subtasks

# --------------------------------------

.controller 'JobPlanCheckpointsController', ($scope, JobsService) ->
  console.log 'JobPlanCheckpointsController'

  # Get the per job stats
  JobsService.getJobCheckpointStats($scope.jobid).then (data) ->
    $scope.jobCheckpointStats = data

  # Get the per operator stats
  if $scope.nodeid and (!$scope.vertex or !$scope.vertex.operatorCheckpointStats)
    JobsService.getOperatorCheckpointStats($scope.nodeid).then (data) ->
      $scope.operatorCheckpointStats = data.operatorStats
      $scope.subtasksCheckpointStats = data.subtasksStats

  $scope.$on 'reload', (event) ->
    console.log 'JobPlanCheckpointsController'

    JobsService.getJobCheckpointStats($scope.jobid).then (data) ->
      $scope.jobCheckpointStats = data

    if $scope.nodeid
      JobsService.getOperatorCheckpointStats($scope.nodeid).then (data) ->
        $scope.operatorCheckpointStats = data.operatorStats
        $scope.subtasksCheckpointStats = data.subtasksStats

# --------------------------------------

.controller 'JobPlanBackPressureController', ($scope, JobsService) ->
  console.log 'JobPlanBackPressureController'
  $scope.now = Date.now()

  if $scope.nodeid
    JobsService.getOperatorBackPressure($scope.nodeid).then (data) ->
      $scope.backPressureOperatorStats[$scope.nodeid] = data

  $scope.$on 'reload', (event) ->
    console.log 'JobPlanBackPressureController (relaod)'
    $scope.now = Date.now()

    if $scope.nodeid
      JobsService.getOperatorBackPressure($scope.nodeid).then (data) ->
        $scope.backPressureOperatorStats[$scope.nodeid] = data

# --------------------------------------

.controller 'JobTimelineVertexController', ($scope, $state, $stateParams, JobsService) ->
  console.log 'JobTimelineVertexController'

  JobsService.getVertex($stateParams.vertexId).then (data) ->
    $scope.vertex = data

  $scope.$on 'reload', (event) ->
    console.log 'JobTimelineVertexController'
    JobsService.getVertex($stateParams.vertexId).then (data) ->
      $scope.vertex = data

# --------------------------------------

.controller 'JobExceptionsController', ($scope, $state, $stateParams, JobsService) ->
  JobsService.loadExceptions().then (data) ->
    $scope.exceptions = data

# --------------------------------------

.controller 'JobPropertiesController', ($scope, JobsService) ->
  console.log 'JobPropertiesController'

  $scope.changeNode = (nodeid) ->
    if nodeid != $scope.nodeid
      $scope.nodeid = nodeid

      JobsService.getNode(nodeid).then (data) ->
        $scope.node = data

    else
      $scope.nodeid = null
      $scope.node = null
