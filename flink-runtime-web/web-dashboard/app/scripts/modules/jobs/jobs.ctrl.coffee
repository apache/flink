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

.controller 'SingleJobController', ($scope, $state, $stateParams, JobsService, $rootScope) ->
  console.log 'SingleJobController'

  $scope.jobid = $stateParams.jobid
  $rootScope.job = null
  $rootScope.plan = null

  JobsService.loadJob($stateParams.jobid).then (data) ->
    $rootScope.job = data
    $rootScope.plan = data.plan

  $scope.$on '$destroy', ->
    $rootScope.job = null
    $rootScope.plan = null

# --------------------------------------

.controller 'JobPlanController', ($scope, $state, $stateParams, JobsService) ->
  console.log 'JobPlanController'

  $scope.nodeid = null
  $scope.stateList = JobsService.stateList()

  $scope.changeNode = (nodeid) ->
    if nodeid != $scope.nodeid
      $scope.nodeid = nodeid
      $scope.vertex = null

      if $state.is('single-job.plan.overview')
        JobsService.getSubtasks(nodeid).then (data) ->
          $scope.vertex = data

      else if $state.is('single-job.plan.accumulators')
        JobsService.getAccumulators(nodeid).then (data) ->
          $scope.vertex = data

    else
      $scope.nodeid = null
      $scope.vertex = null

# --------------------------------------

.controller 'JobPlanOverviewController', ($scope, JobsService) ->
  console.log 'JobPlanOverviewController'

  if $scope.nodeid and !$scope.vertex.st
    JobsService.getSubtasks($scope.nodeid).then (data) ->
      $scope.vertex = data

# --------------------------------------

.controller 'JobPlanAccumulatorsController', ($scope, JobsService) ->
  console.log 'JobPlanAccumulatorsController'

  if $scope.nodeid and !$scope.vertex.accumulators
    JobsService.getAccumulators($scope.nodeid).then (data) ->
      $scope.vertex = data

# --------------------------------------

.controller 'JobTimelineVertexController', ($scope, $state, $stateParams, JobsService) ->
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
