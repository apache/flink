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

angular.module('flinkApp', ['ui.router', 'angularMoment', 'dndLists'])

# --------------------------------------

.run ($rootScope) ->
  $rootScope.sidebarVisible = false
  $rootScope.showSidebar = ->
    $rootScope.sidebarVisible = !$rootScope.sidebarVisible
    $rootScope.sidebarClass = 'force-show'

# --------------------------------------

.value 'flinkConfig', {
  jobServer: ''
#  jobServer: 'http://localhost:8081/'
  "refresh-interval": 10000
}

# --------------------------------------

.value 'watermarksConfig', {
  # A value of (Java) Long.MIN_VALUE indicates that there is no watermark
  # available. This is parsed by Javascript as this number. We have it as
  # a constant here to compare available watermarks against.
  noWatermark: -9223372036854776000
}

# --------------------------------------

.run (JobsService, MainService, flinkConfig, $interval) ->
  MainService.loadConfig().then (config) ->
    angular.extend flinkConfig, config

    JobsService.listJobs()

    $interval ->
      JobsService.listJobs()
    , flinkConfig["refresh-interval"]


# --------------------------------------

.config ($uiViewScrollProvider) ->
  $uiViewScrollProvider.useAnchorScroll()

# --------------------------------------

.run ($rootScope, $state) ->
  $rootScope.$on '$stateChangeStart', (event, toState, toParams, fromState) ->
    if toState.redirectTo
      event.preventDefault()
      $state.go toState.redirectTo, toParams

# --------------------------------------

.config ($stateProvider, $urlRouterProvider) ->
  $stateProvider.state "completed-jobs",
    url: "/completed-jobs"
    views:
      main:
        templateUrl: "partials/jobs/completed-jobs.html"
        controller: 'CompletedJobsController'

  .state "single-job",
    url: "/jobs/{jobid}"
    abstract: true
    views:
      main:
        templateUrl: "partials/jobs/job.html"
        controller: 'SingleJobController'

  .state "single-job.plan",
    url: ""
    redirectTo: "single-job.plan.subtasks"
    views:
      details:
        templateUrl: "partials/jobs/job.plan.html"
        controller: 'JobPlanController'

  .state "single-job.plan.subtasks",
    url: ""
    views:
      'node-details':
        templateUrl: "partials/jobs/job.plan.node-list.subtasks.html"
        controller: 'JobPlanSubtasksController'

  .state "single-job.plan.metrics",
    url: "/metrics"
    views:
      'node-details':
        templateUrl: "partials/jobs/job.plan.node-list.metrics.html"
        controller: 'JobPlanMetricsController'

  .state "single-job.plan.watermarks",
    url: "/watermarks"
    views:
      'node-details':
        templateUrl: "partials/jobs/job.plan.node-list.watermarks.html"

  .state "single-job.plan.taskmanagers",
    url: "/taskmanagers"
    views:
      'node-details':
        templateUrl: "partials/jobs/job.plan.node-list.taskmanagers.html"
        controller: 'JobPlanTaskManagersController'

  .state "single-job.plan.accumulators",
    url: "/accumulators"
    views:
      'node-details':
        templateUrl: "partials/jobs/job.plan.node-list.accumulators.html"
        controller: 'JobPlanAccumulatorsController'

  .state "single-job.plan.checkpoints",
      url: "/checkpoints"
      redirectTo: "single-job.plan.checkpoints.overview"
      views:
        'node-details':
          templateUrl: "partials/jobs/job.plan.node-list.checkpoints.html"
          controller: 'JobPlanCheckpointsController'
  
    .state "single-job.plan.checkpoints.overview",
      url: "/overview"
      views:
        'checkpoints-view':
          templateUrl: "partials/jobs/job.plan.node.checkpoints.overview.html"
          controller: 'JobPlanCheckpointsController'
  
    .state "single-job.plan.checkpoints.summary",
      url: "/summary"
      views:
        'checkpoints-view':
          templateUrl: "partials/jobs/job.plan.node.checkpoints.summary.html"
          controller: 'JobPlanCheckpointsController'
  
    .state "single-job.plan.checkpoints.history",
      url: "/history"
      views:
        'checkpoints-view':
          templateUrl: "partials/jobs/job.plan.node.checkpoints.history.html"
          controller: 'JobPlanCheckpointsController'
  
    .state "single-job.plan.checkpoints.config",
      url: "/config"
      views:
        'checkpoints-view':
          templateUrl: "partials/jobs/job.plan.node.checkpoints.config.html"
          controller: 'JobPlanCheckpointsController'
  
    .state "single-job.plan.checkpoints.details",
      url: "/details/{checkpointId}"
      views:
        'checkpoints-view':
          templateUrl: "partials/jobs/job.plan.node.checkpoints.details.html"
          controller: 'JobPlanCheckpointDetailsController'

  .state "single-job.plan.backpressure",
    url: "/backpressure"
    views:
      'node-details':
        templateUrl: "partials/jobs/job.plan.node-list.backpressure.html"
        controller: 'JobPlanBackPressureController'

  .state "single-job.timeline",
    url: "/timeline"
    views:
      details:
        templateUrl: "partials/jobs/job.timeline.html"

  .state "single-job.timeline.vertex",
    url: "/{vertexId}"
    views:
      vertex:
        templateUrl: "partials/jobs/job.timeline.vertex.html"
        controller: 'JobTimelineVertexController'

  .state "single-job.exceptions",
    url: "/exceptions"
    views:
      details:
        templateUrl: "partials/jobs/job.exceptions.html"
        controller: 'JobExceptionsController'

  .state "single-job.config",
    url: "/config"
    views:
      details:
        templateUrl: "partials/jobs/job.config.html"

  $urlRouterProvider.otherwise "/completed-jobs"
