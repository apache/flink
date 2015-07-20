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

angular.module('flinkApp', ['ui.router', 'angularMoment'])

# --------------------------------------

.run ($rootScope) ->
  $rootScope.sidebarVisible = false
  $rootScope.showSidebar = ->
    $rootScope.sidebarVisible = !$rootScope.sidebarVisible
    $rootScope.sidebarClass = 'force-show'

# --------------------------------------

.constant 'flinkConfig', {
  webServer: 'http://localhost:8080'
  jobServer: 'http://localhost:8081'
  newServer: 'http://localhost:8082'
#  webServer: 'http://localhost:3000/web-server'
#  jobServer: 'http://localhost:3000/job-server'
#  newServer: 'http://localhost:3000/new-server'
  refreshInterval: 10000
}

# --------------------------------------

.run (JobsService, flinkConfig, $interval) ->
  JobsService.listJobs()

  $interval ->
    JobsService.listJobs()
  , flinkConfig.refreshInterval


# --------------------------------------

.config ($stateProvider, $urlRouterProvider) ->
  $stateProvider.state "overview",
    url: "/overview"
    views:
      main:
        templateUrl: "partials/overview.html"
        controller: 'OverviewController'

  .state "running-jobs",
    url: "/running-jobs"
    views:
      main:
        templateUrl: "partials/jobs/running-jobs.html"
        controller: 'RunningJobsController'
  
  .state "completed-jobs",
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
    views:
      details:
        templateUrl: "partials/jobs/job.plan.html"
        controller: 'JobPlanController'

  .state "single-job.plan.node",
    url: "/{nodeid:int}"
    views:
      node:
        templateUrl: "partials/jobs/job.plan.node.html"
        controller: 'JobPlanNodeController'

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

  .state "single-job.statistics",
    url: "/statistics"
    views:
      details:
        templateUrl: "partials/jobs/job.statistics.html"

  .state "single-job.exceptions",
    url: "/exceptions"
    views:
      details:
        templateUrl: "partials/jobs/job.exceptions.html"

  $urlRouterProvider.otherwise "/overview"
