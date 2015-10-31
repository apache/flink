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

.value 'flinkConfig', {
  "refresh-interval": 10000
}

# --------------------------------------

.run (JobsService, MainService, flinkConfig, $interval) ->
  MainService.loadConfig().then (config) ->
    angular.extend flinkConfig, config

    JobsService.listJobs()

    $interval ->
      JobsService.listJobs()
    , flinkConfig["refresh-interval"]

  Highcharts.setOptions({
    global: {
      useUTC: false
    }
  })

  #
  # Grid-light theme for Highcharts JS
  # @author Torstein Honsi
  #
  # Taken from https://github.com/highslide-software/highcharts.com
  #


  Highcharts.createElement('link', {
  	href: '//fonts.googleapis.com/css?family=Dosis:400,600',
  	rel: 'stylesheet',
  	type: 'text/css'
  }, null, document.getElementsByTagName('head')[0]);

  Highcharts.theme = {
  	colors: ["#7cb5ec", "#f7a35c", "#90ee7e", "#7798BF", "#aaeeee", "#ff0066", "#eeaaee",
  		"#55BF3B", "#DF5353", "#7798BF", "#aaeeee"],
  	chart: {
  		backgroundColor: null,
  		style: {
  			fontFamily: "Dosis, sans-serif"
  		}
  	},
  	title: {
  		style: {
  			fontSize: '16px',
  			fontWeight: 'bold',
  			textTransform: 'uppercase'
  		}
  	},
  	tooltip: {
  		borderWidth: 0,
  		backgroundColor: 'rgba(219,219,216,0.8)',
  		shadow: false
  	},
  	legend: {
  		itemStyle: {
  			fontWeight: 'bold',
  			fontSize: '13px'
  		}
  	},
  	xAxis: {
  		gridLineWidth: 1,
  		labels: {
  			style: {
  				fontSize: '12px'
  			}
  		}
  	},
  	yAxis: {
  		minorTickInterval: 'auto',
  		title: {
  			style: {
  				textTransform: 'uppercase'
  			}
  		},
  		labels: {
  			style: {
  				fontSize: '12px'
  			}
  		}
  	},
  	plotOptions: {
  		candlestick: {
  			lineColor: '#404048'
  		}
  	},

  	background2: '#F0F0EA'

  };

  Highcharts.setOptions(Highcharts.theme);


# --------------------------------------

.config ($uiViewScrollProvider) ->
  $uiViewScrollProvider.useAnchorScroll()

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
    abstract: true
    views:
      details:
        templateUrl: "partials/jobs/job.plan.html"
        controller: 'JobPlanController'

  .state "single-job.plan.overview",
    url: ""
    views:
      'node-details':
        templateUrl: "partials/jobs/job.plan.node-list.overview.html"
        controller: 'JobPlanOverviewController' 

  .state "single-job.plan.accumulators",
    url: "/accumulators"
    views:
      'node-details':
        templateUrl: "partials/jobs/job.plan.node-list.accumulators.html"
        controller: 'JobPlanAccumulatorsController' 

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

  .state "single-job.properties",
    url: "/properties"
    views:
      details:
        templateUrl: "partials/jobs/job.properties.html"
        controller: 'JobPropertiesController'

  .state "single-job.config",
    url: "/config"
    views:
      details:
        templateUrl: "partials/jobs/job.config.html"

  .state "all-manager",
    url: "/taskmanagers"
    views:
      main:
        templateUrl: "partials/taskmanager/index.html"
        controller: 'AllTaskManagersController'

  .state "single-manager",
      url: "/taskmanager/{taskmanagerid}"
      views:
        main:
          templateUrl: "partials/taskmanager/taskmanager.html"
          controller: 'SingleTaskManagerController'

  .state "single-manager.metrics",
    url: "/metrics"
    views:
      details:
        templateUrl: "partials/taskmanager/taskmanager.metrics.html"

  .state "jobmanager",
      url: "/jobmanager"
      views:
        main:
          templateUrl: "partials/jobmanager/index.html"

  .state "jobmanager.config",
    url: "/config"
    views:
      details:
        templateUrl: "partials/jobmanager/config.html"
        controller: 'JobManagerConfigController'

  .state "jobmanager.stdout",
    url: "/stdout"
    views:
      details:
        templateUrl: "partials/jobmanager/stdout.html"
        controller: 'JobManagerStdoutController'

  .state "jobmanager.log",
    url: "/log"
    views:
      details:
        templateUrl: "partials/jobmanager/log.html"
        controller: 'JobManagerLogsController'

  $urlRouterProvider.otherwise "/overview"

