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

.controller 'SingleJobController', ($scope, $state, $stateParams, JobsService, MetricsService, $rootScope, flinkConfig, $interval, $q, watermarksConfig) ->
  $scope.jobid = $stateParams.jobid
  $scope.job = null
  $scope.plan = null
  $scope.watermarks = {}
  $scope.vertices = null
  $scope.backPressureOperatorStats = {}

  refresher = $interval ->
    JobsService.loadJob($stateParams.jobid).then (data) ->
      $scope.job = data
      $scope.$broadcast 'reload'

  , flinkConfig["refresh-interval"]

  $scope.$on '$destroy', ->
    $scope.job = null
    $scope.plan = null
    $scope.watermarks = {}
    $scope.vertices = null
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

  JobsService.loadJob($stateParams.jobid).then (data) ->
    $scope.job = data
    $scope.vertices = data.vertices
    $scope.plan = data.plan
    MetricsService.setupMetrics($stateParams.jobid, data.vertices)

  # Asynchronously requests the watermark metrics for the given nodes. The
  # returned object has the following structure:
  #
  # {
  #    "<nodeId>": {
  #          "lowWatermark": <lowWatermark>
  #          "watermarks": {
  #               0: <watermark for subtask 0>
  #               ...
  #               n: <watermark for subtask n>
  #            }
  #       }
  # }
  #
  # If no watermark is available, lowWatermark will be NaN and
  # the watermarks will be empty.
  getWatermarks = (nodes) ->
    # Requests the watermarks for a single vertex. Triggers a request
    # to the Metrics service.
    requestWatermarkForNode = (node) =>
      deferred = $q.defer()

      jid = $scope.job.jid

      # Request metrics for each subtask
      metricIds = (i + ".currentInputWatermark" for i in [0..node.parallelism - 1])
      MetricsService.getMetrics(jid, node.id, metricIds).then (metrics) ->
        minValue = NaN
        watermarks = {}

        for key, value of metrics.values
          subtaskIndex = key.replace('.currentInputWatermark', '')
          watermarks[subtaskIndex] = value

          if (isNaN(minValue) || value < minValue)
            minValue = value

        if (!isNaN(minValue) && minValue > watermarksConfig.noWatermark)
          lowWatermark = minValue
        else
          # NaN indicates no watermark available
          lowWatermark = NaN

        deferred.resolve({"lowWatermark": lowWatermark, "watermarks": watermarks})

      deferred.promise

    deferred = $q.defer()
    watermarks = {}

    # Request watermarks for each node and update watermarks
    len = nodes.length
    angular.forEach nodes, (node, index) =>
      nodeId = node.id
      requestWatermarkForNode(node).then (data) ->
        watermarks[nodeId] = data
        if (index >= len - 1)
          deferred.resolve(watermarks)

    deferred.promise

  # Returns true if the lowWatermark is != NaN
  $scope.hasWatermark = (nodeid) ->
    $scope.watermarks[nodeid] && !isNaN($scope.watermarks[nodeid]["lowWatermark"])

  $scope.$watch 'plan', (newPlan) ->
    if newPlan
      getWatermarks(newPlan.nodes).then (data) ->
        $scope.watermarks = data

  $scope.$on 'reload', () ->
    if $scope.plan
      getWatermarks($scope.plan.nodes).then (data) ->
        $scope.watermarks = data

# --------------------------------------

.controller 'JobPlanController', ($scope, $state, $stateParams, $window, JobsService) ->
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
      $scope.$broadcast 'node:change', $scope.nodeid

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
  $scope.aggregate = false

  getSubtasks = ->
    if $scope.aggregate
      JobsService.getTaskManagers($scope.nodeid).then (data) ->
        $scope.taskmanagers = data
    else
      JobsService.getSubtasks($scope.nodeid).then (data) ->
        $scope.subtasks = data

  if $scope.nodeid and (!$scope.vertex or !$scope.vertex.st)
    getSubtasks()

  $scope.$on 'reload', (event) ->
    getSubtasks() if $scope.nodeid

# --------------------------------------

.controller 'JobPlanAccumulatorsController', ($scope, JobsService) ->
  getAccumulators = ->
    JobsService.getAccumulators($scope.nodeid).then (data) ->
      $scope.accumulators = data.main
      $scope.subtaskAccumulators = data.subtasks

  if $scope.nodeid and (!$scope.vertex or !$scope.vertex.accumulators)
    getAccumulators()

  $scope.$on 'reload', (event) ->
    getAccumulators() if $scope.nodeid

# --------------------------------------

.controller 'JobPlanCheckpointsController', ($scope, $state, $stateParams, JobsService) ->
  # Updated by the details handler for the sub checkpoints nav bar.
  $scope.checkpointDetails = {}
  $scope.checkpointDetails.id = -1

  # Request the config once (it's static)
  JobsService.getCheckpointConfig().then (data) ->
    $scope.checkpointConfig = data

  # General stats like counts, history, etc.
  getGeneralCheckpointStats = ->
    JobsService.getCheckpointStats().then (data) ->
      if (data != null)
        $scope.checkpointStats = data

  # Trigger request
  getGeneralCheckpointStats()

  $scope.$on 'reload', (event) ->
    # Retrigger request
    getGeneralCheckpointStats()

# --------------------------------------

.controller 'JobPlanCheckpointDetailsController', ($scope, $state, $stateParams, JobsService) ->
  $scope.subtaskDetails = {}
  $scope.checkpointDetails.id = $stateParams.checkpointId

  # Detailed stats for a single checkpoint
  getCheckpointDetails = (checkpointId) ->
    JobsService.getCheckpointDetails(checkpointId).then (data) ->
      if (data != null)
        $scope.checkpoint = data
      else
        $scope.unknown_checkpoint = true

  getCheckpointSubtaskDetails = (checkpointId, vertexId) ->
    JobsService.getCheckpointSubtaskDetails(checkpointId, vertexId).then (data) ->
      if (data != null)
        $scope.subtaskDetails[vertexId] = data

  getCheckpointDetails($stateParams.checkpointId)

  if ($scope.nodeid)
    getCheckpointSubtaskDetails($stateParams.checkpointId, $scope.nodeid)

  $scope.$on 'reload', (event) ->
    getCheckpointDetails($stateParams.checkpointId)

    if ($scope.nodeid)
      getCheckpointSubtaskDetails($stateParams.checkpointId, $scope.nodeid)

  $scope.$on '$destroy', ->
    $scope.checkpointDetails.id = -1

# --------------------------------------

.controller 'JobPlanBackPressureController', ($scope, JobsService) ->
  getOperatorBackPressure = ->
    $scope.now = Date.now()

    if $scope.nodeid
      JobsService.getOperatorBackPressure($scope.nodeid).then (data) ->
        $scope.backPressureOperatorStats[$scope.nodeid] = data

  getOperatorBackPressure()

  $scope.$on 'reload', (event) ->
    getOperatorBackPressure()

# --------------------------------------

.controller 'JobTimelineVertexController', ($scope, $state, $stateParams, JobsService) ->
  getVertex = ->
    JobsService.getVertex($stateParams.vertexId).then (data) ->
      $scope.vertex = data

  getVertex()

  $scope.$on 'reload', (event) ->
    getVertex()

# --------------------------------------

.controller 'JobExceptionsController', ($scope, $state, $stateParams, JobsService) ->
  JobsService.loadExceptions().then (data) ->
    $scope.exceptions = data

# --------------------------------------

.controller 'JobPropertiesController', ($scope, JobsService) ->
  $scope.changeNode = (nodeid) ->
    if nodeid != $scope.nodeid
      $scope.nodeid = nodeid

      JobsService.getNode(nodeid).then (data) ->
        $scope.node = data

    else
      $scope.nodeid = null
      $scope.node = null

# --------------------------------------

.controller 'JobPlanMetricsController', ($scope, JobsService, MetricsService) ->
  $scope.dragging = false
  $scope.window = MetricsService.getWindow()
  $scope.availableMetrics = null

  $scope.$on '$destroy', ->
    MetricsService.unRegisterObserver()

  loadMetrics = ->
    JobsService.getVertex($scope.nodeid).then (data) ->
      $scope.vertex = data

    MetricsService.getAvailableMetrics($scope.jobid, $scope.nodeid).then (data) ->
      $scope.availableMetrics = data.sort(alphabeticalSortById)
      $scope.metrics = MetricsService.getMetricsSetup($scope.jobid, $scope.nodeid).names

      MetricsService.registerObserver($scope.jobid, $scope.nodeid, (data) ->
        $scope.$broadcast "metrics:data:update", data.timestamp, data.values
      )

  alphabeticalSortById = (a, b) ->
    A = a.id.toLowerCase()
    B = b.id.toLowerCase()
    if A < B
      return -1
    else if A > B
      return 1
    else
      return 0

  $scope.dropped = (event, index, item, external, type) ->

    MetricsService.orderMetrics($scope.jobid, $scope.nodeid, item, index)
    $scope.$broadcast "metrics:refresh", item
    loadMetrics()
    false

  $scope.dragStart = ->
    $scope.dragging = true

  $scope.dragEnd = ->
    $scope.dragging = false

  $scope.addMetric = (metric) ->
    MetricsService.addMetric($scope.jobid, $scope.nodeid, metric.id)
    loadMetrics()

  $scope.removeMetric = (metric) ->
    MetricsService.removeMetric($scope.jobid, $scope.nodeid, metric)
    loadMetrics()

  $scope.setMetricSize = (metric, size) ->
    MetricsService.setMetricSize($scope.jobid, $scope.nodeid, metric, size)
    loadMetrics()

  $scope.setMetricView = (metric, view) ->
    MetricsService.setMetricView($scope.jobid, $scope.nodeid, metric, view)
    loadMetrics()

  $scope.getValues = (metric) ->
    MetricsService.getValues($scope.jobid, $scope.nodeid, metric)

  $scope.$on 'node:change', (event, nodeid) ->
    loadMetrics() if !$scope.dragging

  loadMetrics() if $scope.nodeid

# --------------------------------------
