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
  $scope.watermarks = null
  $scope.lowWatermarks = null
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
    $scope.watermarks = null
    $scope.lowWatermarks = null
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

  getWatermarks = (nodes)->
    # This function uses a promise to resolve watermarks once fetched via the metrics service, since watermarks have to be fetched individually for each node, we have to wait until all API calls have been made before we can resolve the promise. In the end we will have an array of low watermarks for each node: e.g. {somenodeid: [{id: 0, value: -9223372036854776000}], anothernodeid: [{id: 0, value: -9223372036854776000}, {id: 1, value: -9223372036854776000}]}.
    deferred = $q.defer()
    watermarks = {}
    jid = $scope.job.jid
    angular.forEach nodes, (node, index) =>
      metricIds = []
      # for each node, we need to specify which metrics we want to collect, for each subtask, we need to fetch the currentLowWatermark, and each param is formed by concatenating subtask index to '.currentLowWatermark'.
      for num in [0..node.parallelism - 1]
        metricIds.push(num + ".currentLowWatermark")
      MetricsService.getMetrics(jid, node.id, metricIds).then (data) ->
        values = []
        for key, value of data.values
          values.push(id: key.replace('.currentLowWatermark', ''), value: value)
        watermarks[node.id] = values
        if index >= $scope.plan.nodes.length - 1
          deferred.resolve(watermarks)
    deferred.promise

  getLowWatermarks = (watermarks)->
    lowWatermarks = []
    for k,v of watermarks
      minValue = Math.min.apply(null,(watermark.value for watermark in v))
      lowWatermarks[k] = if minValue <= watermarksConfig.minValue || v.length == 0 then 'No Watermark' else minValue
    return lowWatermarks

  $scope.$watch 'plan', (newPlan) ->
    if newPlan
      getWatermarks(newPlan.nodes).then (data) ->
        $scope.watermarks = data
        $scope.lowWatermarks = getLowWatermarks(data)

  $scope.$on 'reload', (event) ->
    if $scope.plan
      getWatermarks($scope.plan.nodes).then (data) ->
        $scope.watermarks = data
        $scope.lowWatermarks = getLowWatermarks(data)

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
  getSubtasks = ->
    JobsService.getSubtasks($scope.nodeid).then (data) ->
      $scope.subtasks = data

  if $scope.nodeid and (!$scope.vertex or !$scope.vertex.st)
    getSubtasks()

  $scope.$on 'reload', (event) ->
    getSubtasks() if $scope.nodeid

# --------------------------------------

.controller 'JobPlanTaskManagersController', ($scope, JobsService) ->
  getTaskManagers = ->
    JobsService.getTaskManagers($scope.nodeid).then (data) ->
      $scope.taskmanagers = data

  if $scope.nodeid and (!$scope.vertex or !$scope.vertex.st)
    getTaskManagers()

  $scope.$on 'reload', (event) ->
    getTaskManagers() if $scope.nodeid

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
      $scope.availableMetrics = data
      $scope.metrics = MetricsService.getMetricsSetup($scope.jobid, $scope.nodeid).names

      MetricsService.registerObserver($scope.jobid, $scope.nodeid, (data) ->
        $scope.$broadcast "metrics:data:update", data.timestamp, data.values
      )

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

  $scope.getValues = (metric) ->
    MetricsService.getValues($scope.jobid, $scope.nodeid, metric)

  $scope.$on 'node:change', (event, nodeid) ->
    loadMetrics() if !$scope.dragging

  loadMetrics() if $scope.nodeid

# --------------------------------------

.controller 'JobPlanWatermarksController', ($scope, $filter) ->
  $scope.hasWatermarks = (nodeid) ->
    return true if $scope.watermarksByNode(nodeid).length

  $scope.watermarksByNode = (nodeid) ->
    if $scope.watermarks != null && $scope.watermarks[nodeid] && $scope.watermarks[nodeid].length
      return $scope.watermarks[nodeid]
    return []

# --------------------------------------
