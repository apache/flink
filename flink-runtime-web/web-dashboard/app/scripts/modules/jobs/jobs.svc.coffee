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

.service 'JobsService', ($http, flinkConfig, $log, amMoment, $q, $timeout) ->
  currentJob = null
  currentPlan = null
  deferreds = {}
  jobs = {
    running: []
    finished: []
    cancelled: []
    failed: []
  }

  jobObservers = []

  notifyObservers = ->
    angular.forEach jobObservers, (callback) ->
      callback()

  @registerObserver = (callback) ->
    jobObservers.push(callback)

  @unRegisterObserver = (callback) ->
    index = jobObservers.indexOf(callback)
    jobObservers.splice(index, 1)

  @stateList = ->
    [ 
      # 'CREATED'
      'SCHEDULED'
      'DEPLOYING'
      'RUNNING'
      'FINISHED'
      'FAILED'
      'CANCELING'
      'CANCELED'
    ]

  @translateLabelState = (state) ->
    switch state.toLowerCase()
      when 'finished' then 'success'
      when 'failed' then 'danger'
      when 'scheduled' then 'default'
      when 'deploying' then 'info'
      when 'running' then 'primary'
      when 'canceling' then 'warning'
      when 'pending' then 'info'
      when 'total' then 'black'
      else 'default'

  @listJobs = ->
    deferred = $q.defer()

    $http.get flinkConfig.newServer + "/jobs"
    .success (data, status, headers, config) ->

      angular.forEach data, (list, listKey) ->

        switch listKey
          when 'jobs-running' then jobs.running = list
          when 'jobs-finished' then jobs.finished = list
          when 'jobs-cancelled' then jobs.cancelled = list
          when 'jobs-failed' then jobs.failed = list

        angular.forEach list, (jobid, index) ->
          $http.get flinkConfig.newServer + "/jobs/" + jobid
          .success (details) ->
            list[index] = details

      deferred.resolve(jobs)
      notifyObservers()

    deferred.promise

  @getJobs = (type) ->
    jobs[type]

  @getAllJobs = ->
    jobs

  @loadJob = (jobid) ->
    currentJob = null
    deferreds.job = $q.defer()

    $http.get flinkConfig.newServer + "/jobs/" + jobid
    .success (data, status, headers, config) ->
      data.time = Date.now()

      $http.get flinkConfig.newServer + "/jobs/" + jobid + "/vertices"
      .success (vertices) ->
        data = angular.extend(data, vertices)

        $http.get flinkConfig.jobServer + "/jobsInfo?get=job&job=" + jobid
        .success (oldVertices) ->
          data.oldV = oldVertices[0]

          currentJob = data
          deferreds.job.resolve(data)

    deferreds.job.promise

  @loadPlan = (jobid) ->
    currentPlan = null
    deferreds.plan = $q.defer()

    $http.get flinkConfig.newServer + "/jobs/" + jobid + "/plan"
    .success (data) ->
      currentPlan = data

      deferreds.plan.resolve(data)

    deferreds.plan.promise

  @getNode = (nodeid) ->
    seekNode = (nodeid, data) ->
      nodeid = parseInt(nodeid)

      for node in data
        return node if node.id is nodeid
        sub = seekNode(nodeid, node.step_function) if node.step_function
        return sub if sub

      null

    deferred = $q.defer()

    # if currentPlan
    #   deferred.resolve(seekNode(nodeid, currentPlan.nodes))
    # else
    #   # deferreds.plan.promise.then (data) ->
    #   $q.all([deferreds.plan.promise, deferreds.job.promise]).then (data) ->
    #     console.log 'resolving getNode'
    #     deferred.resolve(seekNode(nodeid, currentPlan.nodes))

    $q.all([deferreds.plan.promise, deferreds.job.promise]).then (data) =>
      foundNode = seekNode(nodeid, currentPlan.nodes)

      # TODO link to real vertex. for now there is no way to get the right one, so we are showing the first one - just for testing
      @getVertex(currentJob.jid, currentJob.oldV.groupvertices[0].groupvertexid).then (vertex) ->
        foundNode.vertex = vertex
        deferred.resolve(foundNode)

    deferred.promise


  @getVertex = (jobId, vertexId) ->
    deferred = $q.defer()

    $http.get flinkConfig.jobServer + "/jobsInfo?get=groupvertex&job=" + jobId + "&groupvertex=" + vertexId
    .success (data) ->
      deferred.resolve(data)

    deferred.promise

  @
