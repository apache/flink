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

  @setEndTimes = (list) ->
    angular.forEach list, (job, jobKey) ->
      unless job['end-time'] > -1
        job['end-time'] = job['start-time'] + job['duration']

  @listJobs = ->
    deferred = $q.defer()

    $http.get flinkConfig.jobServer + "/joboverview"
    .success (data, status, headers, config) =>
      angular.forEach data, (list, listKey) =>
        switch listKey
          when 'running' then jobs.running = @setEndTimes(list)
          when 'finished' then jobs.finished = @setEndTimes(list)
          when 'cancelled' then jobs.cancelled = @setEndTimes(list)
          when 'failed' then jobs.failed = @setEndTimes(list)

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

    $http.get flinkConfig.jobServer + "/jobs/" + jobid
    .success (data, status, headers, config) =>
      # console.log data.vertices
      @setEndTimes(data.vertices)
      # console.log data.vertices

      # $http.get flinkConfig.jobServer + "/jobs/" + jobid + "/vertices"
      # .success (vertices) ->
      #   data = angular.extend(data, vertices)

        # $http.get flinkConfig.jobServer + "/jobsInfo?get=job&job=" + jobid
        # .success (oldVertices) ->
        #   data.oldV = oldVertices[0]

      $http.get flinkConfig.jobServer + "/jobs/" + jobid + "/config"
      .success (jobConfig) ->
        data = angular.extend(data, jobConfig)

        currentJob = data

        deferreds.job.resolve(data)

    deferreds.job.promise

  @loadPlan = (jobid) ->
    currentPlan = null
    deferreds.plan = $q.defer()

    $http.get flinkConfig.jobServer + "/jobs/" + jobid + "/plan"
    .success (data) ->
      currentPlan = data

      deferreds.plan.resolve(data)

    deferreds.plan.promise

  @getNode = (nodeid) ->
    seekNode = (nodeid, data) ->
      for node in data
        return node if node.id is nodeid
        sub = seekNode(nodeid, node.step_function) if node.step_function
        return sub if sub

      null


    deferred = $q.defer()

    # $q.all([deferreds.plan.promise, deferreds.job.promise]).then (data) =>
    $q.all([deferreds.job.promise]).then (data) =>
      # foundNode = seekNode(nodeid, currentPlan.nodes)
      foundNode = seekNode(nodeid, currentJob.plan.nodes)

      foundNode.vertex = @seekVertex(nodeid)

      deferred.resolve(foundNode)

    deferred.promise

  @seekVertex = (nodeid) ->
    for vertex in currentJob.vertices
      return vertex if vertex.id is nodeid

    return null

  @getVertex = (vertexid) ->
    deferred = $q.defer()

    $q.all([deferreds.job.promise]).then (data) =>
      vertex = @seekVertex(vertexid)

      $http.get flinkConfig.jobServer + "/jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasktimes"
      .success (data) ->
        # TODO: change to subtasktimes
        vertex.subtasks = data.subtasks

        deferred.resolve(vertex)

    deferred.promise

  @getSubtasks = (vertexid) ->
    deferred = $q.defer()

    $q.all([deferreds.job.promise]).then (data) =>
      vertex = @seekVertex(vertexid)

      $http.get flinkConfig.jobServer + "/jobs/" + currentJob.jid + "/vertices/" + vertexid
      .success (data) ->
        vertex.st = data.subtasks

        deferred.resolve(vertex)

    deferred.promise


  @loadExceptions = ->
    deferred = $q.defer()

    $q.all([deferreds.job.promise]).then (data) =>

      $http.get flinkConfig.jobServer + "/jobs/" + currentJob.jid + "/exceptions"
      .success (exceptions) ->
        currentJob.exceptions = exceptions

        deferred.resolve(exceptions)

    deferred.promise

  @
