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
    angular.forEach list, (item, jobKey) ->
      unless item['end-time'] > -1
        item['end-time'] = item['start-time'] + item['duration']

  @processVertices = (data) ->
    angular.forEach data.vertices, (vertex, i) ->
      vertex.type = 'regular'

    data.vertices.unshift({
      name: 'Scheduled'
      'start-time': data.timestamps['CREATED']
      'end-time': data.timestamps['CREATED'] + 1
      type: 'scheduled'
    })

  @listJobs = ->
    deferred = $q.defer()

    $http.get flinkConfig.jobServer + "joboverview"
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

    $http.get flinkConfig.jobServer + "jobs/" + jobid
    .success (data, status, headers, config) =>
      @setEndTimes(data.vertices)
      @processVertices(data)

      $http.get flinkConfig.jobServer + "jobs/" + jobid + "/config"
      .success (jobConfig) ->
        data = angular.extend(data, jobConfig)

        currentJob = data

        deferreds.job.resolve(currentJob)

    deferreds.job.promise

  @getNode = (nodeid) ->
    seekNode = (nodeid, data) ->
      for node in data
        return node if node.id is nodeid
        sub = seekNode(nodeid, node.step_function) if node.step_function
        return sub if sub

      null

    deferred = $q.defer()

    deferreds.job.promise.then (data) =>
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

    deferreds.job.promise.then (data) =>
      vertex = @seekVertex(vertexid)

      $http.get flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasktimes"
      .success (data) =>
        # TODO: change to subtasktimes
        vertex.subtasks = data.subtasks

        deferred.resolve(vertex)

    deferred.promise

  @getSubtasks = (vertexid) ->
    deferred = $q.defer()

    deferreds.job.promise.then (data) =>
      # vertex = @seekVertex(vertexid)

      $http.get flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid
      .success (data) ->
        subtasks = data.subtasks

        deferred.resolve(subtasks)

    deferred.promise

  @getAccumulators = (vertexid) ->
    deferred = $q.defer()

    deferreds.job.promise.then (data) =>
      # vertex = @seekVertex(vertexid)

      $http.get flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/accumulators"
      .success (data) ->
        accumulators = data['user-accumulators']

        $http.get flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasks/accumulators"
        .success (data) ->
          subtaskAccumulators = data.subtasks

          deferred.resolve({ main: accumulators, subtasks: subtaskAccumulators })

    deferred.promise

  @loadExceptions = ->
    deferred = $q.defer()

    deferreds.job.promise.then (data) =>

      $http.get flinkConfig.jobServer + "jobs/" + currentJob.jid + "/exceptions"
      .success (exceptions) ->
        currentJob.exceptions = exceptions

        deferred.resolve(exceptions)

    deferred.promise

  @cancelJob = (jobid) ->
    # uses the non REST-compliant GET yarn-cancel handler which is available in addition to the
    # proper "DELETE jobs/<jobid>/"
    $http.get flinkConfig.jobServer + "jobs/" + jobid + "/yarn-cancel"

  @
