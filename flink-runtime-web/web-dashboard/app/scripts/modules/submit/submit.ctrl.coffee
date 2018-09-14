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

.controller 'JobSubmitController', ($scope, JobSubmitService, $interval, flinkConfig, $state, $location) ->
  $scope.yarn = $location.absUrl().indexOf("/proxy/application_") != -1
  $scope.loadList = () ->
    JobSubmitService.loadJarList().then (data) ->
      $scope.address = data.address
      if data.error?
        $scope.noaccess = data.error
      else if data.errors?
        $scope.noaccess = data.errors[0]
      $scope.jars = data.files

  $scope.defaultState = () ->
    $scope.plan = null
    $scope.error = null
    $scope.state = {
      selected: null,
      parallelism: "",
      savepointPath: "",
      allowNonRestoredState: false
      'entry-class': "",
      'program-args': "",
      'plan-button': "Show Plan",
      'submit-button': "Submit",
      'action-time': 0
    }

  $scope.defaultState()
  $scope.uploader = {}
  $scope.loadList()

  refresh = $interval ->
    $scope.loadList()
  , flinkConfig["refresh-interval"]

  $scope.$on '$destroy', ->
    $interval.cancel(refresh)

  $scope.selectJar = (id) ->
    if $scope.state.selected == id
      $scope.defaultState()
    else
      $scope.defaultState()
      $scope.state.selected = id

  $scope.deleteJar = (event, id) ->
    if $scope.state.selected == id
      $scope.defaultState()
    angular.element(event.currentTarget).removeClass("fa-remove").addClass("fa-spin fa-spinner")
    JobSubmitService.deleteJar(id).then (data) ->
      angular.element(event.currentTarget).removeClass("fa-spin fa-spinner").addClass("fa-remove")
      if data.error?
        alert(data.error)
      else if data.errors?
        alert(data.errors[0])

  $scope.loadEntryClass = (name) ->
    $scope.state['entry-class'] = name

  $scope.getPlan = () ->
    if $scope.state['plan-button'] == "Show Plan"
      action = new Date().getTime()
      $scope.state['action-time'] = action
      $scope.state['submit-button'] = "Submit"
      $scope.state['plan-button'] = "Getting Plan"
      $scope.error = null
      $scope.plan = null

      queryParameters = {}

      if $scope.state['entry-class']
        queryParameters['entry-class'] = $scope.state['entry-class']

      if $scope.state.parallelism
        queryParameters['parallelism'] = $scope.state['parallelism']

      if $scope.state['program-args']
        queryParameters['program-args'] = $scope.state['program-args']

      JobSubmitService.getPlan(
        $scope.state.selected, queryParameters
      ).then (data) ->
        if action == $scope.state['action-time']
          $scope.state['plan-button'] = "Show Plan"
          if data.error?
            $scope.error = data.error
          else if data.errors?
            $scope.error = data.errors[0]
          $scope.plan = data.plan
      .catch (err) ->
        $scope.state['plan-button'] = "Show Plan"
        $scope.error = err

  $scope.runJob = () ->
    if $scope.state['submit-button'] == "Submit"
      action = new Date().getTime()
      $scope.state['action-time'] = action
      $scope.state['submit-button'] = "Submitting"
      $scope.state['plan-button'] = "Show Plan"
      $scope.error = null

      request = {}
      # legacy compatibility
      queryParameters = {}

      if $scope.state['entry-class']
        request['entryClass'] = $scope.state['entry-class']
        queryParameters['entry-class'] = $scope.state['entry-class']

      if $scope.state.parallelism
        request['parallelism'] = $scope.state['parallelism']
        queryParameters['parallelism'] = $scope.state['parallelism']

      if $scope.state['program-args']
        request['programArgs'] = $scope.state['program-args']
        queryParameters['program-args'] = $scope.state['program-args']

      if $scope.state['savepointPath']
        request['savepointPath'] = $scope.state['savepointPath']
        queryParameters['savepointPath'] = $scope.state['savepointPath']

      if $scope.state['allowNonRestoredState']
        request['allowNonRestoredState'] = $scope.state['allowNonRestoredState']
        queryParameters['allowNonRestoredState'] = $scope.state['allowNonRestoredState']

      JobSubmitService.runJob(
        $scope.state.selected, request, queryParameters
      ).then (data) ->
        if action == $scope.state['action-time']
          $scope.state['submit-button'] = "Submit"
          if data.error?
            $scope.error = data.error
          else if data.errors?
            $scope.error = data.errors[0]
          if data.jobid?
            $state.go("single-job.plan.subtasks", {jobid: data.jobid})
      .catch (err) ->
        $scope.state['submit-button'] = "Submit"
        $scope.error = err

  # job plan display related stuff
  $scope.nodeid = null
  $scope.changeNode = (nodeid) ->
    if nodeid != $scope.nodeid
      $scope.nodeid = nodeid
      $scope.vertex = null
      $scope.subtasks = null
      $scope.accumulators = null

      $scope.$broadcast 'reload'

    else
      $scope.nodeid = null
      $scope.nodeUnfolded = false
      $scope.vertex = null
      $scope.subtasks = null
      $scope.accumulators = null

  $scope.clearFiles = () ->
    $scope.uploader = {}

  $scope.uploadFiles = (files) ->
    # make sure everything is clear again.
    $scope.uploader = {}
    if files.length == 1
      $scope.uploader['file'] = files[0]
      $scope.uploader['upload'] = true
    else
      $scope.uploader['error'] = "Did ya forget to select a file?"

  $scope.startUpload = () ->
    if $scope.uploader['file']?
      formdata = new FormData()
      formdata.append("jarfile", $scope.uploader['file'])
      $scope.uploader['upload'] = false
      $scope.uploader['success'] = "Initializing upload..."
      xhr = new XMLHttpRequest()
      xhr.upload.onprogress = (event) ->
        $scope.uploader['success'] = null
        $scope.uploader['progress'] = parseInt(100 * event.loaded / event.total)
      xhr.upload.onerror = (event) ->
        $scope.uploader['progress'] = null
        $scope.uploader['error'] = "An error occurred while uploading your file"
      xhr.upload.onload = (event) ->
        $scope.uploader['progress'] = null
        $scope.uploader['success'] = "Saving..."
      xhr.onreadystatechange = () ->
        if xhr.readyState == 4
          response = JSON.parse(xhr.responseText)
          if response.error?
            $scope.uploader['error'] = response.error
            $scope.uploader['success'] = null
          else if response.errors?
            $scope.uploader['error'] = response.errors[0]
            $scope.uploader['success'] = null
          else
            $scope.uploader['success'] = "Uploaded!"
      xhr.open("POST", flinkConfig.jobServer + "jars/upload")
      xhr.send(formdata)
    else
      console.log("Unexpected Error. This should not happen")

.filter 'getJarSelectClass', ->
  (selected, actual) ->
    if selected == actual
      "fa-check-square"
    else
      "fa-square-o"
