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

# ----------------------------------------------

.directive 'vertex', ($state) ->
  template: "<svg class='timeline secondary' width='0' height='0'></svg>"

  scope:
    data: "="

  link: (scope, elem, attrs) ->
    svgEl = elem.children()[0]

    containerW = elem.width()
    angular.element(svgEl).attr('width', containerW)

    analyzeTime = (data) ->
      d3.select(svgEl).selectAll("*").remove()

      testData = []

      angular.forEach data.subtasks, (subtask, i) ->
        times = [
          {
            label: "Scheduled"
            color: "#666"
            borderColor: "#555"
            starting_time: subtask.timestamps["SCHEDULED"]
            ending_time: subtask.timestamps["DEPLOYING"]
            type: 'regular'
          }
          {
            label: "Deploying"
            color: "#aaa"
            borderColor: "#555"
            starting_time: subtask.timestamps["DEPLOYING"]
            ending_time: subtask.timestamps["RUNNING"]
            type: 'regular'
          }
        ]

        if subtask.timestamps["FINISHED"] > 0
          times.push {
            label: "Running"
            color: "#ddd"
            borderColor: "#555"
            starting_time: subtask.timestamps["RUNNING"]
            ending_time: subtask.timestamps["FINISHED"]
            type: 'regular'
          }

        testData.push {
          label: "(#{subtask.subtask}) #{subtask.host}"
          times: times
        }

      chart = d3.timeline().stack()
      .tickFormat({
        format: d3.time.format("%L")
        # tickInterval: 1
        tickSize: 1
      })
      .prefix("single")
      .labelFormat((label) ->
        label
      )
      .margin({ left: 100, right: 0, top: 0, bottom: 0 })
      .itemHeight(30)
      .relativeTime()

      svg = d3.select(svgEl)
      .datum(testData)
      .call(chart)

    analyzeTime(scope.data)

    return

# ----------------------------------------------

.directive 'timeline', ($state) ->
  template: "<svg class='timeline' width='0' height='0'></svg>"

  scope:
    vertices: "="
    jobid: "="

  link: (scope, elem, attrs) ->
    svgEl = elem.children()[0]

    containerW = elem.width()
    angular.element(svgEl).attr('width', containerW)

    translateLabel = (label) ->
      label.replace("&gt;", ">")

    analyzeTime = (data) ->
      d3.select(svgEl).selectAll("*").remove()

      testData = []

      angular.forEach data, (vertex) ->
        if vertex['start-time'] > -1
          if vertex.type is 'scheduled'
            testData.push
              times: [
                label: translateLabel(vertex.name)
                color: "#cccccc"
                borderColor: "#555555"
                starting_time: vertex['start-time']
                ending_time: vertex['end-time']
                type: vertex.type
              ]
          else
            testData.push
              times: [
                label: translateLabel(vertex.name)
                color: "#d9f1f7"
                borderColor: "#62cdea"
                starting_time: vertex['start-time']
                ending_time: vertex['end-time']
                link: vertex.id
                type: vertex.type
              ]

      chart = d3.timeline().stack().click((d, i, datum) ->
        if d.link
          $state.go "single-job.timeline.vertex", { jobid: scope.jobid, vertexId: d.link }

      )
      .tickFormat({
        format: d3.time.format("%L")
        # tickTime: d3.time.second
        # tickInterval: 0.5
        tickSize: 1
      })
      .prefix("main")
      .margin({ left: 0, right: 0, top: 0, bottom: 0 })
      .itemHeight(30)
      .showBorderLine()
      .showHourTimeline()

      svg = d3.select(svgEl)
      .datum(testData)
      .call(chart)

    scope.$watch attrs.vertices, (data) ->
      analyzeTime(data) if data

    return

# ----------------------------------------------

.directive 'split', () ->
  return compile: (tElem, tAttrs) ->
      Split(tElem.children(), (
        sizes: [50, 50]
        direction: 'vertical'
      ))

# ----------------------------------------------

.directive 'jobPlan', ($timeout) ->
  template: "
    <svg class='graph'><g /></svg>
    <svg class='tmp' width='1' height='1'><g /></svg>
    <div class='btn-group zoom-buttons'>
      <a class='btn btn-default zoom-in' ng-click='zoomIn()'><i class='fa fa-plus' /></a>
      <a class='btn btn-default zoom-out' ng-click='zoomOut()'><i class='fa fa-minus' /></a>
    </div>"

  scope:
    plan: '='
    watermarks: '='
    setNode: '&'

  link: (scope, elem, attrs) ->
    g = null
    mainZoom = d3.behavior.zoom()
    subgraphs = []
    jobid = attrs.jobid

    mainSvgElement = elem.children()[0]
    mainG = elem.children().children()[0]
    mainTmpElement = elem.children()[1]

    d3mainSvg = d3.select(mainSvgElement)
    d3mainSvgG = d3.select(mainG)
    d3tmpSvg = d3.select(mainTmpElement)

    # angular.element(mainG).empty()
    # d3mainSvgG.selectAll("*").remove()

    containerW = elem.width()
    angular.element(elem.children()[0]).width(containerW)

    lastZoomScale = 0
    lastPosition = 0

    scope.zoomIn = ->
      if mainZoom.scale() < 2.99

        # Calculate and store new values in zoom object
        translate = mainZoom.translate()
        v1 = translate[0] * (mainZoom.scale() + 0.1 / (mainZoom.scale()))
        v2 = translate[1] * (mainZoom.scale() + 0.1 / (mainZoom.scale()))
        mainZoom.scale mainZoom.scale() + 0.1
        mainZoom.translate [ v1, v2 ]

        # Transform svg
        d3mainSvgG.attr "transform", "translate(" + v1 + "," + v2 + ") scale(" + mainZoom.scale() + ")"

        lastZoomScale = mainZoom.scale()
        lastPosition = mainZoom.translate()

    scope.zoomOut = ->
      if mainZoom.scale() > 0.31

        # Calculate and store new values in mainZoom object
        mainZoom.scale mainZoom.scale() - 0.1
        translate = mainZoom.translate()
        v1 = translate[0] * (mainZoom.scale() - 0.1 / (mainZoom.scale()))
        v2 = translate[1] * (mainZoom.scale() - 0.1 / (mainZoom.scale()))
        mainZoom.translate [ v1, v2 ]

        # Transform svg
        d3mainSvgG.attr "transform", "translate(" + v1 + "," + v2 + ") scale(" + mainZoom.scale() + ")"

        lastZoomScale = mainZoom.scale()
        lastPosition = mainZoom.translate()

    #create a label of an edge
    createLabelEdge = (el) ->
      labelValue = ""
      if el.ship_strategy? or el.local_strategy?
        labelValue += "<div class='edge-label'>"
        labelValue += el.ship_strategy  if el.ship_strategy?
        labelValue += " (" + el.temp_mode + ")"  unless el.temp_mode is `undefined`
        labelValue += ",<br>" + el.local_strategy  unless el.local_strategy is `undefined`
        labelValue += "</div>"
      labelValue


    # true, if the node is a special node from an iteration
    isSpecialIterationNode = (info) ->
      (info is "partialSolution" or info is "nextPartialSolution" or info is "workset" or info is "nextWorkset" or info is "solutionSet" or info is "solutionDelta")

    getNodeType = (el, info) ->
      if info is "mirror"
        'node-mirror'

      else if isSpecialIterationNode(info)
        'node-iteration'

      else
        'node-normal'

    # creates the label of a node, in info is stored, whether it is a special node (like a mirror in an iteration)
    createLabelNode = (el, info, maxW, maxH) ->
      # labelValue = "<a href='#/jobs/" + jobid + "/vertex/" + el.id + "' class='node-label " + getNodeType(el, info) + "'>"
      labelValue = "<div href='#/jobs/" + jobid + "/vertex/" + el.id + "' class='node-label " + getNodeType(el, info) + "'>"

      # Nodename
      if info is "mirror"
        labelValue += "<h3 class='node-name'>Mirror of " + el.operator + "</h3>"
      else
        labelValue += "<h3 class='node-name'>" + el.operator + "</h3>"
      if el.description is ""
        labelValue += ""
      else
        stepName = el.description

        # clean stepName
        stepName = shortenString(stepName)
        labelValue += "<h4 class='step-name'>" + stepName + "</h4>"

      # If this node is an "iteration" we need a different panel-body
      if el.step_function?
        labelValue += extendLabelNodeForIteration(el.id, maxW, maxH)
      else

        # Otherwise add infos
        labelValue += "<h5>" + info + " Node</h5>"  if isSpecialIterationNode(info)
        labelValue += "<h5>Parallelism: " + el.parallelism + "</h5>"  unless el.parallelism is ""
        labelValue += "<h5>Low Watermark: " + el.lowWatermark + "</h5>"  unless el.lowWatermark is `undefined`
        labelValue += "<h5>Operation: " + shortenString(el.operator_strategy) + "</h5>" unless el.operator is `undefined` or not el.operator_strategy
      # labelValue += "</a>"
      labelValue += "</div>"
      labelValue

    # Extends the label of a node with an additional svg Element to present the iteration.
    extendLabelNodeForIteration = (id, maxW, maxH) ->
      svgID = "svg-" + id

      labelValue = "<svg class='" + svgID + "' width=" + maxW + " height=" + maxH + "><g /></svg>"
      labelValue

    # Split a string into multiple lines so that each line has less than 30 letters.
    shortenString = (s) ->
      # make sure that name does not contain a < (because of html)
      if s.charAt(0) is "<"
        s = s.replace("<", "&lt;")
        s = s.replace(">", "&gt;")
      sbr = ""
      while s.length > 30
        sbr = sbr + s.substring(0, 30) + "<br>"
        s = s.substring(30, s.length)
      sbr = sbr + s
      sbr

    createNode = (g, data, el, isParent = false, maxW, maxH) ->
      # create node, send additional informations about the node if it is a special one
      if el.id is data.partial_solution
        g.setNode el.id,
          label: createLabelNode(el, "partialSolution", maxW, maxH)
          labelType: 'html'
          class: getNodeType(el, "partialSolution")

      else if el.id is data.next_partial_solution
        g.setNode el.id,
          label: createLabelNode(el, "nextPartialSolution", maxW, maxH)
          labelType: 'html'
          class: getNodeType(el, "nextPartialSolution")

      else if el.id is data.workset
        g.setNode el.id,
          label: createLabelNode(el, "workset", maxW, maxH)
          labelType: 'html'
          class: getNodeType(el, "workset")

      else if el.id is data.next_workset
        g.setNode el.id,
          label: createLabelNode(el, "nextWorkset", maxW, maxH)
          labelType: 'html'
          class: getNodeType(el, "nextWorkset")

      else if el.id is data.solution_set
        g.setNode el.id,
          label: createLabelNode(el, "solutionSet", maxW, maxH)
          labelType: 'html'
          class: getNodeType(el, "solutionSet")

      else if el.id is data.solution_delta
        g.setNode el.id,
          label: createLabelNode(el, "solutionDelta", maxW, maxH)
          labelType: 'html'
          class: getNodeType(el, "solutionDelta")

      else
        g.setNode el.id,
          label: createLabelNode(el, "", maxW, maxH)
          labelType: 'html'
          class: getNodeType(el, "")

    createEdge = (g, data, el, existingNodes, pred) ->
      g.setEdge pred.id, el.id,
        label: createLabelEdge(pred)
        labelType: 'html'
        arrowhead: 'normal'

    loadJsonToDagre = (g, data) ->
      existingNodes = []

      if data.nodes?
        # This is the normal json data
        toIterate = data.nodes

      else
        # This is an iteration, we now store special iteration nodes if possible
        toIterate = data.step_function
        isParent = true

      for el in toIterate
        maxW = 0
        maxH = 0

        if el.step_function
          sg = new dagreD3.graphlib.Graph({ multigraph: true, compound: true }).setGraph({
            nodesep: 20
            edgesep: 0
            ranksep: 20
            rankdir: "LR"
            marginx: 10
            marginy: 10
            })

          subgraphs[el.id] = sg

          loadJsonToDagre(sg, el)

          r = new dagreD3.render()
          d3tmpSvg.select('g').call(r, sg)
          maxW = sg.graph().width
          maxH = sg.graph().height

          angular.element(mainTmpElement).empty()

        createNode(g, data, el, isParent, maxW, maxH)

        existingNodes.push el.id

        # create edges from inputs to current node
        if el.inputs?
          for pred in el.inputs
            createEdge(g, data, el, existingNodes, pred)

      g

    # searches in the global JSONData for the node with the given id
    searchForNode = (data, nodeID) ->
      for i of data.nodes
        el = data.nodes[i]
        return el  if el.id is nodeID

        # look for nodes that are in iterations
        if el.step_function?
          for j of el.step_function
            return el.step_function[j]  if el.step_function[j].id is nodeID

    mergeWatermarks = (data, watermarks) ->
      if (!_.isEmpty(watermarks))
        for node in data.nodes
          if (watermarks[node.id] && !isNaN(watermarks[node.id]["lowWatermark"]))
            node.lowWatermark = watermarks[node.id]["lowWatermark"]

      return data

    lastPosition = 0
    lastZoomScale = 0

    drawGraph = () ->
      if scope.plan
        g = new dagreD3.graphlib.Graph({ multigraph: true, compound: true }).setGraph({
          nodesep: 70
          edgesep: 0
          ranksep: 50
          rankdir: "LR"
          marginx: 40
          marginy: 40
          })

        loadJsonToDagre(g, mergeWatermarks(scope.plan, scope.watermarks))

        d3mainSvgG.selectAll("*").remove()

        d3mainSvgG.attr("transform", "scale(" + 1 + ")")

        renderer = new dagreD3.render()
        d3mainSvgG.call(renderer, g)

        for i, sg of subgraphs
          d3mainSvg.select('svg.svg-' + i + ' g').call(renderer, sg)

        newScale = 0.5

        xCenterOffset = Math.floor((angular.element(mainSvgElement).width() - g.graph().width * newScale) / 2)
        yCenterOffset = Math.floor((angular.element(mainSvgElement).height() - g.graph().height * newScale) / 2)

        if lastZoomScale != 0 && lastPosition != 0
          mainZoom.scale(lastZoomScale).translate(lastPosition)
          d3mainSvgG.attr("transform", "translate(" + lastPosition + ") scale(" + lastZoomScale + ")")
        else
          mainZoom.scale(newScale).translate([xCenterOffset, yCenterOffset])
          d3mainSvgG.attr("transform", "translate(" + xCenterOffset + ", " + yCenterOffset + ") scale(" + mainZoom.scale() + ")")

        mainZoom.on("zoom", ->
          ev = d3.event
          lastZoomScale = ev.scale
          lastPosition = ev.translate
          d3mainSvgG.attr "transform", "translate(" + lastPosition + ") scale(" + lastZoomScale + ")"
        )
        mainZoom(d3mainSvg)

        d3mainSvgG.selectAll('.node').on 'click', (d) ->
          scope.setNode({ nodeid: d })

    scope.$watch attrs.plan, (newPlan) ->
      drawGraph() if newPlan

    scope.$watch attrs.watermarks, (newWatermarks) ->
      drawGraph() if newWatermarks && scope.plan

    return
