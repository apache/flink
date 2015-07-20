angular.module('flinkApp')

# ----------------------------------------------

.directive 'vertex', ($state) ->
  template: "<svg class='timeline secondary' width='0' height='0'></svg>"

  scope:
    data: "="

  link: (scope, elem, attrs) ->
    zoom = d3.behavior.zoom()
    svgEl = elem.children()[0]

    containerW = elem.width()
    angular.element(svgEl).attr('width', containerW - 16)

    analyzeTime = (data) ->
      testData = []

      angular.forEach data.groupvertex.groupmembers, (vertex, i) ->
        vTime = data.verticetimes[vertex.vertexid]

        testData.push {
          label: "#{vertex.vertexinstancename} (#{i})"
          times: [
            {
              label: "Scheduled"
              color: "#666"
              starting_time: vTime["SCHEDULED"] * 100
              ending_time: vTime["DEPLOYING"] * 100
            }
            {
              label: "Deploying"
              color: "#aaa"
              starting_time: vTime["DEPLOYING"] * 100
              ending_time: vTime["RUNNING"] * 100
            }
            {
              label: "Running"
              color: "#ddd"
              starting_time: vTime["RUNNING"] * 100
              ending_time: vTime["FINISHED"] * 100
            }
          ]
        }

      chart = d3.timeline().stack().tickFormat({
        format: d3.time.format("%S"),
        # tickTime: d3.time.milliseconds,
        tickInterval: 1,
        tickSize: 1
      }).labelFormat((label) ->
        label
      ).margin({ left: 100, right: 0, top: 0, bottom: 0 })

      svg = d3.select(svgEl)
      .datum(testData)
      .call(chart)
      .call(zoom)

      svgG = svg.select("g")

      zoom.on("zoom", ->
        ev = d3.event

        svgG.selectAll('rect').attr("transform", "translate(" + ev.translate[0] + ",0) scale(" + ev.scale + ",1)")
        svgG.selectAll('text').attr("transform", "translate(" + ev.translate[0] + ",0) scale(" + ev.scale + ",1)")
      )

      bbox = svgG[0][0].getBBox()
      svg.attr('height', bbox.height + 30)

    analyzeTime(scope.data)

    return

# ----------------------------------------------

.directive 'timeline', ($state) ->
  template: "<svg class='timeline' width='0' height='0'></svg>"

  scope:
    job: "="

  link: (scope, elem, attrs) ->
    zoom = d3.behavior.zoom()
    svgEl = elem.children()[0]

    containerW = elem.width()
    angular.element(svgEl).attr('width', containerW - 16)

    analyzeTime = (data) ->
      testData = []

      angular.forEach data.oldV.groupvertices, (vertex) ->
        vTime = data.oldV.groupverticetimes[vertex.groupvertexid]

        # console.log vTime, vertex.groupvertexid

        testData.push 
          times: [
            label: vertex.groupvertexname
            color: "#3fb6d8"
            starting_time: vTime["STARTED"]
            ending_time: vTime["ENDED"]
            link: vertex.groupvertexid
          ]

      chart = d3.timeline().stack().click((d, i, datum) ->
        $state.go "single-job.timeline.vertex", { jobid: data.jid, vertexId: d.link }

      ).tickFormat({
        format: d3.time.format("%S")
        # tickTime: d3.time.milliseconds
        tickInterval: 1
        tickSize: 1
      }).margin({ left: 0, right: 0, top: 0, bottom: 0 })

      svg = d3.select(svgEl)
      .datum(testData)
      .call(chart)
      .call(zoom)

      svgG = svg.select("g")

      zoom.on("zoom", ->
        ev = d3.event

        svgG.selectAll('rect').attr("transform", "translate(" + ev.translate[0] + ",0) scale(" + ev.scale + ",1)")
        svgG.selectAll('text').attr("transform", "translate(" + ev.translate[0] + ",0) scale(" + ev.scale + ",1)")
      )

      bbox = svgG[0][0].getBBox()
      svg.attr('height', bbox.height + 30)

    scope.$watch attrs.job, (data) ->
      analyzeTime(data) if data

    return

# ----------------------------------------------

.directive 'jobPlan', ($timeout) ->
  template: "
    <svg class='graph' width='500' height='400'><g /></svg>
    <svg class='tmp' width='1' height='1'><g /></svg>
    <div class='btn-group zoom-buttons'>
      <a class='btn btn-default zoom-in' ng-click='zoomIn()'><i class='fa fa-plus' /></a>
      <a class='btn btn-default zoom-out' ng-click='zoomOut()'><i class='fa fa-minus' /></a>
    </div>"

  scope:
    plan: '='

  link: (scope, elem, attrs) ->
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

    containerW = elem.width()
    angular.element(elem.children()[0]).width(containerW)

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
        if el.pact is "Data Source"
          'node-source'
        else if el.pact is "Data Sink"
          'node-sink'
        else
          'node-normal'
      
    # creates the label of a node, in info is stored, whether it is a special node (like a mirror in an iteration)
    createLabelNode = (el, info, maxW, maxH) ->
      labelValue = "<a href='#/jobs/" + jobid + "/" + el.id + "' class='node-label " + getNodeType(el, info) + "'>"

      # Nodename
      if info is "mirror"
        labelValue += "<h3 class='node-name'>Mirror of " + el.pact + "</h3>"
      else
        labelValue += "<h3 class='node-name'>" + el.pact + "</h3>"
      if el.contents is ""
        labelValue += ""
      else
        stepName = el.contents
        
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
        labelValue += "<h5>Driver Strategy: " + shortenString(el.driver_strategy) + "</h5"  unless el.driver_strategy is `undefined`
      
      labelValue += "</a>"
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
      unless existingNodes.indexOf(pred.id) is -1
        g.setEdge pred.id, el.id,
          label: createLabelEdge(pred)
          labelType: 'html'
          arrowhead: 'normal'

      else
        missingNode = searchForNode(data, pred.id)
        unless !missingNode or missingNode.alreadyAdded is true
          missingNode.alreadyAdded = true
          g.setNode missingNode.id,
            label: createLabelNode(missingNode, "mirror")
            labelType: 'html'
            class: getNodeType(missingNode, 'mirror')

          g.setEdge missingNode.id, el.id,
            label: createLabelEdge(missingNode)
            labelType: 'html'

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
        
        # create edges from predecessors to current node
        if el.predecessors?
          for pred in el.predecessors
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

    drawGraph = (data) ->
      g = new dagreD3.graphlib.Graph({ multigraph: true, compound: true }).setGraph({
        nodesep: 70
        edgesep: 0
        ranksep: 50
        rankdir: "LR"
        marginx: 40
        marginy: 40
        })

      loadJsonToDagre(g, data)

      renderer = new dagreD3.render()
      d3mainSvgG.call(renderer, g)

      for i, sg of subgraphs
        d3mainSvg.select('svg.svg-' + i + ' g').call(renderer, sg)

      newScale = 0.5

      xCenterOffset = Math.floor((angular.element(mainSvgElement).width() - g.graph().width * newScale) / 2)
      yCenterOffset = Math.floor((angular.element(mainSvgElement).height() - g.graph().height * newScale) / 2)

      mainZoom.scale(newScale).translate([xCenterOffset, yCenterOffset])

      d3mainSvgG.attr("transform", "translate(" + xCenterOffset + ", " + yCenterOffset + ") scale(" + mainZoom.scale() + ")")

      mainZoom.on("zoom", ->
        ev = d3.event
        d3mainSvgG.attr "transform", "translate(" + ev.translate + ") scale(" + ev.scale + ")"
      )
      mainZoom(d3mainSvg)

    scope.$watch attrs.plan, (newPlan) ->
      drawGraph(newPlan) if newPlan

    return
