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

.directive 'metricsGraph', ->
  template: '<div class="panel panel-default panel-metric">
               <div class="panel-heading">
                 <span class="metric-title">{{metric.id}}</span>
                 <div class="buttons">
                   <div class="btn-group">
                     <button type="button" ng-class="[btnClasses, {active: metric.size != \'big\'}]" ng-click="setSize(\'small\')">Small</button>
                     <button type="button" ng-class="[btnClasses, {active: metric.size == \'big\'}]" ng-click="setSize(\'big\')">Big</button>
                   </div>
                   <a title="Remove" class="btn btn-default btn-xs remove" ng-click="removeMetric()"><i class="fa fa-close" /></a>
                 </div>
               </div>
               <div class="panel-body">
                  <svg ng-if="metric.view == \'chart\'"/>
                  <div ng-if="metric.view != \'chart\'">
                      <div class="metric-numeric" title="{{value | humanizeChartNumericTitle:metric}}">{{value | humanizeChartNumeric:metric}}</div>
                  </div>
               </div>
               <div class="buttons">
                 <div class="btn-group">
                   <button type="button" ng-class="[btnClasses, {active: metric.view == \'chart\'}]" ng-click="setView(\'chart\')">Chart</button>
                   <button type="button" ng-class="[btnClasses, {active: metric.view != \'chart\'}]" ng-click="setView(\'numeric\')">Numeric</button>
                 </div>
             </div>'
  replace: true
  scope:
    metric: "="
    window: "="
    removeMetric: "&"
    setMetricSize: "="
    setMetricView: "="
    getValues: "&"

  link: (scope, element, attrs) ->
    scope.btnClasses = ['btn', 'btn-default', 'btn-xs']

    scope.value = null
    scope.data = [{
      values: scope.getValues()
    }]

    scope.options = {
      x: (d, i) ->
        d.x
      y: (d, i) ->
        d.y

      xTickFormat: (d) ->
        d3.time.format('%H:%M:%S')(new Date(d))

      yTickFormat: (d) ->
        found = false
        pow = 0
        step = 1
        absD = Math.abs(d)

        while !found && pow < 50
          if Math.pow(10, pow) <= absD && absD < Math.pow(10, pow + step)
            found = true
          else
            pow += step

        if found && pow > 6
          "#{d / Math.pow(10, pow)}E#{pow}"
        else
          "#{d}"
    }

    scope.showChart = ->
      d3.select(element.find("svg")[0])
      .datum(scope.data)
      .transition().duration(250)
      .call(scope.chart)

    scope.chart = nv.models.lineChart()
      .options(scope.options)
      .showLegend(false)
      .margin({
        top: 15
        left: 60
        bottom: 30
        right: 30
      })

    scope.chart.yAxis.showMaxMin(false)
    scope.chart.tooltip.hideDelay(0)
    scope.chart.tooltip.contentGenerator((obj) ->
      "<p>#{d3.time.format('%H:%M:%S')(new Date(obj.point.x))} | #{obj.point.y}</p>"
    )

    nv.utils.windowResize(scope.chart.update);

    scope.setSize = (size) ->
      scope.setMetricSize(scope.metric, size)

    scope.setView = (view) ->
      scope.setMetricView(scope.metric, view)
      scope.showChart() if view == 'chart'

    scope.showChart() if scope.metric.view == 'chart'

    scope.$on 'metrics:data:update', (event, timestamp, data) ->
      scope.value = parseFloat(data[scope.metric.id])

      scope.data[0].values.push {
        x: timestamp
        y: scope.value
      }

      if scope.data[0].values.length > scope.window
        scope.data[0].values.shift()

      scope.showChart() if scope.metric.view == 'chart'
      scope.chart.clearHighlights() if scope.metric.view == 'chart'
      scope.chart.tooltip.hidden(true)

    element.find(".metric-title").qtip({
      content: {
        text: scope.metric.id
      },
      position: {
        my: 'bottom left',
        at: 'top left'
      },
      style: {
        classes: 'qtip-light qtip-timeline-bar'
      }
    });