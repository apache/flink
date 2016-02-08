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

.directive 'bsLabel', (JobsService) ->
  transclude: true
  replace: true
  scope: 
    getLabelClass: "&"
    status: "@"

  template: "<span title='{{status}}' ng-class='getLabelClass()'><ng-transclude></ng-transclude></span>"
  
  link: (scope, element, attrs) ->
    scope.getLabelClass = ->
      'label label-' + JobsService.translateLabelState(attrs.status)

# ----------------------------------------------

.directive 'bpLabel', (JobsService) ->
  transclude: true
  replace: true
  scope:
    getBackPressureLabelClass: "&"
    status: "@"

  template: "<span title='{{status}}' ng-class='getBackPressureLabelClass()'><ng-transclude></ng-transclude></span>"

  link: (scope, element, attrs) ->
    scope.getBackPressureLabelClass = ->
      'label label-' + JobsService.translateBackPressureLabelState(attrs.status)

# ----------------------------------------------

.directive 'indicatorPrimary', (JobsService) ->
  replace: true
  scope: 
    getLabelClass: "&"
    status: '@'

  template: "<i title='{{status}}' ng-class='getLabelClass()' />"
  
  link: (scope, element, attrs) ->
    scope.getLabelClass = ->
      'fa fa-circle indicator indicator-' + JobsService.translateLabelState(attrs.status)

# ----------------------------------------------

.directive 'tableProperty', ->
  replace: true
  scope:
    value: '='

  template: "<td title=\"{{value || 'None'}}\">{{value || 'None'}}</td>"
