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
