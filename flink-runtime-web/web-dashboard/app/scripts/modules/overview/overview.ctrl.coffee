angular.module('flinkApp')

.controller 'OverviewController', ($scope, OverviewService, JobsService) ->
  $scope.jobObserver = ->
    $scope.runningJobs = JobsService.getJobs('running')
    $scope.finishedJobs = JobsService.getJobs('finished')

  JobsService.registerObserver($scope.jobObserver)
  $scope.$on '$destroy', ->
    JobsService.unRegisterObserver($scope.jobObserver)

  $scope.jobObserver()
