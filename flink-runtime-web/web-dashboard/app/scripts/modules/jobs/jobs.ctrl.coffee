angular.module('flinkApp')

.controller 'RunningJobsController', ($scope, $state, $stateParams, JobsService) ->
  $scope.jobObserver = ->
    $scope.jobs = JobsService.getJobs('running')

  JobsService.registerObserver($scope.jobObserver)
  $scope.$on '$destroy', ->
    JobsService.unRegisterObserver($scope.jobObserver)

  $scope.jobObserver()

# --------------------------------------

.controller 'CompletedJobsController', ($scope, $state, $stateParams, JobsService) ->
  $scope.jobObserver = ->
    $scope.jobs = JobsService.getJobs('finished')

  JobsService.registerObserver($scope.jobObserver)
  $scope.$on '$destroy', ->
    JobsService.unRegisterObserver($scope.jobObserver)

  $scope.jobObserver()

# --------------------------------------

.controller 'SingleJobController', ($scope, $state, $stateParams, JobsService, $rootScope) ->
  $scope.jobid = $stateParams.jobid
  $rootScope.job = null

  JobsService.loadJob($stateParams.jobid).then (data) ->
    $rootScope.job = data

  $scope.$on '$destroy', ->
    $rootScope.job = null

# --------------------------------------

.controller 'JobPlanController', ($scope, $state, $stateParams, JobsService) ->
  JobsService.loadPlan($stateParams.jobid).then (data) ->
    $scope.plan = data

# --------------------------------------

.controller 'JobPlanNodeController', ($scope, $state, $stateParams, JobsService) ->
  $scope.nodeid = $stateParams.nodeid
  $scope.stateList = JobsService.stateList()

  JobsService.getNode($scope.nodeid).then (data) ->
    $scope.node = data

# --------------------------------------

.controller 'JobTimelineVertexController', ($scope, $state, $stateParams, JobsService) ->
  JobsService.getVertex($stateParams.jobid, $stateParams.vertexId).then (data) ->
    $scope.vertex = data
