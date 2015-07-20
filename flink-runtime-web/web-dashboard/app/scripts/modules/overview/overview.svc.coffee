angular.module('flinkApp')

.service 'OverviewService', ($http, flinkConfig, $log) ->
  serverStatus = {}

  @loadServerStatus = ->
    $http.get(flinkConfig.jobServer + "/monitor/status")
    .success (data, status, headers, config) ->
      $log data

    .error (data, status, headers, config) ->
      return

    serverStatus

  @
