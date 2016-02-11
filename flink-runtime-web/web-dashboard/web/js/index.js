angular.module('flinkApp', ['ui.router', 'angularMoment']).run(["$rootScope", function($rootScope) {
  $rootScope.sidebarVisible = false;
  return $rootScope.showSidebar = function() {
    $rootScope.sidebarVisible = !$rootScope.sidebarVisible;
    return $rootScope.sidebarClass = 'force-show';
  };
}]).value('flinkConfig', {
  jobServer: '',
  "refresh-interval": 10000
}).run(["JobsService", "MainService", "flinkConfig", "$interval", function(JobsService, MainService, flinkConfig, $interval) {
  return MainService.loadConfig().then(function(config) {
    angular.extend(flinkConfig, config);
    JobsService.listJobs();
    return $interval(function() {
      return JobsService.listJobs();
    }, flinkConfig["refresh-interval"]);
  });
}]).config(["$uiViewScrollProvider", function($uiViewScrollProvider) {
  return $uiViewScrollProvider.useAnchorScroll();
}]).config(["$stateProvider", "$urlRouterProvider", function($stateProvider, $urlRouterProvider) {
  $stateProvider.state("overview", {
    url: "/overview",
    views: {
      main: {
        templateUrl: "partials/overview.html",
        controller: 'OverviewController'
      }
    }
  }).state("running-jobs", {
    url: "/running-jobs",
    views: {
      main: {
        templateUrl: "partials/jobs/running-jobs.html",
        controller: 'RunningJobsController'
      }
    }
  }).state("completed-jobs", {
    url: "/completed-jobs",
    views: {
      main: {
        templateUrl: "partials/jobs/completed-jobs.html",
        controller: 'CompletedJobsController'
      }
    }
  }).state("single-job", {
    url: "/jobs/{jobid}",
    abstract: true,
    views: {
      main: {
        templateUrl: "partials/jobs/job.html",
        controller: 'SingleJobController'
      }
    }
  }).state("single-job.plan", {
    url: "",
    abstract: true,
    views: {
      details: {
        templateUrl: "partials/jobs/job.plan.html",
        controller: 'JobPlanController'
      }
    }
  }).state("single-job.plan.subtasks", {
    url: "",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.subtasks.html",
        controller: 'JobPlanSubtasksController'
      }
    }
  }).state("single-job.plan.taskmanagers", {
    url: "/taskmanagers",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.taskmanagers.html",
        controller: 'JobPlanTaskManagersController'
      }
    }
  }).state("single-job.plan.accumulators", {
    url: "/accumulators",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.accumulators.html",
        controller: 'JobPlanAccumulatorsController'
      }
    }
  }).state("single-job.plan.checkpoints", {
    url: "/checkpoints",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.checkpoints.html",
        controller: 'JobPlanCheckpointsController'
      }
    }
  }).state("single-job.plan.backpressure", {
    url: "/backpressure",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.backpressure.html",
        controller: 'JobPlanBackPressureController'
      }
    }
  }).state("single-job.timeline", {
    url: "/timeline",
    views: {
      details: {
        templateUrl: "partials/jobs/job.timeline.html"
      }
    }
  }).state("single-job.timeline.vertex", {
    url: "/{vertexId}",
    views: {
      vertex: {
        templateUrl: "partials/jobs/job.timeline.vertex.html",
        controller: 'JobTimelineVertexController'
      }
    }
  }).state("single-job.exceptions", {
    url: "/exceptions",
    views: {
      details: {
        templateUrl: "partials/jobs/job.exceptions.html",
        controller: 'JobExceptionsController'
      }
    }
  }).state("single-job.properties", {
    url: "/properties",
    views: {
      details: {
        templateUrl: "partials/jobs/job.properties.html",
        controller: 'JobPropertiesController'
      }
    }
  }).state("single-job.config", {
    url: "/config",
    views: {
      details: {
        templateUrl: "partials/jobs/job.config.html"
      }
    }
  }).state("all-manager", {
    url: "/taskmanagers",
    views: {
      main: {
        templateUrl: "partials/taskmanager/index.html",
        controller: 'AllTaskManagersController'
      }
    }
  }).state("single-manager", {
    url: "/taskmanager/{taskmanagerid}",
    views: {
      main: {
        templateUrl: "partials/taskmanager/taskmanager.html",
        controller: 'SingleTaskManagerController'
      }
    }
  }).state("single-manager.metrics", {
    url: "/metrics",
    views: {
      details: {
        templateUrl: "partials/taskmanager/taskmanager.metrics.html"
      }
    }
  }).state("jobmanager", {
    url: "/jobmanager",
    views: {
      main: {
        templateUrl: "partials/jobmanager/index.html"
      }
    }
  }).state("jobmanager.config", {
    url: "/config",
    views: {
      details: {
        templateUrl: "partials/jobmanager/config.html",
        controller: 'JobManagerConfigController'
      }
    }
  }).state("jobmanager.stdout", {
    url: "/stdout",
    views: {
      details: {
        templateUrl: "partials/jobmanager/stdout.html",
        controller: 'JobManagerStdoutController'
      }
    }
  }).state("jobmanager.log", {
    url: "/log",
    views: {
      details: {
        templateUrl: "partials/jobmanager/log.html",
        controller: 'JobManagerLogsController'
      }
    }
  }).state("submit", {
    url: "/submit",
    views: {
      main: {
        templateUrl: "partials/submit.html",
        controller: "JobSubmitController"
      }
    }
  });
  return $urlRouterProvider.otherwise("/overview");
}]);

angular.module('flinkApp').directive('bsLabel', ["JobsService", function(JobsService) {
  return {
    transclude: true,
    replace: true,
    scope: {
      getLabelClass: "&",
      status: "@"
    },
    template: "<span title='{{status}}' ng-class='getLabelClass()'><ng-transclude></ng-transclude></span>",
    link: function(scope, element, attrs) {
      return scope.getLabelClass = function() {
        return 'label label-' + JobsService.translateLabelState(attrs.status);
      };
    }
  };
}]).directive('bpLabel', ["JobsService", function(JobsService) {
  return {
    transclude: true,
    replace: true,
    scope: {
      getBackPressureLabelClass: "&",
      status: "@"
    },
    template: "<span title='{{status}}' ng-class='getBackPressureLabelClass()'><ng-transclude></ng-transclude></span>",
    link: function(scope, element, attrs) {
      return scope.getBackPressureLabelClass = function() {
        return 'label label-' + JobsService.translateBackPressureLabelState(attrs.status);
      };
    }
  };
}]).directive('indicatorPrimary', ["JobsService", function(JobsService) {
  return {
    replace: true,
    scope: {
      getLabelClass: "&",
      status: '@'
    },
    template: "<i title='{{status}}' ng-class='getLabelClass()' />",
    link: function(scope, element, attrs) {
      return scope.getLabelClass = function() {
        return 'fa fa-circle indicator indicator-' + JobsService.translateLabelState(attrs.status);
      };
    }
  };
}]).directive('tableProperty', function() {
  return {
    replace: true,
    scope: {
      value: '='
    },
    template: "<td title=\"{{value || 'None'}}\">{{value || 'None'}}</td>"
  };
});

angular.module('flinkApp').filter("amDurationFormatExtended", ["angularMomentConfig", function(angularMomentConfig) {
  var amDurationFormatExtendedFilter;
  amDurationFormatExtendedFilter = function(value, format, durationFormat) {
    if (typeof value === "undefined" || value === null) {
      return "";
    }
    return moment.duration(value, format).format(durationFormat, {
      trim: false
    });
  };
  amDurationFormatExtendedFilter.$stateful = angularMomentConfig.statefulFilters;
  return amDurationFormatExtendedFilter;
}]).filter("humanizeDuration", function() {
  return function(value, short) {
    var days, hours, minutes, ms, seconds, x;
    if (typeof value === "undefined" || value === null) {
      return "";
    }
    ms = value % 1000;
    x = Math.floor(value / 1000);
    seconds = x % 60;
    x = Math.floor(x / 60);
    minutes = x % 60;
    x = Math.floor(x / 60);
    hours = x % 24;
    x = Math.floor(x / 24);
    days = x;
    if (days === 0) {
      if (hours === 0) {
        if (minutes === 0) {
          if (seconds === 0) {
            return ms + "ms";
          } else {
            return seconds + "s ";
          }
        } else {
          return minutes + "m " + seconds + "s";
        }
      } else {
        if (short) {
          return hours + "h " + minutes + "m";
        } else {
          return hours + "h " + minutes + "m " + seconds + "s";
        }
      }
    } else {
      if (short) {
        return days + "d " + hours + "h";
      } else {
        return days + "d " + hours + "h " + minutes + "m " + seconds + "s";
      }
    }
  };
}).filter("humanizeText", function() {
  return function(text) {
    if (text) {
      return text.replace(/&gt;/g, ">").replace(/<br\/>/g, "");
    } else {
      return '';
    }
  };
}).filter("humanizeBytes", function() {
  return function(bytes) {
    var converter, units;
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
    converter = function(value, power) {
      var base;
      base = Math.pow(1024, power);
      if (value < base) {
        return (value / base).toFixed(2) + " " + units[power];
      } else if (value < base * 1000) {
        return (value / base).toPrecision(3) + " " + units[power];
      } else {
        return converter(value, power + 1);
      }
    };
    if (typeof bytes === "undefined" || bytes === null) {
      return "";
    }
    if (bytes < 1000) {
      return bytes + " B";
    } else {
      return converter(bytes, 1);
    }
  };
}).filter("toUpperCase", function() {
  return function(text) {
    return text.toUpperCase();
  };
});

angular.module('flinkApp').service('MainService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadConfig = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "config").success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('OverviewController', ["$scope", "OverviewService", "JobsService", "$interval", "flinkConfig", function($scope, OverviewService, JobsService, $interval, flinkConfig) {
  var refresh;
  $scope.jobObserver = function() {
    $scope.runningJobs = JobsService.getJobs('running');
    return $scope.finishedJobs = JobsService.getJobs('finished');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  $scope.jobObserver();
  OverviewService.loadOverview().then(function(data) {
    return $scope.overview = data;
  });
  refresh = $interval(function() {
    return OverviewService.loadOverview().then(function(data) {
      return $scope.overview = data;
    });
  }, flinkConfig["refresh-interval"]);
  return $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
}]);

angular.module('flinkApp').service('OverviewService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var overview;
  overview = {};
  this.loadOverview = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "overview").success(function(data, status, headers, config) {
      overview = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('JobManagerConfigController', ["$scope", "JobManagerConfigService", function($scope, JobManagerConfigService) {
  return JobManagerConfigService.loadConfig().then(function(data) {
    if ($scope.jobmanager == null) {
      $scope.jobmanager = {};
    }
    return $scope.jobmanager['config'] = data;
  });
}]).controller('JobManagerLogsController', ["$scope", "JobManagerLogsService", function($scope, JobManagerLogsService) {
  JobManagerLogsService.loadLogs().then(function(data) {
    if ($scope.jobmanager == null) {
      $scope.jobmanager = {};
    }
    return $scope.jobmanager['log'] = data;
  });
  return $scope.reloadData = function() {
    return JobManagerLogsService.loadLogs().then(function(data) {
      return $scope.jobmanager['log'] = data;
    });
  };
}]).controller('JobManagerStdoutController', ["$scope", "JobManagerStdoutService", function($scope, JobManagerStdoutService) {
  JobManagerStdoutService.loadStdout().then(function(data) {
    if ($scope.jobmanager == null) {
      $scope.jobmanager = {};
    }
    return $scope.jobmanager['stdout'] = data;
  });
  return $scope.reloadData = function() {
    return JobManagerStdoutService.loadStdout().then(function(data) {
      return $scope.jobmanager['stdout'] = data;
    });
  };
}]);

angular.module('flinkApp').service('JobManagerConfigService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var config;
  config = {};
  this.loadConfig = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobmanager/config").success(function(data, status, headers, config) {
      config = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]).service('JobManagerLogsService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var logs;
  logs = {};
  this.loadLogs = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobmanager/log").success(function(data, status, headers, config) {
      logs = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]).service('JobManagerStdoutService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var stdout;
  stdout = {};
  this.loadStdout = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobmanager/stdout").success(function(data, status, headers, config) {
      stdout = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('RunningJobsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  $scope.jobObserver = function() {
    return $scope.jobs = JobsService.getJobs('running');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  return $scope.jobObserver();
}]).controller('CompletedJobsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  $scope.jobObserver = function() {
    return $scope.jobs = JobsService.getJobs('finished');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  return $scope.jobObserver();
}]).controller('SingleJobController', ["$scope", "$state", "$stateParams", "JobsService", "$rootScope", "flinkConfig", "$interval", function($scope, $state, $stateParams, JobsService, $rootScope, flinkConfig, $interval) {
  var refresher;
  console.log('SingleJobController');
  $scope.jobid = $stateParams.jobid;
  $scope.job = null;
  $scope.plan = null;
  $scope.vertices = null;
  $scope.jobCheckpointStats = null;
  $scope.showHistory = false;
  $scope.backPressureOperatorStats = {};
  JobsService.loadJob($stateParams.jobid).then(function(data) {
    $scope.job = data;
    $scope.plan = data.plan;
    return $scope.vertices = data.vertices;
  });
  refresher = $interval(function() {
    return JobsService.loadJob($stateParams.jobid).then(function(data) {
      $scope.job = data;
      return $scope.$broadcast('reload');
    });
  }, flinkConfig["refresh-interval"]);
  $scope.$on('$destroy', function() {
    $scope.job = null;
    $scope.plan = null;
    $scope.vertices = null;
    $scope.jobCheckpointStats = null;
    $scope.backPressureOperatorStats = null;
    return $interval.cancel(refresher);
  });
  $scope.cancelJob = function(cancelEvent) {
    angular.element(cancelEvent.currentTarget).removeClass("btn").removeClass("btn-default").html('Cancelling...');
    return JobsService.cancelJob($stateParams.jobid).then(function(data) {
      return {};
    });
  };
  $scope.stopJob = function(stopEvent) {
    angular.element(stopEvent.currentTarget).removeClass("btn").removeClass("btn-default").html('Stopping...');
    return JobsService.stopJob($stateParams.jobid).then(function(data) {
      return {};
    });
  };
  return $scope.toggleHistory = function() {
    return $scope.showHistory = !$scope.showHistory;
  };
}]).controller('JobPlanController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  console.log('JobPlanController');
  $scope.nodeid = null;
  $scope.nodeUnfolded = false;
  $scope.stateList = JobsService.stateList();
  $scope.changeNode = function(nodeid) {
    if (nodeid !== $scope.nodeid) {
      $scope.nodeid = nodeid;
      $scope.vertex = null;
      $scope.subtasks = null;
      $scope.accumulators = null;
      $scope.operatorCheckpointStats = null;
      return $scope.$broadcast('reload');
    } else {
      $scope.nodeid = null;
      $scope.nodeUnfolded = false;
      $scope.vertex = null;
      $scope.subtasks = null;
      $scope.accumulators = null;
      return $scope.operatorCheckpointStats = null;
    }
  };
  $scope.deactivateNode = function() {
    $scope.nodeid = null;
    $scope.nodeUnfolded = false;
    $scope.vertex = null;
    $scope.subtasks = null;
    $scope.accumulators = null;
    return $scope.operatorCheckpointStats = null;
  };
  return $scope.toggleFold = function() {
    return $scope.nodeUnfolded = !$scope.nodeUnfolded;
  };
}]).controller('JobPlanSubtasksController', ["$scope", "JobsService", function($scope, JobsService) {
  console.log('JobPlanSubtasksController');
  if ($scope.nodeid && (!$scope.vertex || !$scope.vertex.st)) {
    JobsService.getSubtasks($scope.nodeid).then(function(data) {
      return $scope.subtasks = data;
    });
  }
  return $scope.$on('reload', function(event) {
    console.log('JobPlanSubtasksController');
    if ($scope.nodeid) {
      return JobsService.getSubtasks($scope.nodeid).then(function(data) {
        return $scope.subtasks = data;
      });
    }
  });
}]).controller('JobPlanTaskManagersController', ["$scope", "JobsService", function($scope, JobsService) {
  console.log('JobPlanTaskManagersController');
  if ($scope.nodeid && (!$scope.vertex || !$scope.vertex.st)) {
    JobsService.getTaskManagers($scope.nodeid).then(function(data) {
      return $scope.taskmanagers = data;
    });
  }
  return $scope.$on('reload', function(event) {
    console.log('JobPlanTaskManagersController');
    if ($scope.nodeid) {
      return JobsService.getTaskManagers($scope.nodeid).then(function(data) {
        return $scope.taskmanagers = data;
      });
    }
  });
}]).controller('JobPlanAccumulatorsController', ["$scope", "JobsService", function($scope, JobsService) {
  console.log('JobPlanAccumulatorsController');
  if ($scope.nodeid && (!$scope.vertex || !$scope.vertex.accumulators)) {
    JobsService.getAccumulators($scope.nodeid).then(function(data) {
      $scope.accumulators = data.main;
      return $scope.subtaskAccumulators = data.subtasks;
    });
  }
  return $scope.$on('reload', function(event) {
    console.log('JobPlanAccumulatorsController');
    if ($scope.nodeid) {
      return JobsService.getAccumulators($scope.nodeid).then(function(data) {
        $scope.accumulators = data.main;
        return $scope.subtaskAccumulators = data.subtasks;
      });
    }
  });
}]).controller('JobPlanCheckpointsController', ["$scope", "JobsService", function($scope, JobsService) {
  console.log('JobPlanCheckpointsController');
  JobsService.getJobCheckpointStats($scope.jobid).then(function(data) {
    return $scope.jobCheckpointStats = data;
  });
  if ($scope.nodeid && (!$scope.vertex || !$scope.vertex.operatorCheckpointStats)) {
    JobsService.getOperatorCheckpointStats($scope.nodeid).then(function(data) {
      $scope.operatorCheckpointStats = data.operatorStats;
      return $scope.subtasksCheckpointStats = data.subtasksStats;
    });
  }
  return $scope.$on('reload', function(event) {
    console.log('JobPlanCheckpointsController');
    JobsService.getJobCheckpointStats($scope.jobid).then(function(data) {
      return $scope.jobCheckpointStats = data;
    });
    if ($scope.nodeid) {
      return JobsService.getOperatorCheckpointStats($scope.nodeid).then(function(data) {
        $scope.operatorCheckpointStats = data.operatorStats;
        return $scope.subtasksCheckpointStats = data.subtasksStats;
      });
    }
  });
}]).controller('JobPlanBackPressureController', ["$scope", "JobsService", function($scope, JobsService) {
  console.log('JobPlanBackPressureController');
  $scope.now = Date.now();
  if ($scope.nodeid) {
    JobsService.getOperatorBackPressure($scope.nodeid).then(function(data) {
      return $scope.backPressureOperatorStats[$scope.nodeid] = data;
    });
  }
  return $scope.$on('reload', function(event) {
    console.log('JobPlanBackPressureController (relaod)');
    $scope.now = Date.now();
    if ($scope.nodeid) {
      return JobsService.getOperatorBackPressure($scope.nodeid).then(function(data) {
        return $scope.backPressureOperatorStats[$scope.nodeid] = data;
      });
    }
  });
}]).controller('JobTimelineVertexController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  console.log('JobTimelineVertexController');
  JobsService.getVertex($stateParams.vertexId).then(function(data) {
    return $scope.vertex = data;
  });
  return $scope.$on('reload', function(event) {
    console.log('JobTimelineVertexController');
    return JobsService.getVertex($stateParams.vertexId).then(function(data) {
      return $scope.vertex = data;
    });
  });
}]).controller('JobExceptionsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  return JobsService.loadExceptions().then(function(data) {
    return $scope.exceptions = data;
  });
}]).controller('JobPropertiesController', ["$scope", "JobsService", function($scope, JobsService) {
  console.log('JobPropertiesController');
  return $scope.changeNode = function(nodeid) {
    if (nodeid !== $scope.nodeid) {
      $scope.nodeid = nodeid;
      return JobsService.getNode(nodeid).then(function(data) {
        return $scope.node = data;
      });
    } else {
      $scope.nodeid = null;
      return $scope.node = null;
    }
  };
}]);

angular.module('flinkApp').directive('vertex', ["$state", function($state) {
  return {
    template: "<svg class='timeline secondary' width='0' height='0'></svg>",
    scope: {
      data: "="
    },
    link: function(scope, elem, attrs) {
      var analyzeTime, containerW, svgEl;
      svgEl = elem.children()[0];
      containerW = elem.width();
      angular.element(svgEl).attr('width', containerW);
      analyzeTime = function(data) {
        var chart, svg, testData;
        d3.select(svgEl).selectAll("*").remove();
        testData = [];
        angular.forEach(data.subtasks, function(subtask, i) {
          var times;
          times = [
            {
              label: "Scheduled",
              color: "#666",
              borderColor: "#555",
              starting_time: subtask.timestamps["SCHEDULED"],
              ending_time: subtask.timestamps["DEPLOYING"],
              type: 'regular'
            }, {
              label: "Deploying",
              color: "#aaa",
              borderColor: "#555",
              starting_time: subtask.timestamps["DEPLOYING"],
              ending_time: subtask.timestamps["RUNNING"],
              type: 'regular'
            }
          ];
          if (subtask.timestamps["FINISHED"] > 0) {
            times.push({
              label: "Running",
              color: "#ddd",
              borderColor: "#555",
              starting_time: subtask.timestamps["RUNNING"],
              ending_time: subtask.timestamps["FINISHED"],
              type: 'regular'
            });
          }
          return testData.push({
            label: "(" + subtask.subtask + ") " + subtask.host,
            times: times
          });
        });
        chart = d3.timeline().stack().tickFormat({
          format: d3.time.format("%L"),
          tickSize: 1
        }).prefix("single").labelFormat(function(label) {
          return label;
        }).margin({
          left: 100,
          right: 0,
          top: 0,
          bottom: 0
        }).itemHeight(30).relativeTime();
        return svg = d3.select(svgEl).datum(testData).call(chart);
      };
      analyzeTime(scope.data);
    }
  };
}]).directive('timeline', ["$state", function($state) {
  return {
    template: "<svg class='timeline' width='0' height='0'></svg>",
    scope: {
      vertices: "=",
      jobid: "="
    },
    link: function(scope, elem, attrs) {
      var analyzeTime, containerW, svgEl, translateLabel;
      svgEl = elem.children()[0];
      containerW = elem.width();
      angular.element(svgEl).attr('width', containerW);
      translateLabel = function(label) {
        return label.replace("&gt;", ">");
      };
      analyzeTime = function(data) {
        var chart, svg, testData;
        d3.select(svgEl).selectAll("*").remove();
        testData = [];
        angular.forEach(data, function(vertex) {
          if (vertex['start-time'] > -1) {
            if (vertex.type === 'scheduled') {
              return testData.push({
                times: [
                  {
                    label: translateLabel(vertex.name),
                    color: "#cccccc",
                    borderColor: "#555555",
                    starting_time: vertex['start-time'],
                    ending_time: vertex['end-time'],
                    type: vertex.type
                  }
                ]
              });
            } else {
              return testData.push({
                times: [
                  {
                    label: translateLabel(vertex.name),
                    color: "#d9f1f7",
                    borderColor: "#62cdea",
                    starting_time: vertex['start-time'],
                    ending_time: vertex['end-time'],
                    link: vertex.id,
                    type: vertex.type
                  }
                ]
              });
            }
          }
        });
        chart = d3.timeline().stack().click(function(d, i, datum) {
          if (d.link) {
            return $state.go("single-job.timeline.vertex", {
              jobid: scope.jobid,
              vertexId: d.link
            });
          }
        }).tickFormat({
          format: d3.time.format("%L"),
          tickSize: 1
        }).prefix("main").margin({
          left: 0,
          right: 0,
          top: 0,
          bottom: 0
        }).itemHeight(30).showBorderLine().showHourTimeline();
        return svg = d3.select(svgEl).datum(testData).call(chart);
      };
      scope.$watch(attrs.vertices, function(data) {
        if (data) {
          return analyzeTime(data);
        }
      });
    }
  };
}]).directive('jobPlan', ["$timeout", function($timeout) {
  return {
    template: "<svg class='graph' width='500' height='400'><g /></svg> <svg class='tmp' width='1' height='1'><g /></svg> <div class='btn-group zoom-buttons'> <a class='btn btn-default zoom-in' ng-click='zoomIn()'><i class='fa fa-plus' /></a> <a class='btn btn-default zoom-out' ng-click='zoomOut()'><i class='fa fa-minus' /></a> </div>",
    scope: {
      plan: '=',
      setNode: '&'
    },
    link: function(scope, elem, attrs) {
      var containerW, createEdge, createLabelEdge, createLabelNode, createNode, d3mainSvg, d3mainSvgG, d3tmpSvg, drawGraph, extendLabelNodeForIteration, g, getNodeType, isSpecialIterationNode, jobid, loadJsonToDagre, mainG, mainSvgElement, mainTmpElement, mainZoom, searchForNode, shortenString, subgraphs;
      g = null;
      mainZoom = d3.behavior.zoom();
      subgraphs = [];
      jobid = attrs.jobid;
      mainSvgElement = elem.children()[0];
      mainG = elem.children().children()[0];
      mainTmpElement = elem.children()[1];
      d3mainSvg = d3.select(mainSvgElement);
      d3mainSvgG = d3.select(mainG);
      d3tmpSvg = d3.select(mainTmpElement);
      containerW = elem.width();
      angular.element(elem.children()[0]).width(containerW);
      scope.zoomIn = function() {
        var translate, v1, v2;
        if (mainZoom.scale() < 2.99) {
          translate = mainZoom.translate();
          v1 = translate[0] * (mainZoom.scale() + 0.1 / (mainZoom.scale()));
          v2 = translate[1] * (mainZoom.scale() + 0.1 / (mainZoom.scale()));
          mainZoom.scale(mainZoom.scale() + 0.1);
          mainZoom.translate([v1, v2]);
          return d3mainSvgG.attr("transform", "translate(" + v1 + "," + v2 + ") scale(" + mainZoom.scale() + ")");
        }
      };
      scope.zoomOut = function() {
        var translate, v1, v2;
        if (mainZoom.scale() > 0.31) {
          mainZoom.scale(mainZoom.scale() - 0.1);
          translate = mainZoom.translate();
          v1 = translate[0] * (mainZoom.scale() - 0.1 / (mainZoom.scale()));
          v2 = translate[1] * (mainZoom.scale() - 0.1 / (mainZoom.scale()));
          mainZoom.translate([v1, v2]);
          return d3mainSvgG.attr("transform", "translate(" + v1 + "," + v2 + ") scale(" + mainZoom.scale() + ")");
        }
      };
      createLabelEdge = function(el) {
        var labelValue;
        labelValue = "";
        if ((el.ship_strategy != null) || (el.local_strategy != null)) {
          labelValue += "<div class='edge-label'>";
          if (el.ship_strategy != null) {
            labelValue += el.ship_strategy;
          }
          if (el.temp_mode !== undefined) {
            labelValue += " (" + el.temp_mode + ")";
          }
          if (el.local_strategy !== undefined) {
            labelValue += ",<br>" + el.local_strategy;
          }
          labelValue += "</div>";
        }
        return labelValue;
      };
      isSpecialIterationNode = function(info) {
        return info === "partialSolution" || info === "nextPartialSolution" || info === "workset" || info === "nextWorkset" || info === "solutionSet" || info === "solutionDelta";
      };
      getNodeType = function(el, info) {
        if (info === "mirror") {
          return 'node-mirror';
        } else if (isSpecialIterationNode(info)) {
          return 'node-iteration';
        } else {
          return 'node-normal';
        }
      };
      createLabelNode = function(el, info, maxW, maxH) {
        var labelValue, stepName;
        labelValue = "<div href='#/jobs/" + jobid + "/vertex/" + el.id + "' class='node-label " + getNodeType(el, info) + "'>";
        if (info === "mirror") {
          labelValue += "<h3 class='node-name'>Mirror of " + el.operator + "</h3>";
        } else {
          labelValue += "<h3 class='node-name'>" + el.operator + "</h3>";
        }
        if (el.description === "") {
          labelValue += "";
        } else {
          stepName = el.description;
          stepName = shortenString(stepName);
          labelValue += "<h4 class='step-name'>" + stepName + "</h4>";
        }
        if (el.step_function != null) {
          labelValue += extendLabelNodeForIteration(el.id, maxW, maxH);
        } else {
          if (isSpecialIterationNode(info)) {
            labelValue += "<h5>" + info + " Node</h5>";
          }
          if (el.parallelism !== "") {
            labelValue += "<h5>Parallelism: " + el.parallelism + "</h5>";
          }
          if (el.operator !== undefined) {
            labelValue += "<h5>Operation: " + shortenString(el.operator_strategy) + "</h5>";
          }
        }
        labelValue += "</div>";
        return labelValue;
      };
      extendLabelNodeForIteration = function(id, maxW, maxH) {
        var labelValue, svgID;
        svgID = "svg-" + id;
        labelValue = "<svg class='" + svgID + "' width=" + maxW + " height=" + maxH + "><g /></svg>";
        return labelValue;
      };
      shortenString = function(s) {
        var sbr;
        if (s.charAt(0) === "<") {
          s = s.replace("<", "&lt;");
          s = s.replace(">", "&gt;");
        }
        sbr = "";
        while (s.length > 30) {
          sbr = sbr + s.substring(0, 30) + "<br>";
          s = s.substring(30, s.length);
        }
        sbr = sbr + s;
        return sbr;
      };
      createNode = function(g, data, el, isParent, maxW, maxH) {
        if (isParent == null) {
          isParent = false;
        }
        if (el.id === data.partial_solution) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "partialSolution", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "partialSolution")
          });
        } else if (el.id === data.next_partial_solution) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "nextPartialSolution", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "nextPartialSolution")
          });
        } else if (el.id === data.workset) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "workset", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "workset")
          });
        } else if (el.id === data.next_workset) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "nextWorkset", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "nextWorkset")
          });
        } else if (el.id === data.solution_set) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "solutionSet", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "solutionSet")
          });
        } else if (el.id === data.solution_delta) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "solutionDelta", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "solutionDelta")
          });
        } else {
          return g.setNode(el.id, {
            label: createLabelNode(el, "", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "")
          });
        }
      };
      createEdge = function(g, data, el, existingNodes, pred) {
        var missingNode;
        if (existingNodes.indexOf(pred.id) !== -1) {
          return g.setEdge(pred.id, el.id, {
            label: createLabelEdge(pred),
            labelType: 'html',
            arrowhead: 'normal'
          });
        } else {
          missingNode = searchForNode(data, pred.id);
          if (!!missingNode) {
            return g.setEdge(missingNode.id, el.id, {
              label: createLabelEdge(missingNode),
              labelType: 'html'
            });
          }
        }
      };
      loadJsonToDagre = function(g, data) {
        var el, existingNodes, isParent, k, l, len, len1, maxH, maxW, pred, r, ref, sg, toIterate;
        existingNodes = [];
        if (data.nodes != null) {
          toIterate = data.nodes;
        } else {
          toIterate = data.step_function;
          isParent = true;
        }
        for (k = 0, len = toIterate.length; k < len; k++) {
          el = toIterate[k];
          maxW = 0;
          maxH = 0;
          if (el.step_function) {
            sg = new dagreD3.graphlib.Graph({
              multigraph: true,
              compound: true
            }).setGraph({
              nodesep: 20,
              edgesep: 0,
              ranksep: 20,
              rankdir: "LR",
              marginx: 10,
              marginy: 10
            });
            subgraphs[el.id] = sg;
            loadJsonToDagre(sg, el);
            r = new dagreD3.render();
            d3tmpSvg.select('g').call(r, sg);
            maxW = sg.graph().width;
            maxH = sg.graph().height;
            angular.element(mainTmpElement).empty();
          }
          createNode(g, data, el, isParent, maxW, maxH);
          existingNodes.push(el.id);
          if (el.inputs != null) {
            ref = el.inputs;
            for (l = 0, len1 = ref.length; l < len1; l++) {
              pred = ref[l];
              createEdge(g, data, el, existingNodes, pred);
            }
          }
        }
        return g;
      };
      searchForNode = function(data, nodeID) {
        var el, i, j;
        for (i in data.nodes) {
          el = data.nodes[i];
          if (el.id === nodeID) {
            return el;
          }
          if (el.step_function != null) {
            for (j in el.step_function) {
              if (el.step_function[j].id === nodeID) {
                return el.step_function[j];
              }
            }
          }
        }
      };
      drawGraph = function(data) {
        var i, newScale, renderer, sg, xCenterOffset, yCenterOffset;
        g = new dagreD3.graphlib.Graph({
          multigraph: true,
          compound: true
        }).setGraph({
          nodesep: 70,
          edgesep: 0,
          ranksep: 50,
          rankdir: "LR",
          marginx: 40,
          marginy: 40
        });
        loadJsonToDagre(g, data);
        renderer = new dagreD3.render();
        d3mainSvgG.call(renderer, g);
        for (i in subgraphs) {
          sg = subgraphs[i];
          d3mainSvg.select('svg.svg-' + i + ' g').call(renderer, sg);
        }
        newScale = 0.5;
        xCenterOffset = Math.floor((angular.element(mainSvgElement).width() - g.graph().width * newScale) / 2);
        yCenterOffset = Math.floor((angular.element(mainSvgElement).height() - g.graph().height * newScale) / 2);
        mainZoom.scale(newScale).translate([xCenterOffset, yCenterOffset]);
        d3mainSvgG.attr("transform", "translate(" + xCenterOffset + ", " + yCenterOffset + ") scale(" + mainZoom.scale() + ")");
        mainZoom.on("zoom", function() {
          var ev;
          ev = d3.event;
          return d3mainSvgG.attr("transform", "translate(" + ev.translate + ") scale(" + ev.scale + ")");
        });
        mainZoom(d3mainSvg);
        return d3mainSvgG.selectAll('.node').on('click', function(d) {
          return scope.setNode({
            nodeid: d
          });
        });
      };
      scope.$watch(attrs.plan, function(newPlan) {
        if (newPlan) {
          return drawGraph(newPlan);
        }
      });
    }
  };
}]);

angular.module('flinkApp').service('JobsService', ["$http", "flinkConfig", "$log", "amMoment", "$q", "$timeout", function($http, flinkConfig, $log, amMoment, $q, $timeout) {
  var currentJob, currentPlan, deferreds, jobObservers, jobs, notifyObservers;
  currentJob = null;
  currentPlan = null;
  deferreds = {};
  jobs = {
    running: [],
    finished: [],
    cancelled: [],
    failed: []
  };
  jobObservers = [];
  notifyObservers = function() {
    return angular.forEach(jobObservers, function(callback) {
      return callback();
    });
  };
  this.registerObserver = function(callback) {
    return jobObservers.push(callback);
  };
  this.unRegisterObserver = function(callback) {
    var index;
    index = jobObservers.indexOf(callback);
    return jobObservers.splice(index, 1);
  };
  this.stateList = function() {
    return ['SCHEDULED', 'DEPLOYING', 'RUNNING', 'FINISHED', 'FAILED', 'CANCELING', 'CANCELED'];
  };
  this.translateLabelState = function(state) {
    switch (state.toLowerCase()) {
      case 'finished':
        return 'success';
      case 'failed':
        return 'danger';
      case 'scheduled':
        return 'default';
      case 'deploying':
        return 'info';
      case 'running':
        return 'primary';
      case 'canceling':
        return 'warning';
      case 'pending':
        return 'info';
      case 'total':
        return 'black';
      default:
        return 'default';
    }
  };
  this.setEndTimes = function(list) {
    return angular.forEach(list, function(item, jobKey) {
      if (!(item['end-time'] > -1)) {
        return item['end-time'] = item['start-time'] + item['duration'];
      }
    });
  };
  this.processVertices = function(data) {
    angular.forEach(data.vertices, function(vertex, i) {
      return vertex.type = 'regular';
    });
    return data.vertices.unshift({
      name: 'Scheduled',
      'start-time': data.timestamps['CREATED'],
      'end-time': data.timestamps['CREATED'] + 1,
      type: 'scheduled'
    });
  };
  this.listJobs = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "joboverview").success((function(_this) {
      return function(data, status, headers, config) {
        angular.forEach(data, function(list, listKey) {
          switch (listKey) {
            case 'running':
              return jobs.running = _this.setEndTimes(list);
            case 'finished':
              return jobs.finished = _this.setEndTimes(list);
            case 'cancelled':
              return jobs.cancelled = _this.setEndTimes(list);
            case 'failed':
              return jobs.failed = _this.setEndTimes(list);
          }
        });
        deferred.resolve(jobs);
        return notifyObservers();
      };
    })(this));
    return deferred.promise;
  };
  this.getJobs = function(type) {
    return jobs[type];
  };
  this.getAllJobs = function() {
    return jobs;
  };
  this.loadJob = function(jobid) {
    currentJob = null;
    deferreds.job = $q.defer();
    $http.get(flinkConfig.jobServer + "jobs/" + jobid).success((function(_this) {
      return function(data, status, headers, config) {
        _this.setEndTimes(data.vertices);
        _this.processVertices(data);
        return $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/config").success(function(jobConfig) {
          data = angular.extend(data, jobConfig);
          currentJob = data;
          return deferreds.job.resolve(currentJob);
        });
      };
    })(this));
    return deferreds.job.promise;
  };
  this.getNode = function(nodeid) {
    var deferred, seekNode;
    seekNode = function(nodeid, data) {
      var j, len, node, sub;
      for (j = 0, len = data.length; j < len; j++) {
        node = data[j];
        if (node.id === nodeid) {
          return node;
        }
        if (node.step_function) {
          sub = seekNode(nodeid, node.step_function);
        }
        if (sub) {
          return sub;
        }
      }
      return null;
    };
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        var foundNode;
        foundNode = seekNode(nodeid, currentJob.plan.nodes);
        foundNode.vertex = _this.seekVertex(nodeid);
        return deferred.resolve(foundNode);
      };
    })(this));
    return deferred.promise;
  };
  this.seekVertex = function(nodeid) {
    var j, len, ref, vertex;
    ref = currentJob.vertices;
    for (j = 0, len = ref.length; j < len; j++) {
      vertex = ref[j];
      if (vertex.id === nodeid) {
        return vertex;
      }
    }
    return null;
  };
  this.getVertex = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        var vertex;
        vertex = _this.seekVertex(vertexid);
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasktimes").success(function(data) {
          vertex.subtasks = data.subtasks;
          return deferred.resolve(vertex);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getSubtasks = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid).success(function(data) {
          var subtasks;
          subtasks = data.subtasks;
          return deferred.resolve(subtasks);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getTaskManagers = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/taskmanagers").success(function(data) {
          var taskmanagers;
          taskmanagers = data.taskmanagers;
          return deferred.resolve(taskmanagers);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getAccumulators = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/accumulators").success(function(data) {
          var accumulators;
          accumulators = data['user-accumulators'];
          return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasks/accumulators").success(function(data) {
            var subtaskAccumulators;
            subtaskAccumulators = data.subtasks;
            return deferred.resolve({
              main: accumulators,
              subtasks: subtaskAccumulators
            });
          });
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getJobCheckpointStats = function(jobid) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/checkpoints").success((function(_this) {
      return function(data, status, headers, config) {
        if (angular.equals({}, data)) {
          return deferred.resolve(deferred.resolve(null));
        } else {
          return deferred.resolve(data);
        }
      };
    })(this));
    return deferred.promise;
  };
  this.getOperatorCheckpointStats = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/checkpoints").success(function(data) {
          var operatorStats, subtaskStats;
          if (angular.equals({}, data)) {
            return deferred.resolve({
              operatorStats: null,
              subtasksStats: null
            });
          } else {
            operatorStats = {
              id: data['id'],
              timestamp: data['timestamp'],
              duration: data['duration'],
              size: data['size']
            };
            if (angular.equals({}, data['subtasks'])) {
              return deferred.resolve({
                operatorStats: operatorStats,
                subtasksStats: null
              });
            } else {
              subtaskStats = data['subtasks'];
              return deferred.resolve({
                operatorStats: operatorStats,
                subtasksStats: subtaskStats
              });
            }
          }
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getOperatorBackPressure = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/backpressure").success((function(_this) {
      return function(data) {
        return deferred.resolve(data);
      };
    })(this));
    return deferred.promise;
  };
  this.translateBackPressureLabelState = function(state) {
    switch (state.toLowerCase()) {
      case 'in-progress':
        return 'danger';
      case 'ok':
        return 'success';
      case 'low':
        return 'warning';
      case 'high':
        return 'danger';
      default:
        return 'default';
    }
  };
  this.loadExceptions = function() {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/exceptions").success(function(exceptions) {
          currentJob.exceptions = exceptions;
          return deferred.resolve(exceptions);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.cancelJob = function(jobid) {
    return $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/yarn-cancel");
  };
  this.stopJob = function(jobid) {
    return $http.get("jobs/" + jobid + "/yarn-stop");
  };
  return this;
}]);

angular.module('flinkApp').controller('AllTaskManagersController', ["$scope", "TaskManagersService", "$interval", "flinkConfig", function($scope, TaskManagersService, $interval, flinkConfig) {
  var refresh;
  TaskManagersService.loadManagers().then(function(data) {
    return $scope.managers = data;
  });
  refresh = $interval(function() {
    return TaskManagersService.loadManagers().then(function(data) {
      return $scope.managers = data;
    });
  }, flinkConfig["refresh-interval"]);
  return $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
}]).controller('SingleTaskManagerController', ["$scope", "$stateParams", "SingleTaskManagerService", "$interval", "flinkConfig", function($scope, $stateParams, SingleTaskManagerService, $interval, flinkConfig) {
  var refresh;
  $scope.metrics = {};
  SingleTaskManagerService.loadMetrics($stateParams.taskmanagerid).then(function(data) {
    return $scope.metrics = data[0];
  });
  refresh = $interval(function() {
    return SingleTaskManagerService.loadMetrics($stateParams.taskmanagerid).then(function(data) {
      return $scope.metrics = data[0];
    });
  }, flinkConfig["refresh-interval"]);
  return $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
}]);

angular.module('flinkApp').service('TaskManagersService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadManagers = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "taskmanagers").success(function(data, status, headers, config) {
      return deferred.resolve(data['taskmanagers']);
    });
    return deferred.promise;
  };
  return this;
}]).service('SingleTaskManagerService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadMetrics = function(taskmanagerid) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "taskmanagers/" + taskmanagerid).success(function(data, status, headers, config) {
      return deferred.resolve(data['taskmanagers']);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('JobSubmitController', ["$scope", "JobSubmitService", "$interval", "flinkConfig", "$state", "$location", function($scope, JobSubmitService, $interval, flinkConfig, $state, $location) {
  var refresh;
  $scope.yarn = $location.absUrl().indexOf("/proxy/application_") !== -1;
  $scope.loadList = function() {
    return JobSubmitService.loadJarList().then(function(data) {
      $scope.address = data.address;
      $scope.noaccess = data.error;
      return $scope.jars = data.files;
    });
  };
  $scope.defaultState = function() {
    $scope.plan = null;
    $scope.error = null;
    return $scope.state = {
      selected: null,
      parallelism: "",
      'entry-class': "",
      'program-args': "",
      'plan-button': "Show Plan",
      'submit-button': "Submit",
      'action-time': 0
    };
  };
  $scope.defaultState();
  $scope.uploader = {};
  $scope.loadList();
  refresh = $interval(function() {
    return $scope.loadList();
  }, flinkConfig["refresh-interval"]);
  $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
  $scope.selectJar = function(id) {
    if ($scope.state.selected === id) {
      return $scope.defaultState();
    } else {
      $scope.defaultState();
      return $scope.state.selected = id;
    }
  };
  $scope.deleteJar = function(event, id) {
    if ($scope.state.selected === id) {
      $scope.defaultState();
    }
    angular.element(event.currentTarget).removeClass("fa-remove").addClass("fa-spin fa-spinner");
    return JobSubmitService.deleteJar(id).then(function(data) {
      angular.element(event.currentTarget).removeClass("fa-spin fa-spinner").addClass("fa-remove");
      if (data.error != null) {
        return alert(data.error);
      }
    });
  };
  $scope.loadEntryClass = function(name) {
    return $scope.state['entry-class'] = name;
  };
  $scope.getPlan = function() {
    var action;
    if ($scope.state['plan-button'] === "Show Plan") {
      action = new Date().getTime();
      $scope.state['action-time'] = action;
      $scope.state['submit-button'] = "Submit";
      $scope.state['plan-button'] = "Getting Plan";
      $scope.error = null;
      $scope.plan = null;
      return JobSubmitService.getPlan($scope.state.selected, {
        'entry-class': $scope.state['entry-class'],
        parallelism: $scope.state.parallelism,
        'program-args': $scope.state['program-args']
      }).then(function(data) {
        if (action === $scope.state['action-time']) {
          $scope.state['plan-button'] = "Show Plan";
          $scope.error = data.error;
          return $scope.plan = data.plan;
        }
      });
    }
  };
  $scope.runJob = function() {
    var action;
    if ($scope.state['submit-button'] === "Submit") {
      action = new Date().getTime();
      $scope.state['action-time'] = action;
      $scope.state['submit-button'] = "Submitting";
      $scope.state['plan-button'] = "Show Plan";
      $scope.error = null;
      return JobSubmitService.runJob($scope.state.selected, {
        'entry-class': $scope.state['entry-class'],
        parallelism: $scope.state.parallelism,
        'program-args': $scope.state['program-args']
      }).then(function(data) {
        if (action === $scope.state['action-time']) {
          $scope.state['submit-button'] = "Submit";
          $scope.error = data.error;
          if (data.jobid != null) {
            return $state.go("single-job.plan.subtasks", {
              jobid: data.jobid
            });
          }
        }
      });
    }
  };
  $scope.nodeid = null;
  $scope.changeNode = function(nodeid) {
    if (nodeid !== $scope.nodeid) {
      $scope.nodeid = nodeid;
      $scope.vertex = null;
      $scope.subtasks = null;
      $scope.accumulators = null;
      return $scope.$broadcast('reload');
    } else {
      $scope.nodeid = null;
      $scope.nodeUnfolded = false;
      $scope.vertex = null;
      $scope.subtasks = null;
      return $scope.accumulators = null;
    }
  };
  $scope.clearFiles = function() {
    return $scope.uploader = {};
  };
  $scope.uploadFiles = function(files) {
    $scope.uploader = {};
    if (files.length === 1) {
      $scope.uploader['file'] = files[0];
      return $scope.uploader['upload'] = true;
    } else {
      return $scope.uploader['error'] = "Did ya forget to select a file?";
    }
  };
  return $scope.startUpload = function() {
    var formdata, xhr;
    if ($scope.uploader['file'] != null) {
      formdata = new FormData();
      formdata.append("jarfile", $scope.uploader['file']);
      $scope.uploader['upload'] = false;
      $scope.uploader['success'] = "Initializing upload...";
      xhr = new XMLHttpRequest();
      xhr.upload.onprogress = function(event) {
        $scope.uploader['success'] = null;
        return $scope.uploader['progress'] = parseInt(100 * event.loaded / event.total);
      };
      xhr.upload.onerror = function(event) {
        $scope.uploader['progress'] = null;
        return $scope.uploader['error'] = "An error occurred while uploading your file";
      };
      xhr.upload.onload = function(event) {
        $scope.uploader['progress'] = null;
        return $scope.uploader['success'] = "Saving...";
      };
      xhr.onreadystatechange = function() {
        var response;
        if (xhr.readyState === 4) {
          response = JSON.parse(xhr.responseText);
          if (response.error != null) {
            $scope.uploader['error'] = response.error;
            return $scope.uploader['success'] = null;
          } else {
            return $scope.uploader['success'] = "Uploaded!";
          }
        }
      };
      xhr.open("POST", "/jars/upload");
      return xhr.send(formdata);
    } else {
      return console.log("Unexpected Error. This should not happen");
    }
  };
}]).filter('getJarSelectClass', function() {
  return function(selected, actual) {
    if (selected === actual) {
      return "fa-check-square";
    } else {
      return "fa-square-o";
    }
  };
});

angular.module('flinkApp').service('JobSubmitService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadJarList = function() {
    var deferred;
    deferred = $q.defer();
    $http.get("jars/").success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  this.deleteJar = function(id) {
    var deferred;
    deferred = $q.defer();
    $http["delete"]("jars/" + id).success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  this.getPlan = function(id, args) {
    var deferred;
    deferred = $q.defer();
    $http.get("jars/" + id + "/plan", {
      params: args
    }).success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  this.runJob = function(id, args) {
    var deferred;
    deferred = $q.defer();
    $http.post("jars/" + id + "/run", {}, {
      params: args
    }).success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbImluZGV4LmNvZmZlZSIsImluZGV4LmpzIiwiY29tbW9uL2RpcmVjdGl2ZXMuY29mZmVlIiwiY29tbW9uL2RpcmVjdGl2ZXMuanMiLCJjb21tb24vZmlsdGVycy5jb2ZmZWUiLCJjb21tb24vZmlsdGVycy5qcyIsImNvbW1vbi9zZXJ2aWNlcy5jb2ZmZWUiLCJjb21tb24vc2VydmljZXMuanMiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LmN0cmwuY29mZmVlIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5jdHJsLmpzIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5zdmMuY29mZmVlIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5zdmMuanMiLCJtb2R1bGVzL2pvYm1hbmFnZXIvam9ibWFuYWdlci5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvam9ibWFuYWdlci9qb2JtYW5hZ2VyLmN0cmwuanMiLCJtb2R1bGVzL2pvYm1hbmFnZXIvam9ibWFuYWdlci5zdmMuY29mZmVlIiwibW9kdWxlcy9qb2JtYW5hZ2VyL2pvYm1hbmFnZXIuc3ZjLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5jdHJsLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuZGlyLmNvZmZlZSIsIm1vZHVsZXMvam9icy9qb2JzLmRpci5qcyIsIm1vZHVsZXMvam9icy9qb2JzLnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5zdmMuanMiLCJtb2R1bGVzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLmN0cmwuY29mZmVlIiwibW9kdWxlcy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5jdHJsLmpzIiwibW9kdWxlcy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5zdmMuY29mZmVlIiwibW9kdWxlcy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5zdmMuanMiLCJtb2R1bGVzL3N1Ym1pdC9zdWJtaXQuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL3N1Ym1pdC9zdWJtaXQuY3RybC5qcyIsIm1vZHVsZXMvc3VibWl0L3N1Ym1pdC5zdmMuY29mZmVlIiwibW9kdWxlcy9zdWJtaXQvc3VibWl0LnN2Yy5qcyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFrQkEsUUFBUSxPQUFPLFlBQVksQ0FBQyxhQUFhLGtCQUl4QyxtQkFBSSxTQUFDLFlBQUQ7RUFDSCxXQUFXLGlCQUFpQjtFQ3JCNUIsT0RzQkEsV0FBVyxjQUFjLFdBQUE7SUFDdkIsV0FBVyxpQkFBaUIsQ0FBQyxXQUFXO0lDckJ4QyxPRHNCQSxXQUFXLGVBQWU7O0lBSTdCLE1BQU0sZUFBZTtFQUNwQixXQUFXO0VBRVgsb0JBQW9CO0dBS3JCLCtEQUFJLFNBQUMsYUFBYSxhQUFhLGFBQWEsV0FBeEM7RUM1QkgsT0Q2QkEsWUFBWSxhQUFhLEtBQUssU0FBQyxRQUFEO0lBQzVCLFFBQVEsT0FBTyxhQUFhO0lBRTVCLFlBQVk7SUM3QlosT0QrQkEsVUFBVSxXQUFBO01DOUJSLE9EK0JBLFlBQVk7T0FDWixZQUFZOztJQUtqQixpQ0FBTyxTQUFDLHVCQUFEO0VDakNOLE9Ea0NBLHNCQUFzQjtJQUl2QixnREFBTyxTQUFDLGdCQUFnQixvQkFBakI7RUFDTixlQUFlLE1BQU0sWUFDbkI7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLE1BQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sZ0JBQ0w7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLE1BQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sa0JBQ0w7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLE1BQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sY0FDTDtJQUFBLEtBQUs7SUFDTCxVQUFVO0lBQ1YsT0FDRTtNQUFBLE1BQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sbUJBQ0w7SUFBQSxLQUFLO0lBQ0wsVUFBVTtJQUNWLE9BQ0U7TUFBQSxTQUNFO1FBQUEsYUFBYTtRQUNiLFlBQVk7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dGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnLCBbJ3VpLnJvdXRlcicsICdhbmd1bGFyTW9tZW50J10pXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLnJ1biAoJHJvb3RTY29wZSkgLT5cbiAgJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZSA9IGZhbHNlXG4gICRyb290U2NvcGUuc2hvd1NpZGViYXIgPSAtPlxuICAgICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSAhJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZVxuICAgICRyb290U2NvcGUuc2lkZWJhckNsYXNzID0gJ2ZvcmNlLXNob3cnXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLnZhbHVlICdmbGlua0NvbmZpZycsIHtcbiAgam9iU2VydmVyOiAnJ1xuIyBqb2JTZXJ2ZXI6ICdodHRwOi8vbG9jYWxob3N0OjgwODEvJ1xuICBcInJlZnJlc2gtaW50ZXJ2YWxcIjogMTAwMDBcbn1cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4ucnVuIChKb2JzU2VydmljZSwgTWFpblNlcnZpY2UsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIC0+XG4gIE1haW5TZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuIChjb25maWcpIC0+XG4gICAgYW5ndWxhci5leHRlbmQgZmxpbmtDb25maWcsIGNvbmZpZ1xuXG4gICAgSm9ic1NlcnZpY2UubGlzdEpvYnMoKVxuXG4gICAgJGludGVydmFsIC0+XG4gICAgICBKb2JzU2VydmljZS5saXN0Sm9icygpXG4gICAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cblxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb25maWcgKCR1aVZpZXdTY3JvbGxQcm92aWRlcikgLT5cbiAgJHVpVmlld1Njcm9sbFByb3ZpZGVyLnVzZUFuY2hvclNjcm9sbCgpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbmZpZyAoJHN0YXRlUHJvdmlkZXIsICR1cmxSb3V0ZXJQcm92aWRlcikgLT5cbiAgJHN0YXRlUHJvdmlkZXIuc3RhdGUgXCJvdmVydmlld1wiLFxuICAgIHVybDogXCIvb3ZlcnZpZXdcIlxuICAgIHZpZXdzOlxuICAgICAgbWFpbjpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvb3ZlcnZpZXcuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdPdmVydmlld0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwicnVubmluZy1qb2JzXCIsXG4gICAgdXJsOiBcIi9ydW5uaW5nLWpvYnNcIlxuICAgIHZpZXdzOlxuICAgICAgbWFpbjpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9ydW5uaW5nLWpvYnMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdSdW5uaW5nSm9ic0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwiY29tcGxldGVkLWpvYnNcIixcbiAgICB1cmw6IFwiL2NvbXBsZXRlZC1qb2JzXCJcbiAgICB2aWV3czpcbiAgICAgIG1haW46XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvY29tcGxldGVkLWpvYnMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iXCIsXG4gICAgdXJsOiBcIi9qb2JzL3tqb2JpZH1cIlxuICAgIGFic3RyYWN0OiB0cnVlXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZUpvYkNvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuXCIsXG4gICAgdXJsOiBcIlwiXG4gICAgYWJzdHJhY3Q6IHRydWVcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4uaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uc3VidGFza3NcIixcbiAgICB1cmw6IFwiXCJcbiAgICB2aWV3czpcbiAgICAgICdub2RlLWRldGFpbHMnOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5zdWJ0YXNrcy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5TdWJ0YXNrc0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLnRhc2ttYW5hZ2Vyc1wiLFxuICAgIHVybDogXCIvdGFza21hbmFnZXJzXCJcbiAgICB2aWV3czpcbiAgICAgICdub2RlLWRldGFpbHMnOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC50YXNrbWFuYWdlcnMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuVGFza01hbmFnZXJzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uYWNjdW11bGF0b3JzXCIsXG4gICAgdXJsOiBcIi9hY2N1bXVsYXRvcnNcIlxuICAgIHZpZXdzOlxuICAgICAgJ25vZGUtZGV0YWlscyc6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmFjY3VtdWxhdG9ycy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi5jaGVja3BvaW50c1wiLFxuICAgIHVybDogXCIvY2hlY2twb2ludHNcIlxuICAgIHZpZXdzOlxuICAgICAgJ25vZGUtZGV0YWlscyc6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmNoZWNrcG9pbnRzLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uYmFja3ByZXNzdXJlXCIsXG4gICAgdXJsOiBcIi9iYWNrcHJlc3N1cmVcIlxuICAgIHZpZXdzOlxuICAgICAgJ25vZGUtZGV0YWlscyc6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmJhY2twcmVzc3VyZS5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5CYWNrUHJlc3N1cmVDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IudGltZWxpbmVcIixcbiAgICB1cmw6IFwiL3RpbWVsaW5lXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLmh0bWxcIlxuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsXG4gICAgdXJsOiBcIi97dmVydGV4SWR9XCJcbiAgICB2aWV3czpcbiAgICAgIHZlcnRleDpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUudmVydGV4Lmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IuZXhjZXB0aW9uc1wiLFxuICAgIHVybDogXCIvZXhjZXB0aW9uc1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5leGNlcHRpb25zLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wcm9wZXJ0aWVzXCIsXG4gICAgdXJsOiBcIi9wcm9wZXJ0aWVzXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnByb3BlcnRpZXMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQcm9wZXJ0aWVzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLmNvbmZpZ1wiLFxuICAgIHVybDogXCIvY29uZmlnXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmNvbmZpZy5odG1sXCJcblxuICAuc3RhdGUgXCJhbGwtbWFuYWdlclwiLFxuICAgIHVybDogXCIvdGFza21hbmFnZXJzXCJcbiAgICB2aWV3czpcbiAgICAgIG1haW46XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL2luZGV4Lmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnQWxsVGFza01hbmFnZXJzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtbWFuYWdlclwiLFxuICAgICAgdXJsOiBcIi90YXNrbWFuYWdlci97dGFza21hbmFnZXJpZH1cIlxuICAgICAgdmlld3M6XG4gICAgICAgIG1haW46XG4gICAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvdGFza21hbmFnZXIvdGFza21hbmFnZXIuaHRtbFwiXG4gICAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZVRhc2tNYW5hZ2VyQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtbWFuYWdlci5tZXRyaWNzXCIsXG4gICAgdXJsOiBcIi9tZXRyaWNzXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLm1ldHJpY3MuaHRtbFwiXG5cbiAgLnN0YXRlIFwiam9ibWFuYWdlclwiLFxuICAgICAgdXJsOiBcIi9qb2JtYW5hZ2VyXCJcbiAgICAgIHZpZXdzOlxuICAgICAgICBtYWluOlxuICAgICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvaW5kZXguaHRtbFwiXG5cbiAgLnN0YXRlIFwiam9ibWFuYWdlci5jb25maWdcIixcbiAgICB1cmw6IFwiL2NvbmZpZ1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2NvbmZpZy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJDb25maWdDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcImpvYm1hbmFnZXIuc3Rkb3V0XCIsXG4gICAgdXJsOiBcIi9zdGRvdXRcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9zdGRvdXQuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JNYW5hZ2VyU3Rkb3V0Q29udHJvbGxlcidcblxuICAuc3RhdGUgXCJqb2JtYW5hZ2VyLmxvZ1wiLFxuICAgIHVybDogXCIvbG9nXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvbG9nLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iTWFuYWdlckxvZ3NDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInN1Ym1pdFwiLFxuICAgICAgdXJsOiBcIi9zdWJtaXRcIlxuICAgICAgdmlld3M6XG4gICAgICAgIG1haW46XG4gICAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvc3VibWl0Lmh0bWxcIlxuICAgICAgICAgIGNvbnRyb2xsZXI6IFwiSm9iU3VibWl0Q29udHJvbGxlclwiXG5cbiAgJHVybFJvdXRlclByb3ZpZGVyLm90aGVyd2lzZSBcIi9vdmVydmlld1wiXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnLCBbJ3VpLnJvdXRlcicsICdhbmd1bGFyTW9tZW50J10pLnJ1bihmdW5jdGlvbigkcm9vdFNjb3BlKSB7XG4gICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSBmYWxzZTtcbiAgcmV0dXJuICRyb290U2NvcGUuc2hvd1NpZGViYXIgPSBmdW5jdGlvbigpIHtcbiAgICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gISRyb290U2NvcGUuc2lkZWJhclZpc2libGU7XG4gICAgcmV0dXJuICRyb290U2NvcGUuc2lkZWJhckNsYXNzID0gJ2ZvcmNlLXNob3cnO1xuICB9O1xufSkudmFsdWUoJ2ZsaW5rQ29uZmlnJywge1xuICBqb2JTZXJ2ZXI6ICcnLFxuICBcInJlZnJlc2gtaW50ZXJ2YWxcIjogMTAwMDBcbn0pLnJ1bihmdW5jdGlvbihKb2JzU2VydmljZSwgTWFpblNlcnZpY2UsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIHtcbiAgcmV0dXJuIE1haW5TZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuKGZ1bmN0aW9uKGNvbmZpZykge1xuICAgIGFuZ3VsYXIuZXh0ZW5kKGZsaW5rQ29uZmlnLCBjb25maWcpO1xuICAgIEpvYnNTZXJ2aWNlLmxpc3RKb2JzKCk7XG4gICAgcmV0dXJuICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICAgIHJldHVybiBKb2JzU2VydmljZS5saXN0Sm9icygpO1xuICAgIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIH0pO1xufSkuY29uZmlnKGZ1bmN0aW9uKCR1aVZpZXdTY3JvbGxQcm92aWRlcikge1xuICByZXR1cm4gJHVpVmlld1Njcm9sbFByb3ZpZGVyLnVzZUFuY2hvclNjcm9sbCgpO1xufSkuY29uZmlnKGZ1bmN0aW9uKCRzdGF0ZVByb3ZpZGVyLCAkdXJsUm91dGVyUHJvdmlkZXIpIHtcbiAgJHN0YXRlUHJvdmlkZXIuc3RhdGUoXCJvdmVydmlld1wiLCB7XG4gICAgdXJsOiBcIi9vdmVydmlld1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL292ZXJ2aWV3Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ092ZXJ2aWV3Q29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwicnVubmluZy1qb2JzXCIsIHtcbiAgICB1cmw6IFwiL3J1bm5pbmctam9ic1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvcnVubmluZy1qb2JzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1J1bm5pbmdKb2JzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiY29tcGxldGVkLWpvYnNcIiwge1xuICAgIHVybDogXCIvY29tcGxldGVkLWpvYnNcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2NvbXBsZXRlZC1qb2JzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iXCIsIHtcbiAgICB1cmw6IFwiL2pvYnMve2pvYmlkfVwiLFxuICAgIGFic3RyYWN0OiB0cnVlLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZUpvYkNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhblwiLCB7XG4gICAgdXJsOiBcIlwiLFxuICAgIGFic3RyYWN0OiB0cnVlLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4uaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5zdWJ0YXNrc1wiLCB7XG4gICAgdXJsOiBcIlwiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnbm9kZS1kZXRhaWxzJzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5zdWJ0YXNrcy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuU3VidGFza3NDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4udGFza21hbmFnZXJzXCIsIHtcbiAgICB1cmw6IFwiL3Rhc2ttYW5hZ2Vyc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnbm9kZS1kZXRhaWxzJzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC50YXNrbWFuYWdlcnMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhblRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5hY2N1bXVsYXRvcnNcIiwge1xuICAgIHVybDogXCIvYWNjdW11bGF0b3JzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdub2RlLWRldGFpbHMnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmFjY3VtdWxhdG9ycy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLmNoZWNrcG9pbnRzXCIsIHtcbiAgICB1cmw6IFwiL2NoZWNrcG9pbnRzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdub2RlLWRldGFpbHMnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmNoZWNrcG9pbnRzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5DaGVja3BvaW50c0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5iYWNrcHJlc3N1cmVcIiwge1xuICAgIHVybDogXCIvYmFja3ByZXNzdXJlXCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdub2RlLWRldGFpbHMnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmJhY2twcmVzc3VyZS5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQmFja1ByZXNzdXJlQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi50aW1lbGluZVwiLCB7XG4gICAgdXJsOiBcIi90aW1lbGluZVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLCB7XG4gICAgdXJsOiBcIi97dmVydGV4SWR9XCIsXG4gICAgdmlld3M6IHtcbiAgICAgIHZlcnRleDoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS52ZXJ0ZXguaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLmV4Y2VwdGlvbnNcIiwge1xuICAgIHVybDogXCIvZXhjZXB0aW9uc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmV4Y2VwdGlvbnMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucHJvcGVydGllc1wiLCB7XG4gICAgdXJsOiBcIi9wcm9wZXJ0aWVzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucHJvcGVydGllcy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQcm9wZXJ0aWVzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5jb25maWdcIiwge1xuICAgIHVybDogXCIvY29uZmlnXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuY29uZmlnLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJhbGwtbWFuYWdlclwiLCB7XG4gICAgdXJsOiBcIi90YXNrbWFuYWdlcnNcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci9pbmRleC5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdBbGxUYXNrTWFuYWdlcnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtbWFuYWdlclwiLCB7XG4gICAgdXJsOiBcIi90YXNrbWFuYWdlci97dGFza21hbmFnZXJpZH1cIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVUYXNrTWFuYWdlckNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1tYW5hZ2VyLm1ldHJpY3NcIiwge1xuICAgIHVybDogXCIvbWV0cmljc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLm1ldHJpY3MuaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImpvYm1hbmFnZXJcIiwge1xuICAgIHVybDogXCIvam9ibWFuYWdlclwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvaW5kZXguaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImpvYm1hbmFnZXIuY29uZmlnXCIsIHtcbiAgICB1cmw6IFwiL2NvbmZpZ1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvY29uZmlnLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJDb25maWdDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJqb2JtYW5hZ2VyLnN0ZG91dFwiLCB7XG4gICAgdXJsOiBcIi9zdGRvdXRcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL3N0ZG91dC5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JNYW5hZ2VyU3Rkb3V0Q29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiam9ibWFuYWdlci5sb2dcIiwge1xuICAgIHVybDogXCIvbG9nXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9sb2cuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iTWFuYWdlckxvZ3NDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzdWJtaXRcIiwge1xuICAgIHVybDogXCIvc3VibWl0XCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvc3VibWl0Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogXCJKb2JTdWJtaXRDb250cm9sbGVyXCJcbiAgICAgIH1cbiAgICB9XG4gIH0pO1xuICByZXR1cm4gJHVybFJvdXRlclByb3ZpZGVyLm90aGVyd2lzZShcIi9vdmVydmlld1wiKTtcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uZGlyZWN0aXZlICdic0xhYmVsJywgKEpvYnNTZXJ2aWNlKSAtPlxuICB0cmFuc2NsdWRlOiB0cnVlXG4gIHJlcGxhY2U6IHRydWVcbiAgc2NvcGU6IFxuICAgIGdldExhYmVsQ2xhc3M6IFwiJlwiXG4gICAgc3RhdHVzOiBcIkBcIlxuXG4gIHRlbXBsYXRlOiBcIjxzcGFuIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJz48bmctdHJhbnNjbHVkZT48L25nLXRyYW5zY2x1ZGU+PC9zcGFuPlwiXG4gIFxuICBsaW5rOiAoc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSAtPlxuICAgIHNjb3BlLmdldExhYmVsQ2xhc3MgPSAtPlxuICAgICAgJ2xhYmVsIGxhYmVsLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ2JwTGFiZWwnLCAoSm9ic1NlcnZpY2UpIC0+XG4gIHRyYW5zY2x1ZGU6IHRydWVcbiAgcmVwbGFjZTogdHJ1ZVxuICBzY29wZTpcbiAgICBnZXRCYWNrUHJlc3N1cmVMYWJlbENsYXNzOiBcIiZcIlxuICAgIHN0YXR1czogXCJAXCJcblxuICB0ZW1wbGF0ZTogXCI8c3BhbiB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldEJhY2tQcmVzc3VyZUxhYmVsQ2xhc3MoKSc+PG5nLXRyYW5zY2x1ZGU+PC9uZy10cmFuc2NsdWRlPjwvc3Bhbj5cIlxuXG4gIGxpbms6IChzY29wZSwgZWxlbWVudCwgYXR0cnMpIC0+XG4gICAgc2NvcGUuZ2V0QmFja1ByZXNzdXJlTGFiZWxDbGFzcyA9IC0+XG4gICAgICAnbGFiZWwgbGFiZWwtJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUJhY2tQcmVzc3VyZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAnaW5kaWNhdG9yUHJpbWFyeScsIChKb2JzU2VydmljZSkgLT5cbiAgcmVwbGFjZTogdHJ1ZVxuICBzY29wZTogXG4gICAgZ2V0TGFiZWxDbGFzczogXCImXCJcbiAgICBzdGF0dXM6ICdAJ1xuXG4gIHRlbXBsYXRlOiBcIjxpIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJyAvPlwiXG4gIFxuICBsaW5rOiAoc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSAtPlxuICAgIHNjb3BlLmdldExhYmVsQ2xhc3MgPSAtPlxuICAgICAgJ2ZhIGZhLWNpcmNsZSBpbmRpY2F0b3IgaW5kaWNhdG9yLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ3RhYmxlUHJvcGVydHknLCAtPlxuICByZXBsYWNlOiB0cnVlXG4gIHNjb3BlOlxuICAgIHZhbHVlOiAnPSdcblxuICB0ZW1wbGF0ZTogXCI8dGQgdGl0bGU9XFxcInt7dmFsdWUgfHwgJ05vbmUnfX1cXFwiPnt7dmFsdWUgfHwgJ05vbmUnfX08L3RkPlwiXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5kaXJlY3RpdmUoJ2JzTGFiZWwnLCBmdW5jdGlvbihKb2JzU2VydmljZSkge1xuICByZXR1cm4ge1xuICAgIHRyYW5zY2x1ZGU6IHRydWUsXG4gICAgcmVwbGFjZTogdHJ1ZSxcbiAgICBzY29wZToge1xuICAgICAgZ2V0TGFiZWxDbGFzczogXCImXCIsXG4gICAgICBzdGF0dXM6IFwiQFwiXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8c3BhbiB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKSc+PG5nLXRyYW5zY2x1ZGU+PC9uZy10cmFuc2NsdWRlPjwvc3Bhbj5cIixcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbWVudCwgYXR0cnMpIHtcbiAgICAgIHJldHVybiBzY29wZS5nZXRMYWJlbENsYXNzID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJldHVybiAnbGFiZWwgbGFiZWwtJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKTtcbiAgICAgIH07XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCdicExhYmVsJywgZnVuY3Rpb24oSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIHtcbiAgICB0cmFuc2NsdWRlOiB0cnVlLFxuICAgIHJlcGxhY2U6IHRydWUsXG4gICAgc2NvcGU6IHtcbiAgICAgIGdldEJhY2tQcmVzc3VyZUxhYmVsQ2xhc3M6IFwiJlwiLFxuICAgICAgc3RhdHVzOiBcIkBcIlxuICAgIH0sXG4gICAgdGVtcGxhdGU6IFwiPHNwYW4gdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRCYWNrUHJlc3N1cmVMYWJlbENsYXNzKCknPjxuZy10cmFuc2NsdWRlPjwvbmctdHJhbnNjbHVkZT48L3NwYW4+XCIsXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSB7XG4gICAgICByZXR1cm4gc2NvcGUuZ2V0QmFja1ByZXNzdXJlTGFiZWxDbGFzcyA9IGZ1bmN0aW9uKCkge1xuICAgICAgICByZXR1cm4gJ2xhYmVsIGxhYmVsLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVCYWNrUHJlc3N1cmVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cyk7XG4gICAgICB9O1xuICAgIH1cbiAgfTtcbn0pLmRpcmVjdGl2ZSgnaW5kaWNhdG9yUHJpbWFyeScsIGZ1bmN0aW9uKEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiB7XG4gICAgcmVwbGFjZTogdHJ1ZSxcbiAgICBzY29wZToge1xuICAgICAgZ2V0TGFiZWxDbGFzczogXCImXCIsXG4gICAgICBzdGF0dXM6ICdAJ1xuICAgIH0sXG4gICAgdGVtcGxhdGU6IFwiPGkgdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRMYWJlbENsYXNzKCknIC8+XCIsXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSB7XG4gICAgICByZXR1cm4gc2NvcGUuZ2V0TGFiZWxDbGFzcyA9IGZ1bmN0aW9uKCkge1xuICAgICAgICByZXR1cm4gJ2ZhIGZhLWNpcmNsZSBpbmRpY2F0b3IgaW5kaWNhdG9yLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cyk7XG4gICAgICB9O1xuICAgIH1cbiAgfTtcbn0pLmRpcmVjdGl2ZSgndGFibGVQcm9wZXJ0eScsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4ge1xuICAgIHJlcGxhY2U6IHRydWUsXG4gICAgc2NvcGU6IHtcbiAgICAgIHZhbHVlOiAnPSdcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjx0ZCB0aXRsZT1cXFwie3t2YWx1ZSB8fCAnTm9uZSd9fVxcXCI+e3t2YWx1ZSB8fCAnTm9uZSd9fTwvdGQ+XCJcbiAgfTtcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5maWx0ZXIgXCJhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRcIiwgKGFuZ3VsYXJNb21lbnRDb25maWcpIC0+XG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlciA9ICh2YWx1ZSwgZm9ybWF0LCBkdXJhdGlvbkZvcm1hdCkgLT5cbiAgICByZXR1cm4gXCJcIiAgaWYgdHlwZW9mIHZhbHVlIGlzIFwidW5kZWZpbmVkXCIgb3IgdmFsdWUgaXMgbnVsbFxuXG4gICAgbW9tZW50LmR1cmF0aW9uKHZhbHVlLCBmb3JtYXQpLmZvcm1hdChkdXJhdGlvbkZvcm1hdCwgeyB0cmltOiBmYWxzZSB9KVxuXG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlci4kc3RhdGVmdWwgPSBhbmd1bGFyTW9tZW50Q29uZmlnLnN0YXRlZnVsRmlsdGVyc1xuXG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlclxuXG4uZmlsdGVyIFwiaHVtYW5pemVEdXJhdGlvblwiLCAtPlxuICAodmFsdWUsIHNob3J0KSAtPlxuICAgIHJldHVybiBcIlwiIGlmIHR5cGVvZiB2YWx1ZSBpcyBcInVuZGVmaW5lZFwiIG9yIHZhbHVlIGlzIG51bGxcbiAgICBtcyA9IHZhbHVlICUgMTAwMFxuICAgIHggPSBNYXRoLmZsb29yKHZhbHVlIC8gMTAwMClcbiAgICBzZWNvbmRzID0geCAlIDYwXG4gICAgeCA9IE1hdGguZmxvb3IoeCAvIDYwKVxuICAgIG1pbnV0ZXMgPSB4ICUgNjBcbiAgICB4ID0gTWF0aC5mbG9vcih4IC8gNjApXG4gICAgaG91cnMgPSB4ICUgMjRcbiAgICB4ID0gTWF0aC5mbG9vcih4IC8gMjQpXG4gICAgZGF5cyA9IHhcbiAgICBpZiBkYXlzID09IDBcbiAgICAgIGlmIGhvdXJzID09IDBcbiAgICAgICAgaWYgbWludXRlcyA9PSAwXG4gICAgICAgICAgaWYgc2Vjb25kcyA9PSAwXG4gICAgICAgICAgICByZXR1cm4gbXMgKyBcIm1zXCJcbiAgICAgICAgICBlbHNlXG4gICAgICAgICAgICByZXR1cm4gc2Vjb25kcyArIFwicyBcIlxuICAgICAgICBlbHNlXG4gICAgICAgICAgcmV0dXJuIG1pbnV0ZXMgKyBcIm0gXCIgKyBzZWNvbmRzICsgXCJzXCJcbiAgICAgIGVsc2VcbiAgICAgICAgaWYgc2hvcnQgdGhlbiByZXR1cm4gaG91cnMgKyBcImggXCIgKyBtaW51dGVzICsgXCJtXCIgZWxzZSByZXR1cm4gaG91cnMgKyBcImggXCIgKyBtaW51dGVzICsgXCJtIFwiICsgc2Vjb25kcyArIFwic1wiXG4gICAgZWxzZVxuICAgICAgaWYgc2hvcnQgdGhlbiByZXR1cm4gZGF5cyArIFwiZCBcIiArIGhvdXJzICsgXCJoXCIgZWxzZSByZXR1cm4gZGF5cyArIFwiZCBcIiArIGhvdXJzICsgXCJoIFwiICsgbWludXRlcyArIFwibSBcIiArIHNlY29uZHMgKyBcInNcIlxuXG4uZmlsdGVyIFwiaHVtYW5pemVUZXh0XCIsIC0+XG4gICh0ZXh0KSAtPlxuICAgICMgVE9ETzogZXh0ZW5kLi4uIGEgbG90XG4gICAgaWYgdGV4dCB0aGVuIHRleHQucmVwbGFjZSgvJmd0Oy9nLCBcIj5cIikucmVwbGFjZSgvPGJyXFwvPi9nLFwiXCIpIGVsc2UgJydcblxuLmZpbHRlciBcImh1bWFuaXplQnl0ZXNcIiwgLT5cbiAgKGJ5dGVzKSAtPlxuICAgIHVuaXRzID0gW1wiQlwiLCBcIktCXCIsIFwiTUJcIiwgXCJHQlwiLCBcIlRCXCIsIFwiUEJcIiwgXCJFQlwiXVxuICAgIGNvbnZlcnRlciA9ICh2YWx1ZSwgcG93ZXIpIC0+XG4gICAgICBiYXNlID0gTWF0aC5wb3coMTAyNCwgcG93ZXIpXG4gICAgICBpZiB2YWx1ZSA8IGJhc2VcbiAgICAgICAgcmV0dXJuICh2YWx1ZSAvIGJhc2UpLnRvRml4ZWQoMikgKyBcIiBcIiArIHVuaXRzW3Bvd2VyXVxuICAgICAgZWxzZSBpZiB2YWx1ZSA8IGJhc2UgKiAxMDAwXG4gICAgICAgIHJldHVybiAodmFsdWUgLyBiYXNlKS50b1ByZWNpc2lvbigzKSArIFwiIFwiICsgdW5pdHNbcG93ZXJdXG4gICAgICBlbHNlXG4gICAgICAgIHJldHVybiBjb252ZXJ0ZXIodmFsdWUsIHBvd2VyICsgMSlcbiAgICByZXR1cm4gXCJcIiBpZiB0eXBlb2YgYnl0ZXMgaXMgXCJ1bmRlZmluZWRcIiBvciBieXRlcyBpcyBudWxsXG4gICAgaWYgYnl0ZXMgPCAxMDAwIHRoZW4gYnl0ZXMgKyBcIiBCXCIgZWxzZSBjb252ZXJ0ZXIoYnl0ZXMsIDEpXG5cbi5maWx0ZXIgXCJ0b1VwcGVyQ2FzZVwiLCAtPlxuICAodGV4dCkgLT4gdGV4dC50b1VwcGVyQ2FzZSgpXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5maWx0ZXIoXCJhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRcIiwgZnVuY3Rpb24oYW5ndWxhck1vbWVudENvbmZpZykge1xuICB2YXIgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyO1xuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIgPSBmdW5jdGlvbih2YWx1ZSwgZm9ybWF0LCBkdXJhdGlvbkZvcm1hdCkge1xuICAgIGlmICh0eXBlb2YgdmFsdWUgPT09IFwidW5kZWZpbmVkXCIgfHwgdmFsdWUgPT09IG51bGwpIHtcbiAgICAgIHJldHVybiBcIlwiO1xuICAgIH1cbiAgICByZXR1cm4gbW9tZW50LmR1cmF0aW9uKHZhbHVlLCBmb3JtYXQpLmZvcm1hdChkdXJhdGlvbkZvcm1hdCwge1xuICAgICAgdHJpbTogZmFsc2VcbiAgICB9KTtcbiAgfTtcbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyLiRzdGF0ZWZ1bCA9IGFuZ3VsYXJNb21lbnRDb25maWcuc3RhdGVmdWxGaWx0ZXJzO1xuICByZXR1cm4gYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyO1xufSkuZmlsdGVyKFwiaHVtYW5pemVEdXJhdGlvblwiLCBmdW5jdGlvbigpIHtcbiAgcmV0dXJuIGZ1bmN0aW9uKHZhbHVlLCBzaG9ydCkge1xuICAgIHZhciBkYXlzLCBob3VycywgbWludXRlcywgbXMsIHNlY29uZHMsIHg7XG4gICAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gXCJ1bmRlZmluZWRcIiB8fCB2YWx1ZSA9PT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFwiXCI7XG4gICAgfVxuICAgIG1zID0gdmFsdWUgJSAxMDAwO1xuICAgIHggPSBNYXRoLmZsb29yKHZhbHVlIC8gMTAwMCk7XG4gICAgc2Vjb25kcyA9IHggJSA2MDtcbiAgICB4ID0gTWF0aC5mbG9vcih4IC8gNjApO1xuICAgIG1pbnV0ZXMgPSB4ICUgNjA7XG4gICAgeCA9IE1hdGguZmxvb3IoeCAvIDYwKTtcbiAgICBob3VycyA9IHggJSAyNDtcbiAgICB4ID0gTWF0aC5mbG9vcih4IC8gMjQpO1xuICAgIGRheXMgPSB4O1xuICAgIGlmIChkYXlzID09PSAwKSB7XG4gICAgICBpZiAoaG91cnMgPT09IDApIHtcbiAgICAgICAgaWYgKG1pbnV0ZXMgPT09IDApIHtcbiAgICAgICAgICBpZiAoc2Vjb25kcyA9PT0gMCkge1xuICAgICAgICAgICAgcmV0dXJuIG1zICsgXCJtc1wiO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICByZXR1cm4gc2Vjb25kcyArIFwicyBcIjtcbiAgICAgICAgICB9XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuIG1pbnV0ZXMgKyBcIm0gXCIgKyBzZWNvbmRzICsgXCJzXCI7XG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGlmIChzaG9ydCkge1xuICAgICAgICAgIHJldHVybiBob3VycyArIFwiaCBcIiArIG1pbnV0ZXMgKyBcIm1cIjtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gaG91cnMgKyBcImggXCIgKyBtaW51dGVzICsgXCJtIFwiICsgc2Vjb25kcyArIFwic1wiO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIGlmIChzaG9ydCkge1xuICAgICAgICByZXR1cm4gZGF5cyArIFwiZCBcIiArIGhvdXJzICsgXCJoXCI7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICByZXR1cm4gZGF5cyArIFwiZCBcIiArIGhvdXJzICsgXCJoIFwiICsgbWludXRlcyArIFwibSBcIiArIHNlY29uZHMgKyBcInNcIjtcbiAgICAgIH1cbiAgICB9XG4gIH07XG59KS5maWx0ZXIoXCJodW1hbml6ZVRleHRcIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbih0ZXh0KSB7XG4gICAgaWYgKHRleHQpIHtcbiAgICAgIHJldHVybiB0ZXh0LnJlcGxhY2UoLyZndDsvZywgXCI+XCIpLnJlcGxhY2UoLzxiclxcLz4vZywgXCJcIik7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiAnJztcbiAgICB9XG4gIH07XG59KS5maWx0ZXIoXCJodW1hbml6ZUJ5dGVzXCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24oYnl0ZXMpIHtcbiAgICB2YXIgY29udmVydGVyLCB1bml0cztcbiAgICB1bml0cyA9IFtcIkJcIiwgXCJLQlwiLCBcIk1CXCIsIFwiR0JcIiwgXCJUQlwiLCBcIlBCXCIsIFwiRUJcIl07XG4gICAgY29udmVydGVyID0gZnVuY3Rpb24odmFsdWUsIHBvd2VyKSB7XG4gICAgICB2YXIgYmFzZTtcbiAgICAgIGJhc2UgPSBNYXRoLnBvdygxMDI0LCBwb3dlcik7XG4gICAgICBpZiAodmFsdWUgPCBiYXNlKSB7XG4gICAgICAgIHJldHVybiAodmFsdWUgLyBiYXNlKS50b0ZpeGVkKDIpICsgXCIgXCIgKyB1bml0c1twb3dlcl07XG4gICAgICB9IGVsc2UgaWYgKHZhbHVlIDwgYmFzZSAqIDEwMDApIHtcbiAgICAgICAgcmV0dXJuICh2YWx1ZSAvIGJhc2UpLnRvUHJlY2lzaW9uKDMpICsgXCIgXCIgKyB1bml0c1twb3dlcl07XG4gICAgICB9IGVsc2Uge1xuICAgICAgICByZXR1cm4gY29udmVydGVyKHZhbHVlLCBwb3dlciArIDEpO1xuICAgICAgfVxuICAgIH07XG4gICAgaWYgKHR5cGVvZiBieXRlcyA9PT0gXCJ1bmRlZmluZWRcIiB8fCBieXRlcyA9PT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFwiXCI7XG4gICAgfVxuICAgIGlmIChieXRlcyA8IDEwMDApIHtcbiAgICAgIHJldHVybiBieXRlcyArIFwiIEJcIjtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIGNvbnZlcnRlcihieXRlcywgMSk7XG4gICAgfVxuICB9O1xufSkuZmlsdGVyKFwidG9VcHBlckNhc2VcIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbih0ZXh0KSB7XG4gICAgcmV0dXJuIHRleHQudG9VcHBlckNhc2UoKTtcbiAgfTtcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5zZXJ2aWNlICdNYWluU2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxuICBAbG9hZENvbmZpZyA9IC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJjb25maWdcIlxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuXG4gIEBcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ01haW5TZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkge1xuICB0aGlzLmxvYWRDb25maWcgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImNvbmZpZ1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uY29udHJvbGxlciAnT3ZlcnZpZXdDb250cm9sbGVyJywgKCRzY29wZSwgT3ZlcnZpZXdTZXJ2aWNlLCBKb2JzU2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZykgLT5cbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gLT5cbiAgICAkc2NvcGUucnVubmluZ0pvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJylcbiAgICAkc2NvcGUuZmluaXNoZWRKb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKVxuXG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcblxuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKVxuXG4gIE92ZXJ2aWV3U2VydmljZS5sb2FkT3ZlcnZpZXcoKS50aGVuIChkYXRhKSAtPlxuICAgICRzY29wZS5vdmVydmlldyA9IGRhdGFcblxuICByZWZyZXNoID0gJGludGVydmFsIC0+XG4gICAgT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhXG4gICwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdXG5cbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaClcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ092ZXJ2aWV3Q29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgT3ZlcnZpZXdTZXJ2aWNlLCBKb2JzU2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZykge1xuICB2YXIgcmVmcmVzaDtcbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gZnVuY3Rpb24oKSB7XG4gICAgJHNjb3BlLnJ1bm5pbmdKb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygncnVubmluZycpO1xuICAgIHJldHVybiAkc2NvcGUuZmluaXNoZWRKb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKTtcbiAgfTtcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgfSk7XG4gICRzY29wZS5qb2JPYnNlcnZlcigpO1xuICBPdmVydmlld1NlcnZpY2UubG9hZE92ZXJ2aWV3KCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5vdmVydmlldyA9IGRhdGE7XG4gIH0pO1xuICByZWZyZXNoID0gJGludGVydmFsKGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBPdmVydmlld1NlcnZpY2UubG9hZE92ZXJ2aWV3KCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLm92ZXJ2aWV3ID0gZGF0YTtcbiAgICB9KTtcbiAgfSwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdKTtcbiAgcmV0dXJuICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaCk7XG4gIH0pO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLnNlcnZpY2UgJ092ZXJ2aWV3U2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxuICBvdmVydmlldyA9IHt9XG5cbiAgQGxvYWRPdmVydmlldyA9IC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJvdmVydmlld1wiKVxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIG92ZXJ2aWV3ID0gZGF0YVxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ092ZXJ2aWV3U2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdmFyIG92ZXJ2aWV3O1xuICBvdmVydmlldyA9IHt9O1xuICB0aGlzLmxvYWRPdmVydmlldyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwib3ZlcnZpZXdcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgb3ZlcnZpZXcgPSBkYXRhO1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLmNvbnRyb2xsZXIgJ0pvYk1hbmFnZXJDb25maWdDb250cm9sbGVyJywgKCRzY29wZSwgSm9iTWFuYWdlckNvbmZpZ1NlcnZpY2UpIC0+XG4gIEpvYk1hbmFnZXJDb25maWdTZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuIChkYXRhKSAtPlxuICAgIGlmICEkc2NvcGUuam9ibWFuYWdlcj9cbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge31cbiAgICAkc2NvcGUuam9ibWFuYWdlclsnY29uZmlnJ10gPSBkYXRhXG5cbi5jb250cm9sbGVyICdKb2JNYW5hZ2VyTG9nc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JNYW5hZ2VyTG9nc1NlcnZpY2UpIC0+XG4gIEpvYk1hbmFnZXJMb2dzU2VydmljZS5sb2FkTG9ncygpLnRoZW4gKGRhdGEpIC0+XG4gICAgaWYgISRzY29wZS5qb2JtYW5hZ2VyP1xuICAgICAgJHNjb3BlLmpvYm1hbmFnZXIgPSB7fVxuICAgICRzY29wZS5qb2JtYW5hZ2VyWydsb2cnXSA9IGRhdGFcblxuICAkc2NvcGUucmVsb2FkRGF0YSA9ICgpIC0+XG4gICAgSm9iTWFuYWdlckxvZ3NTZXJ2aWNlLmxvYWRMb2dzKCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyWydsb2cnXSA9IGRhdGFcblxuLmNvbnRyb2xsZXIgJ0pvYk1hbmFnZXJTdGRvdXRDb250cm9sbGVyJywgKCRzY29wZSwgSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UpIC0+XG4gIEpvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlLmxvYWRTdGRvdXQoKS50aGVuIChkYXRhKSAtPlxuICAgIGlmICEkc2NvcGUuam9ibWFuYWdlcj9cbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge31cbiAgICAkc2NvcGUuam9ibWFuYWdlclsnc3Rkb3V0J10gPSBkYXRhXG5cbiAgJHNjb3BlLnJlbG9hZERhdGEgPSAoKSAtPlxuICAgIEpvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlLmxvYWRTdGRvdXQoKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLmpvYm1hbmFnZXJbJ3N0ZG91dCddID0gZGF0YVxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignSm9iTWFuYWdlckNvbmZpZ0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYk1hbmFnZXJDb25maWdTZXJ2aWNlKSB7XG4gIHJldHVybiBKb2JNYW5hZ2VyQ29uZmlnU2VydmljZS5sb2FkQ29uZmlnKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgaWYgKCRzY29wZS5qb2JtYW5hZ2VyID09IG51bGwpIHtcbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge307XG4gICAgfVxuICAgIHJldHVybiAkc2NvcGUuam9ibWFuYWdlclsnY29uZmlnJ10gPSBkYXRhO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYk1hbmFnZXJMb2dzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9iTWFuYWdlckxvZ3NTZXJ2aWNlKSB7XG4gIEpvYk1hbmFnZXJMb2dzU2VydmljZS5sb2FkTG9ncygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIGlmICgkc2NvcGUuam9ibWFuYWdlciA9PSBudWxsKSB7XG4gICAgICAkc2NvcGUuam9ibWFuYWdlciA9IHt9O1xuICAgIH1cbiAgICByZXR1cm4gJHNjb3BlLmpvYm1hbmFnZXJbJ2xvZyddID0gZGF0YTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUucmVsb2FkRGF0YSA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JNYW5hZ2VyTG9nc1NlcnZpY2UubG9hZExvZ3MoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUuam9ibWFuYWdlclsnbG9nJ10gPSBkYXRhO1xuICAgIH0pO1xuICB9O1xufSkuY29udHJvbGxlcignSm9iTWFuYWdlclN0ZG91dENvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlKSB7XG4gIEpvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlLmxvYWRTdGRvdXQoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICBpZiAoJHNjb3BlLmpvYm1hbmFnZXIgPT0gbnVsbCkge1xuICAgICAgJHNjb3BlLmpvYm1hbmFnZXIgPSB7fTtcbiAgICB9XG4gICAgcmV0dXJuICRzY29wZS5qb2JtYW5hZ2VyWydzdGRvdXQnXSA9IGRhdGE7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLnJlbG9hZERhdGEgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UubG9hZFN0ZG91dCgpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5qb2JtYW5hZ2VyWydzdGRvdXQnXSA9IGRhdGE7XG4gICAgfSk7XG4gIH07XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnSm9iTWFuYWdlckNvbmZpZ1NlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cbiAgY29uZmlnID0ge31cblxuICBAbG9hZENvbmZpZyA9IC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JtYW5hZ2VyL2NvbmZpZ1wiKVxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIGNvbmZpZyA9IGRhdGFcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAXG5cbi5zZXJ2aWNlICdKb2JNYW5hZ2VyTG9nc1NlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cbiAgbG9ncyA9IHt9XG5cbiAgQGxvYWRMb2dzID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm1hbmFnZXIvbG9nXCIpXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgbG9ncyA9IGRhdGFcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAXG5cbi5zZXJ2aWNlICdKb2JNYW5hZ2VyU3Rkb3V0U2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxuICBzdGRvdXQgPSB7fVxuXG4gIEBsb2FkU3Rkb3V0ID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm1hbmFnZXIvc3Rkb3V0XCIpXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgc3Rkb3V0ID0gZGF0YVxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ0pvYk1hbmFnZXJDb25maWdTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkge1xuICB2YXIgY29uZmlnO1xuICBjb25maWcgPSB7fTtcbiAgdGhpcy5sb2FkQ29uZmlnID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JtYW5hZ2VyL2NvbmZpZ1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICBjb25maWcgPSBkYXRhO1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSkuc2VydmljZSgnSm9iTWFuYWdlckxvZ3NTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkge1xuICB2YXIgbG9ncztcbiAgbG9ncyA9IHt9O1xuICB0aGlzLmxvYWRMb2dzID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JtYW5hZ2VyL2xvZ1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICBsb2dzID0gZGF0YTtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pLnNlcnZpY2UoJ0pvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkge1xuICB2YXIgc3Rkb3V0O1xuICBzdGRvdXQgPSB7fTtcbiAgdGhpcy5sb2FkU3Rkb3V0ID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JtYW5hZ2VyL3N0ZG91dFwiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICBzdGRvdXQgPSBkYXRhO1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLmNvbnRyb2xsZXIgJ1J1bm5pbmdKb2JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gLT5cbiAgICAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKVxuXG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcblxuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gLT5cbiAgICAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJylcblxuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG5cbiAgJHNjb3BlLmpvYk9ic2VydmVyKClcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnU2luZ2xlSm9iQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSwgJHJvb3RTY29wZSwgZmxpbmtDb25maWcsICRpbnRlcnZhbCkgLT5cbiAgY29uc29sZS5sb2cgJ1NpbmdsZUpvYkNvbnRyb2xsZXInXG5cbiAgJHNjb3BlLmpvYmlkID0gJHN0YXRlUGFyYW1zLmpvYmlkXG4gICRzY29wZS5qb2IgPSBudWxsXG4gICRzY29wZS5wbGFuID0gbnVsbFxuICAkc2NvcGUudmVydGljZXMgPSBudWxsXG4gICRzY29wZS5qb2JDaGVja3BvaW50U3RhdHMgPSBudWxsXG4gICRzY29wZS5zaG93SGlzdG9yeSA9IGZhbHNlXG4gICRzY29wZS5iYWNrUHJlc3N1cmVPcGVyYXRvclN0YXRzID0ge31cblxuICBKb2JzU2VydmljZS5sb2FkSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUuam9iID0gZGF0YVxuICAgICRzY29wZS5wbGFuID0gZGF0YS5wbGFuXG4gICAgJHNjb3BlLnZlcnRpY2VzID0gZGF0YS52ZXJ0aWNlc1xuXG4gIHJlZnJlc2hlciA9ICRpbnRlcnZhbCAtPlxuICAgIEpvYnNTZXJ2aWNlLmxvYWRKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLmpvYiA9IGRhdGFcblxuICAgICAgJHNjb3BlLiRicm9hZGNhc3QgJ3JlbG9hZCdcblxuICAsIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXVxuXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICAkc2NvcGUuam9iID0gbnVsbFxuICAgICRzY29wZS5wbGFuID0gbnVsbFxuICAgICRzY29wZS52ZXJ0aWNlcyA9IG51bGxcbiAgICAkc2NvcGUuam9iQ2hlY2twb2ludFN0YXRzID0gbnVsbFxuICAgICRzY29wZS5iYWNrUHJlc3N1cmVPcGVyYXRvclN0YXRzID0gbnVsbFxuXG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoZXIpXG5cbiAgJHNjb3BlLmNhbmNlbEpvYiA9IChjYW5jZWxFdmVudCkgLT5cbiAgICBhbmd1bGFyLmVsZW1lbnQoY2FuY2VsRXZlbnQuY3VycmVudFRhcmdldCkucmVtb3ZlQ2xhc3MoXCJidG5cIikucmVtb3ZlQ2xhc3MoXCJidG4tZGVmYXVsdFwiKS5odG1sKCdDYW5jZWxsaW5nLi4uJylcbiAgICBKb2JzU2VydmljZS5jYW5jZWxKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuIChkYXRhKSAtPlxuICAgICAge31cblxuICAkc2NvcGUuc3RvcEpvYiA9IChzdG9wRXZlbnQpIC0+XG4gICAgYW5ndWxhci5lbGVtZW50KHN0b3BFdmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImJ0blwiKS5yZW1vdmVDbGFzcyhcImJ0bi1kZWZhdWx0XCIpLmh0bWwoJ1N0b3BwaW5nLi4uJylcbiAgICBKb2JzU2VydmljZS5zdG9wSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgIHt9XG5cbiAgJHNjb3BlLnRvZ2dsZUhpc3RvcnkgPSAtPlxuICAgICRzY29wZS5zaG93SGlzdG9yeSA9ICEkc2NvcGUuc2hvd0hpc3RvcnlcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iUGxhbkNvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XG4gIGNvbnNvbGUubG9nICdKb2JQbGFuQ29udHJvbGxlcidcblxuICAkc2NvcGUubm9kZWlkID0gbnVsbFxuICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2VcbiAgJHNjb3BlLnN0YXRlTGlzdCA9IEpvYnNTZXJ2aWNlLnN0YXRlTGlzdCgpXG5cbiAgJHNjb3BlLmNoYW5nZU5vZGUgPSAobm9kZWlkKSAtPlxuICAgIGlmIG5vZGVpZCAhPSAkc2NvcGUubm9kZWlkXG4gICAgICAkc2NvcGUubm9kZWlkID0gbm9kZWlkXG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbFxuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbFxuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGxcbiAgICAgICRzY29wZS5vcGVyYXRvckNoZWNrcG9pbnRTdGF0cyA9IG51bGxcblxuICAgICAgJHNjb3BlLiRicm9hZGNhc3QgJ3JlbG9hZCdcblxuICAgIGVsc2VcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsXG4gICAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2VcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBudWxsXG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsXG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbFxuICAgICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gbnVsbFxuXG4gICRzY29wZS5kZWFjdGl2YXRlTm9kZSA9IC0+XG4gICAgJHNjb3BlLm5vZGVpZCA9IG51bGxcbiAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2VcbiAgICAkc2NvcGUudmVydGV4ID0gbnVsbFxuICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGxcbiAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbFxuICAgICRzY29wZS5vcGVyYXRvckNoZWNrcG9pbnRTdGF0cyA9IG51bGxcblxuICAkc2NvcGUudG9nZ2xlRm9sZCA9IC0+XG4gICAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9ICEkc2NvcGUubm9kZVVuZm9sZGVkXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5TdWJ0YXNrc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlBsYW5TdWJ0YXNrc0NvbnRyb2xsZXInXG5cbiAgaWYgJHNjb3BlLm5vZGVpZCBhbmQgKCEkc2NvcGUudmVydGV4IG9yICEkc2NvcGUudmVydGV4LnN0KVxuICAgIEpvYnNTZXJ2aWNlLmdldFN1YnRhc2tzKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBkYXRhXG5cbiAgJHNjb3BlLiRvbiAncmVsb2FkJywgKGV2ZW50KSAtPlxuICAgIGNvbnNvbGUubG9nICdKb2JQbGFuU3VidGFza3NDb250cm9sbGVyJ1xuICAgIGlmICRzY29wZS5ub2RlaWRcbiAgICAgIEpvYnNTZXJ2aWNlLmdldFN1YnRhc2tzKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAgICRzY29wZS5zdWJ0YXNrcyA9IGRhdGFcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iUGxhblRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlBsYW5UYXNrTWFuYWdlcnNDb250cm9sbGVyJ1xuXG4gIGlmICRzY29wZS5ub2RlaWQgYW5kICghJHNjb3BlLnZlcnRleCBvciAhJHNjb3BlLnZlcnRleC5zdClcbiAgICBKb2JzU2VydmljZS5nZXRUYXNrTWFuYWdlcnMoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS50YXNrbWFuYWdlcnMgPSBkYXRhXG5cbiAgJHNjb3BlLiRvbiAncmVsb2FkJywgKGV2ZW50KSAtPlxuICAgIGNvbnNvbGUubG9nICdKb2JQbGFuVGFza01hbmFnZXJzQ29udHJvbGxlcidcbiAgICBpZiAkc2NvcGUubm9kZWlkXG4gICAgICBKb2JzU2VydmljZS5nZXRUYXNrTWFuYWdlcnMoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICAgJHNjb3BlLnRhc2ttYW5hZ2VycyA9IGRhdGFcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iUGxhbkFjY3VtdWxhdG9yc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJ1xuXG4gIGlmICRzY29wZS5ub2RlaWQgYW5kICghJHNjb3BlLnZlcnRleCBvciAhJHNjb3BlLnZlcnRleC5hY2N1bXVsYXRvcnMpXG4gICAgSm9ic1NlcnZpY2UuZ2V0QWNjdW11bGF0b3JzKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gZGF0YS5tYWluXG4gICAgICAkc2NvcGUuc3VidGFza0FjY3VtdWxhdG9ycyA9IGRhdGEuc3VidGFza3NcblxuICAkc2NvcGUuJG9uICdyZWxvYWQnLCAoZXZlbnQpIC0+XG4gICAgY29uc29sZS5sb2cgJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJ1xuICAgIGlmICRzY29wZS5ub2RlaWRcbiAgICAgIEpvYnNTZXJ2aWNlLmdldEFjY3VtdWxhdG9ycygkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gZGF0YS5tYWluXG4gICAgICAgICRzY29wZS5zdWJ0YXNrQWNjdW11bGF0b3JzID0gZGF0YS5zdWJ0YXNrc1xuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuQ2hlY2twb2ludHNDb250cm9sbGVyJywgKCRzY29wZSwgSm9ic1NlcnZpY2UpIC0+XG4gIGNvbnNvbGUubG9nICdKb2JQbGFuQ2hlY2twb2ludHNDb250cm9sbGVyJ1xuXG4gICMgR2V0IHRoZSBwZXIgam9iIHN0YXRzXG4gIEpvYnNTZXJ2aWNlLmdldEpvYkNoZWNrcG9pbnRTdGF0cygkc2NvcGUuam9iaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLmpvYkNoZWNrcG9pbnRTdGF0cyA9IGRhdGFcblxuICAjIEdldCB0aGUgcGVyIG9wZXJhdG9yIHN0YXRzXG4gIGlmICRzY29wZS5ub2RlaWQgYW5kICghJHNjb3BlLnZlcnRleCBvciAhJHNjb3BlLnZlcnRleC5vcGVyYXRvckNoZWNrcG9pbnRTdGF0cylcbiAgICBKb2JzU2VydmljZS5nZXRPcGVyYXRvckNoZWNrcG9pbnRTdGF0cygkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gZGF0YS5vcGVyYXRvclN0YXRzXG4gICAgICAkc2NvcGUuc3VidGFza3NDaGVja3BvaW50U3RhdHMgPSBkYXRhLnN1YnRhc2tzU3RhdHNcblxuICAkc2NvcGUuJG9uICdyZWxvYWQnLCAoZXZlbnQpIC0+XG4gICAgY29uc29sZS5sb2cgJ0pvYlBsYW5DaGVja3BvaW50c0NvbnRyb2xsZXInXG5cbiAgICBKb2JzU2VydmljZS5nZXRKb2JDaGVja3BvaW50U3RhdHMoJHNjb3BlLmpvYmlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLmpvYkNoZWNrcG9pbnRTdGF0cyA9IGRhdGFcblxuICAgIGlmICRzY29wZS5ub2RlaWRcbiAgICAgIEpvYnNTZXJ2aWNlLmdldE9wZXJhdG9yQ2hlY2twb2ludFN0YXRzKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAgICRzY29wZS5vcGVyYXRvckNoZWNrcG9pbnRTdGF0cyA9IGRhdGEub3BlcmF0b3JTdGF0c1xuICAgICAgICAkc2NvcGUuc3VidGFza3NDaGVja3BvaW50U3RhdHMgPSBkYXRhLnN1YnRhc2tzU3RhdHNcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iUGxhbkJhY2tQcmVzc3VyZUNvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlBsYW5CYWNrUHJlc3N1cmVDb250cm9sbGVyJ1xuICAkc2NvcGUubm93ID0gRGF0ZS5ub3coKVxuXG4gIGlmICRzY29wZS5ub2RlaWRcbiAgICBKb2JzU2VydmljZS5nZXRPcGVyYXRvckJhY2tQcmVzc3VyZSgkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLmJhY2tQcmVzc3VyZU9wZXJhdG9yU3RhdHNbJHNjb3BlLm5vZGVpZF0gPSBkYXRhXG5cbiAgJHNjb3BlLiRvbiAncmVsb2FkJywgKGV2ZW50KSAtPlxuICAgIGNvbnNvbGUubG9nICdKb2JQbGFuQmFja1ByZXNzdXJlQ29udHJvbGxlciAocmVsYW9kKSdcbiAgICAkc2NvcGUubm93ID0gRGF0ZS5ub3coKVxuXG4gICAgaWYgJHNjb3BlLm5vZGVpZFxuICAgICAgSm9ic1NlcnZpY2UuZ2V0T3BlcmF0b3JCYWNrUHJlc3N1cmUoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICAgJHNjb3BlLmJhY2tQcmVzc3VyZU9wZXJhdG9yU3RhdHNbJHNjb3BlLm5vZGVpZF0gPSBkYXRhXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcidcblxuICBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHN0YXRlUGFyYW1zLnZlcnRleElkKS50aGVuIChkYXRhKSAtPlxuICAgICRzY29wZS52ZXJ0ZXggPSBkYXRhXG5cbiAgJHNjb3BlLiRvbiAncmVsb2FkJywgKGV2ZW50KSAtPlxuICAgIGNvbnNvbGUubG9nICdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInXG4gICAgSm9ic1NlcnZpY2UuZ2V0VmVydGV4KCRzdGF0ZVBhcmFtcy52ZXJ0ZXhJZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS52ZXJ0ZXggPSBkYXRhXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0pvYkV4Y2VwdGlvbnNDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICBKb2JzU2VydmljZS5sb2FkRXhjZXB0aW9ucygpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLmV4Y2VwdGlvbnMgPSBkYXRhXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0pvYlByb3BlcnRpZXNDb250cm9sbGVyJywgKCRzY29wZSwgSm9ic1NlcnZpY2UpIC0+XG4gIGNvbnNvbGUubG9nICdKb2JQcm9wZXJ0aWVzQ29udHJvbGxlcidcblxuICAkc2NvcGUuY2hhbmdlTm9kZSA9IChub2RlaWQpIC0+XG4gICAgaWYgbm9kZWlkICE9ICRzY29wZS5ub2RlaWRcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWRcblxuICAgICAgSm9ic1NlcnZpY2UuZ2V0Tm9kZShub2RlaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAgICRzY29wZS5ub2RlID0gZGF0YVxuXG4gICAgZWxzZVxuICAgICAgJHNjb3BlLm5vZGVpZCA9IG51bGxcbiAgICAgICRzY29wZS5ub2RlID0gbnVsbFxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignUnVubmluZ0pvYnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygncnVubmluZycpO1xuICB9O1xuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS5qb2JPYnNlcnZlcigpO1xufSkuY29udHJvbGxlcignQ29tcGxldGVkSm9ic0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpO1xuICB9O1xuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS5qb2JPYnNlcnZlcigpO1xufSkuY29udHJvbGxlcignU2luZ2xlSm9iQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlLCAkcm9vdFNjb3BlLCBmbGlua0NvbmZpZywgJGludGVydmFsKSB7XG4gIHZhciByZWZyZXNoZXI7XG4gIGNvbnNvbGUubG9nKCdTaW5nbGVKb2JDb250cm9sbGVyJyk7XG4gICRzY29wZS5qb2JpZCA9ICRzdGF0ZVBhcmFtcy5qb2JpZDtcbiAgJHNjb3BlLmpvYiA9IG51bGw7XG4gICRzY29wZS5wbGFuID0gbnVsbDtcbiAgJHNjb3BlLnZlcnRpY2VzID0gbnVsbDtcbiAgJHNjb3BlLmpvYkNoZWNrcG9pbnRTdGF0cyA9IG51bGw7XG4gICRzY29wZS5zaG93SGlzdG9yeSA9IGZhbHNlO1xuICAkc2NvcGUuYmFja1ByZXNzdXJlT3BlcmF0b3JTdGF0cyA9IHt9O1xuICBKb2JzU2VydmljZS5sb2FkSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgJHNjb3BlLmpvYiA9IGRhdGE7XG4gICAgJHNjb3BlLnBsYW4gPSBkYXRhLnBsYW47XG4gICAgcmV0dXJuICRzY29wZS52ZXJ0aWNlcyA9IGRhdGEudmVydGljZXM7XG4gIH0pO1xuICByZWZyZXNoZXIgPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmxvYWRKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICRzY29wZS5qb2IgPSBkYXRhO1xuICAgICAgcmV0dXJuICRzY29wZS4kYnJvYWRjYXN0KCdyZWxvYWQnKTtcbiAgICB9KTtcbiAgfSwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICAkc2NvcGUuam9iID0gbnVsbDtcbiAgICAkc2NvcGUucGxhbiA9IG51bGw7XG4gICAgJHNjb3BlLnZlcnRpY2VzID0gbnVsbDtcbiAgICAkc2NvcGUuam9iQ2hlY2twb2ludFN0YXRzID0gbnVsbDtcbiAgICAkc2NvcGUuYmFja1ByZXNzdXJlT3BlcmF0b3JTdGF0cyA9IG51bGw7XG4gICAgcmV0dXJuICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaGVyKTtcbiAgfSk7XG4gICRzY29wZS5jYW5jZWxKb2IgPSBmdW5jdGlvbihjYW5jZWxFdmVudCkge1xuICAgIGFuZ3VsYXIuZWxlbWVudChjYW5jZWxFdmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImJ0blwiKS5yZW1vdmVDbGFzcyhcImJ0bi1kZWZhdWx0XCIpLmh0bWwoJ0NhbmNlbGxpbmcuLi4nKTtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UuY2FuY2VsSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4ge307XG4gICAgfSk7XG4gIH07XG4gICRzY29wZS5zdG9wSm9iID0gZnVuY3Rpb24oc3RvcEV2ZW50KSB7XG4gICAgYW5ndWxhci5lbGVtZW50KHN0b3BFdmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImJ0blwiKS5yZW1vdmVDbGFzcyhcImJ0bi1kZWZhdWx0XCIpLmh0bWwoJ1N0b3BwaW5nLi4uJyk7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLnN0b3BKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiB7fTtcbiAgICB9KTtcbiAgfTtcbiAgcmV0dXJuICRzY29wZS50b2dnbGVIaXN0b3J5ID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5zaG93SGlzdG9yeSA9ICEkc2NvcGUuc2hvd0hpc3Rvcnk7XG4gIH07XG59KS5jb250cm9sbGVyKCdKb2JQbGFuQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIGNvbnNvbGUubG9nKCdKb2JQbGFuQ29udHJvbGxlcicpO1xuICAkc2NvcGUubm9kZWlkID0gbnVsbDtcbiAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlO1xuICAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KCk7XG4gICRzY29wZS5jaGFuZ2VOb2RlID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgaWYgKG5vZGVpZCAhPT0gJHNjb3BlLm5vZGVpZCkge1xuICAgICAgJHNjb3BlLm5vZGVpZCA9IG5vZGVpZDtcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBudWxsO1xuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbDtcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsO1xuICAgICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gbnVsbDtcbiAgICAgIHJldHVybiAkc2NvcGUuJGJyb2FkY2FzdCgncmVsb2FkJyk7XG4gICAgfSBlbHNlIHtcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAgICAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlO1xuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGw7XG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsO1xuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGw7XG4gICAgICByZXR1cm4gJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gbnVsbDtcbiAgICB9XG4gIH07XG4gICRzY29wZS5kZWFjdGl2YXRlTm9kZSA9IGZ1bmN0aW9uKCkge1xuICAgICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZTtcbiAgICAkc2NvcGUudmVydGV4ID0gbnVsbDtcbiAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsO1xuICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsO1xuICAgIHJldHVybiAkc2NvcGUub3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSBudWxsO1xuICB9O1xuICByZXR1cm4gJHNjb3BlLnRvZ2dsZUZvbGQgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLm5vZGVVbmZvbGRlZCA9ICEkc2NvcGUubm9kZVVuZm9sZGVkO1xuICB9O1xufSkuY29udHJvbGxlcignSm9iUGxhblN1YnRhc2tzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlBsYW5TdWJ0YXNrc0NvbnRyb2xsZXInKTtcbiAgaWYgKCRzY29wZS5ub2RlaWQgJiYgKCEkc2NvcGUudmVydGV4IHx8ICEkc2NvcGUudmVydGV4LnN0KSkge1xuICAgIEpvYnNTZXJ2aWNlLmdldFN1YnRhc2tzKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5zdWJ0YXNrcyA9IGRhdGE7XG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgY29uc29sZS5sb2coJ0pvYlBsYW5TdWJ0YXNrc0NvbnRyb2xsZXInKTtcbiAgICBpZiAoJHNjb3BlLm5vZGVpZCkge1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldFN1YnRhc2tzKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJHNjb3BlLnN1YnRhc2tzID0gZGF0YTtcbiAgICAgIH0pO1xuICAgIH1cbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuVGFza01hbmFnZXJzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlBsYW5UYXNrTWFuYWdlcnNDb250cm9sbGVyJyk7XG4gIGlmICgkc2NvcGUubm9kZWlkICYmICghJHNjb3BlLnZlcnRleCB8fCAhJHNjb3BlLnZlcnRleC5zdCkpIHtcbiAgICBKb2JzU2VydmljZS5nZXRUYXNrTWFuYWdlcnMoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLnRhc2ttYW5hZ2VycyA9IGRhdGE7XG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgY29uc29sZS5sb2coJ0pvYlBsYW5UYXNrTWFuYWdlcnNDb250cm9sbGVyJyk7XG4gICAgaWYgKCRzY29wZS5ub2RlaWQpIHtcbiAgICAgIHJldHVybiBKb2JzU2VydmljZS5nZXRUYXNrTWFuYWdlcnMoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkc2NvcGUudGFza21hbmFnZXJzID0gZGF0YTtcbiAgICAgIH0pO1xuICAgIH1cbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJyk7XG4gIGlmICgkc2NvcGUubm9kZWlkICYmICghJHNjb3BlLnZlcnRleCB8fCAhJHNjb3BlLnZlcnRleC5hY2N1bXVsYXRvcnMpKSB7XG4gICAgSm9ic1NlcnZpY2UuZ2V0QWNjdW11bGF0b3JzKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IGRhdGEubWFpbjtcbiAgICAgIHJldHVybiAkc2NvcGUuc3VidGFza0FjY3VtdWxhdG9ycyA9IGRhdGEuc3VidGFza3M7XG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgY29uc29sZS5sb2coJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJyk7XG4gICAgaWYgKCRzY29wZS5ub2RlaWQpIHtcbiAgICAgIHJldHVybiBKb2JzU2VydmljZS5nZXRBY2N1bXVsYXRvcnMoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBkYXRhLm1haW47XG4gICAgICAgIHJldHVybiAkc2NvcGUuc3VidGFza0FjY3VtdWxhdG9ycyA9IGRhdGEuc3VidGFza3M7XG4gICAgICB9KTtcbiAgICB9XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlBsYW5DaGVja3BvaW50c0NvbnRyb2xsZXInKTtcbiAgSm9ic1NlcnZpY2UuZ2V0Sm9iQ2hlY2twb2ludFN0YXRzKCRzY29wZS5qb2JpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5qb2JDaGVja3BvaW50U3RhdHMgPSBkYXRhO1xuICB9KTtcbiAgaWYgKCRzY29wZS5ub2RlaWQgJiYgKCEkc2NvcGUudmVydGV4IHx8ICEkc2NvcGUudmVydGV4Lm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzKSkge1xuICAgIEpvYnNTZXJ2aWNlLmdldE9wZXJhdG9yQ2hlY2twb2ludFN0YXRzKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gZGF0YS5vcGVyYXRvclN0YXRzO1xuICAgICAgcmV0dXJuICRzY29wZS5zdWJ0YXNrc0NoZWNrcG9pbnRTdGF0cyA9IGRhdGEuc3VidGFza3NTdGF0cztcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oZXZlbnQpIHtcbiAgICBjb25zb2xlLmxvZygnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcicpO1xuICAgIEpvYnNTZXJ2aWNlLmdldEpvYkNoZWNrcG9pbnRTdGF0cygkc2NvcGUuam9iaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5qb2JDaGVja3BvaW50U3RhdHMgPSBkYXRhO1xuICAgIH0pO1xuICAgIGlmICgkc2NvcGUubm9kZWlkKSB7XG4gICAgICByZXR1cm4gSm9ic1NlcnZpY2UuZ2V0T3BlcmF0b3JDaGVja3BvaW50U3RhdHMoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICRzY29wZS5vcGVyYXRvckNoZWNrcG9pbnRTdGF0cyA9IGRhdGEub3BlcmF0b3JTdGF0cztcbiAgICAgICAgcmV0dXJuICRzY29wZS5zdWJ0YXNrc0NoZWNrcG9pbnRTdGF0cyA9IGRhdGEuc3VidGFza3NTdGF0cztcbiAgICAgIH0pO1xuICAgIH1cbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuQmFja1ByZXNzdXJlQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlBsYW5CYWNrUHJlc3N1cmVDb250cm9sbGVyJyk7XG4gICRzY29wZS5ub3cgPSBEYXRlLm5vdygpO1xuICBpZiAoJHNjb3BlLm5vZGVpZCkge1xuICAgIEpvYnNTZXJ2aWNlLmdldE9wZXJhdG9yQmFja1ByZXNzdXJlKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5iYWNrUHJlc3N1cmVPcGVyYXRvclN0YXRzWyRzY29wZS5ub2RlaWRdID0gZGF0YTtcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oZXZlbnQpIHtcbiAgICBjb25zb2xlLmxvZygnSm9iUGxhbkJhY2tQcmVzc3VyZUNvbnRyb2xsZXIgKHJlbGFvZCknKTtcbiAgICAkc2NvcGUubm93ID0gRGF0ZS5ub3coKTtcbiAgICBpZiAoJHNjb3BlLm5vZGVpZCkge1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldE9wZXJhdG9yQmFja1ByZXNzdXJlKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJHNjb3BlLmJhY2tQcmVzc3VyZU9wZXJhdG9yU3RhdHNbJHNjb3BlLm5vZGVpZF0gPSBkYXRhO1xuICAgICAgfSk7XG4gICAgfVxuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIGNvbnNvbGUubG9nKCdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInKTtcbiAgSm9ic1NlcnZpY2UuZ2V0VmVydGV4KCRzdGF0ZVBhcmFtcy52ZXJ0ZXhJZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS52ZXJ0ZXggPSBkYXRhO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgY29uc29sZS5sb2coJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcicpO1xuICAgIHJldHVybiBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHN0YXRlUGFyYW1zLnZlcnRleElkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUudmVydGV4ID0gZGF0YTtcbiAgICB9KTtcbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JFeGNlcHRpb25zQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiBKb2JzU2VydmljZS5sb2FkRXhjZXB0aW9ucygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUuZXhjZXB0aW9ucyA9IGRhdGE7XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUHJvcGVydGllc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYnNTZXJ2aWNlKSB7XG4gIGNvbnNvbGUubG9nKCdKb2JQcm9wZXJ0aWVzQ29udHJvbGxlcicpO1xuICByZXR1cm4gJHNjb3BlLmNoYW5nZU5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICBpZiAobm9kZWlkICE9PSAkc2NvcGUubm9kZWlkKSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbm9kZWlkO1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldE5vZGUobm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRzY29wZS5ub2RlID0gZGF0YTtcbiAgICAgIH0pO1xuICAgIH0gZWxzZSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbnVsbDtcbiAgICAgIHJldHVybiAkc2NvcGUubm9kZSA9IG51bGw7XG4gICAgfVxuICB9O1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ3ZlcnRleCcsICgkc3RhdGUpIC0+XG4gIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lIHNlY29uZGFyeScgd2lkdGg9JzAnIGhlaWdodD0nMCc+PC9zdmc+XCJcblxuICBzY29wZTpcbiAgICBkYXRhOiBcIj1cIlxuXG4gIGxpbms6IChzY29wZSwgZWxlbSwgYXR0cnMpIC0+XG4gICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF1cblxuICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKClcbiAgICBhbmd1bGFyLmVsZW1lbnQoc3ZnRWwpLmF0dHIoJ3dpZHRoJywgY29udGFpbmVyVylcblxuICAgIGFuYWx5emVUaW1lID0gKGRhdGEpIC0+XG4gICAgICBkMy5zZWxlY3Qoc3ZnRWwpLnNlbGVjdEFsbChcIipcIikucmVtb3ZlKClcblxuICAgICAgdGVzdERhdGEgPSBbXVxuXG4gICAgICBhbmd1bGFyLmZvckVhY2ggZGF0YS5zdWJ0YXNrcywgKHN1YnRhc2ssIGkpIC0+XG4gICAgICAgIHRpbWVzID0gW1xuICAgICAgICAgIHtcbiAgICAgICAgICAgIGxhYmVsOiBcIlNjaGVkdWxlZFwiXG4gICAgICAgICAgICBjb2xvcjogXCIjNjY2XCJcbiAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIlxuICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiU0NIRURVTEVEXCJdXG4gICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiREVQTE9ZSU5HXCJdXG4gICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICB9XG4gICAgICAgICAge1xuICAgICAgICAgICAgbGFiZWw6IFwiRGVwbG95aW5nXCJcbiAgICAgICAgICAgIGNvbG9yOiBcIiNhYWFcIlxuICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiXG4gICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl1cbiAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJSVU5OSU5HXCJdXG4gICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICB9XG4gICAgICAgIF1cblxuICAgICAgICBpZiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJGSU5JU0hFRFwiXSA+IDBcbiAgICAgICAgICB0aW1lcy5wdXNoIHtcbiAgICAgICAgICAgIGxhYmVsOiBcIlJ1bm5pbmdcIlxuICAgICAgICAgICAgY29sb3I6IFwiI2RkZFwiXG4gICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCJcbiAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl1cbiAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJGSU5JU0hFRFwiXVxuICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgfVxuXG4gICAgICAgIHRlc3REYXRhLnB1c2gge1xuICAgICAgICAgIGxhYmVsOiBcIigje3N1YnRhc2suc3VidGFza30pICN7c3VidGFzay5ob3N0fVwiXG4gICAgICAgICAgdGltZXM6IHRpbWVzXG4gICAgICAgIH1cblxuICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKClcbiAgICAgIC50aWNrRm9ybWF0KHtcbiAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVMXCIpXG4gICAgICAgICMgdGlja0ludGVydmFsOiAxXG4gICAgICAgIHRpY2tTaXplOiAxXG4gICAgICB9KVxuICAgICAgLnByZWZpeChcInNpbmdsZVwiKVxuICAgICAgLmxhYmVsRm9ybWF0KChsYWJlbCkgLT5cbiAgICAgICAgbGFiZWxcbiAgICAgIClcbiAgICAgIC5tYXJnaW4oeyBsZWZ0OiAxMDAsIHJpZ2h0OiAwLCB0b3A6IDAsIGJvdHRvbTogMCB9KVxuICAgICAgLml0ZW1IZWlnaHQoMzApXG4gICAgICAucmVsYXRpdmVUaW1lKClcblxuICAgICAgc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKVxuICAgICAgLmRhdHVtKHRlc3REYXRhKVxuICAgICAgLmNhbGwoY2hhcnQpXG5cbiAgICBhbmFseXplVGltZShzY29wZS5kYXRhKVxuXG4gICAgcmV0dXJuXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uZGlyZWN0aXZlICd0aW1lbGluZScsICgkc3RhdGUpIC0+XG4gIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lJyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIlxuXG4gIHNjb3BlOlxuICAgIHZlcnRpY2VzOiBcIj1cIlxuICAgIGpvYmlkOiBcIj1cIlxuXG4gIGxpbms6IChzY29wZSwgZWxlbSwgYXR0cnMpIC0+XG4gICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF1cblxuICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKClcbiAgICBhbmd1bGFyLmVsZW1lbnQoc3ZnRWwpLmF0dHIoJ3dpZHRoJywgY29udGFpbmVyVylcblxuICAgIHRyYW5zbGF0ZUxhYmVsID0gKGxhYmVsKSAtPlxuICAgICAgbGFiZWwucmVwbGFjZShcIiZndDtcIiwgXCI+XCIpXG5cbiAgICBhbmFseXplVGltZSA9IChkYXRhKSAtPlxuICAgICAgZDMuc2VsZWN0KHN2Z0VsKS5zZWxlY3RBbGwoXCIqXCIpLnJlbW92ZSgpXG5cbiAgICAgIHRlc3REYXRhID0gW11cblxuICAgICAgYW5ndWxhci5mb3JFYWNoIGRhdGEsICh2ZXJ0ZXgpIC0+XG4gICAgICAgIGlmIHZlcnRleFsnc3RhcnQtdGltZSddID4gLTFcbiAgICAgICAgICBpZiB2ZXJ0ZXgudHlwZSBpcyAnc2NoZWR1bGVkJ1xuICAgICAgICAgICAgdGVzdERhdGEucHVzaCBcbiAgICAgICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgICAgICBsYWJlbDogdHJhbnNsYXRlTGFiZWwodmVydGV4Lm5hbWUpXG4gICAgICAgICAgICAgICAgY29sb3I6IFwiI2NjY2NjY1wiXG4gICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NTU1NVwiXG4gICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdmVydGV4WydzdGFydC10aW1lJ11cbiAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdmVydGV4WydlbmQtdGltZSddXG4gICAgICAgICAgICAgICAgdHlwZTogdmVydGV4LnR5cGVcbiAgICAgICAgICAgICAgXVxuICAgICAgICAgIGVsc2VcbiAgICAgICAgICAgIHRlc3REYXRhLnB1c2ggXG4gICAgICAgICAgICAgIHRpbWVzOiBbXG4gICAgICAgICAgICAgICAgbGFiZWw6IHRyYW5zbGF0ZUxhYmVsKHZlcnRleC5uYW1lKVxuICAgICAgICAgICAgICAgIGNvbG9yOiBcIiNkOWYxZjdcIlxuICAgICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM2MmNkZWFcIlxuICAgICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHZlcnRleFsnc3RhcnQtdGltZSddXG4gICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZlcnRleFsnZW5kLXRpbWUnXVxuICAgICAgICAgICAgICAgIGxpbms6IHZlcnRleC5pZFxuICAgICAgICAgICAgICAgIHR5cGU6IHZlcnRleC50eXBlXG4gICAgICAgICAgICAgIF1cblxuICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKCkuY2xpY2soKGQsIGksIGRhdHVtKSAtPlxuICAgICAgICBpZiBkLmxpbmtcbiAgICAgICAgICAkc3RhdGUuZ28gXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLCB7IGpvYmlkOiBzY29wZS5qb2JpZCwgdmVydGV4SWQ6IGQubGluayB9XG5cbiAgICAgIClcbiAgICAgIC50aWNrRm9ybWF0KHtcbiAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVMXCIpXG4gICAgICAgICMgdGlja1RpbWU6IGQzLnRpbWUuc2Vjb25kXG4gICAgICAgICMgdGlja0ludGVydmFsOiAwLjVcbiAgICAgICAgdGlja1NpemU6IDFcbiAgICAgIH0pXG4gICAgICAucHJlZml4KFwibWFpblwiKVxuICAgICAgLm1hcmdpbih7IGxlZnQ6IDAsIHJpZ2h0OiAwLCB0b3A6IDAsIGJvdHRvbTogMCB9KVxuICAgICAgLml0ZW1IZWlnaHQoMzApXG4gICAgICAuc2hvd0JvcmRlckxpbmUoKVxuICAgICAgLnNob3dIb3VyVGltZWxpbmUoKVxuXG4gICAgICBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpXG4gICAgICAuZGF0dW0odGVzdERhdGEpXG4gICAgICAuY2FsbChjaGFydClcblxuICAgIHNjb3BlLiR3YXRjaCBhdHRycy52ZXJ0aWNlcywgKGRhdGEpIC0+XG4gICAgICBhbmFseXplVGltZShkYXRhKSBpZiBkYXRhXG5cbiAgICByZXR1cm5cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ2pvYlBsYW4nLCAoJHRpbWVvdXQpIC0+XG4gIHRlbXBsYXRlOiBcIlxuICAgIDxzdmcgY2xhc3M9J2dyYXBoJyB3aWR0aD0nNTAwJyBoZWlnaHQ9JzQwMCc+PGcgLz48L3N2Zz5cbiAgICA8c3ZnIGNsYXNzPSd0bXAnIHdpZHRoPScxJyBoZWlnaHQ9JzEnPjxnIC8+PC9zdmc+XG4gICAgPGRpdiBjbGFzcz0nYnRuLWdyb3VwIHpvb20tYnV0dG9ucyc+XG4gICAgICA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20taW4nIG5nLWNsaWNrPSd6b29tSW4oKSc+PGkgY2xhc3M9J2ZhIGZhLXBsdXMnIC8+PC9hPlxuICAgICAgPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLW91dCcgbmctY2xpY2s9J3pvb21PdXQoKSc+PGkgY2xhc3M9J2ZhIGZhLW1pbnVzJyAvPjwvYT5cbiAgICA8L2Rpdj5cIlxuXG4gIHNjb3BlOlxuICAgIHBsYW46ICc9J1xuICAgIHNldE5vZGU6ICcmJ1xuXG4gIGxpbms6IChzY29wZSwgZWxlbSwgYXR0cnMpIC0+XG4gICAgZyA9IG51bGxcbiAgICBtYWluWm9vbSA9IGQzLmJlaGF2aW9yLnpvb20oKVxuICAgIHN1YmdyYXBocyA9IFtdXG4gICAgam9iaWQgPSBhdHRycy5qb2JpZFxuXG4gICAgbWFpblN2Z0VsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMF1cbiAgICBtYWluRyA9IGVsZW0uY2hpbGRyZW4oKS5jaGlsZHJlbigpWzBdXG4gICAgbWFpblRtcEVsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMV1cblxuICAgIGQzbWFpblN2ZyA9IGQzLnNlbGVjdChtYWluU3ZnRWxlbWVudClcbiAgICBkM21haW5TdmdHID0gZDMuc2VsZWN0KG1haW5HKVxuICAgIGQzdG1wU3ZnID0gZDMuc2VsZWN0KG1haW5UbXBFbGVtZW50KVxuXG4gICAgIyBhbmd1bGFyLmVsZW1lbnQobWFpbkcpLmVtcHR5KClcbiAgICAjIGQzbWFpblN2Z0cuc2VsZWN0QWxsKFwiKlwiKS5yZW1vdmUoKVxuXG4gICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKVxuICAgIGFuZ3VsYXIuZWxlbWVudChlbGVtLmNoaWxkcmVuKClbMF0pLndpZHRoKGNvbnRhaW5lclcpXG5cbiAgICBzY29wZS56b29tSW4gPSAtPlxuICAgICAgaWYgbWFpblpvb20uc2NhbGUoKSA8IDIuOTlcbiAgICAgICAgXG4gICAgICAgICMgQ2FsY3VsYXRlIGFuZCBzdG9yZSBuZXcgdmFsdWVzIGluIHpvb20gb2JqZWN0XG4gICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpXG4gICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXG4gICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXG4gICAgICAgIG1haW5ab29tLnNjYWxlIG1haW5ab29tLnNjYWxlKCkgKyAwLjFcbiAgICAgICAgbWFpblpvb20udHJhbnNsYXRlIFsgdjEsIHYyIF1cbiAgICAgICAgXG4gICAgICAgICMgVHJhbnNmb3JtIHN2Z1xuICAgICAgICBkM21haW5TdmdHLmF0dHIgXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB2MSArIFwiLFwiICsgdjIgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCJcblxuICAgIHNjb3BlLnpvb21PdXQgPSAtPlxuICAgICAgaWYgbWFpblpvb20uc2NhbGUoKSA+IDAuMzFcbiAgICAgICAgXG4gICAgICAgICMgQ2FsY3VsYXRlIGFuZCBzdG9yZSBuZXcgdmFsdWVzIGluIG1haW5ab29tIG9iamVjdFxuICAgICAgICBtYWluWm9vbS5zY2FsZSBtYWluWm9vbS5zY2FsZSgpIC0gMC4xXG4gICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpXG4gICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXG4gICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXG4gICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZSBbIHYxLCB2MiBdXG4gICAgICAgIFxuICAgICAgICAjIFRyYW5zZm9ybSBzdmdcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyIFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiXG5cbiAgICAjY3JlYXRlIGEgbGFiZWwgb2YgYW4gZWRnZVxuICAgIGNyZWF0ZUxhYmVsRWRnZSA9IChlbCkgLT5cbiAgICAgIGxhYmVsVmFsdWUgPSBcIlwiXG4gICAgICBpZiBlbC5zaGlwX3N0cmF0ZWd5PyBvciBlbC5sb2NhbF9zdHJhdGVneT9cbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxkaXYgY2xhc3M9J2VkZ2UtbGFiZWwnPlwiXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gZWwuc2hpcF9zdHJhdGVneSAgaWYgZWwuc2hpcF9zdHJhdGVneT9cbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIiAoXCIgKyBlbC50ZW1wX21vZGUgKyBcIilcIiAgdW5sZXNzIGVsLnRlbXBfbW9kZSBpcyBgdW5kZWZpbmVkYFxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiLDxicj5cIiArIGVsLmxvY2FsX3N0cmF0ZWd5ICB1bmxlc3MgZWwubG9jYWxfc3RyYXRlZ3kgaXMgYHVuZGVmaW5lZGBcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjwvZGl2PlwiXG4gICAgICBsYWJlbFZhbHVlXG5cblxuICAgICMgdHJ1ZSwgaWYgdGhlIG5vZGUgaXMgYSBzcGVjaWFsIG5vZGUgZnJvbSBhbiBpdGVyYXRpb25cbiAgICBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlID0gKGluZm8pIC0+XG4gICAgICAoaW5mbyBpcyBcInBhcnRpYWxTb2x1dGlvblwiIG9yIGluZm8gaXMgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIgb3IgaW5mbyBpcyBcIndvcmtzZXRcIiBvciBpbmZvIGlzIFwibmV4dFdvcmtzZXRcIiBvciBpbmZvIGlzIFwic29sdXRpb25TZXRcIiBvciBpbmZvIGlzIFwic29sdXRpb25EZWx0YVwiKVxuXG4gICAgZ2V0Tm9kZVR5cGUgPSAoZWwsIGluZm8pIC0+XG4gICAgICBpZiBpbmZvIGlzIFwibWlycm9yXCJcbiAgICAgICAgJ25vZGUtbWlycm9yJ1xuXG4gICAgICBlbHNlIGlmIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUoaW5mbylcbiAgICAgICAgJ25vZGUtaXRlcmF0aW9uJ1xuXG4gICAgICBlbHNlXG4gICAgICAgICAgJ25vZGUtbm9ybWFsJ1xuICAgICAgXG4gICAgIyBjcmVhdGVzIHRoZSBsYWJlbCBvZiBhIG5vZGUsIGluIGluZm8gaXMgc3RvcmVkLCB3aGV0aGVyIGl0IGlzIGEgc3BlY2lhbCBub2RlIChsaWtlIGEgbWlycm9yIGluIGFuIGl0ZXJhdGlvbilcbiAgICBjcmVhdGVMYWJlbE5vZGUgPSAoZWwsIGluZm8sIG1heFcsIG1heEgpIC0+XG4gICAgICAjIGxhYmVsVmFsdWUgPSBcIjxhIGhyZWY9JyMvam9icy9cIiArIGpvYmlkICsgXCIvdmVydGV4L1wiICsgZWwuaWQgKyBcIicgY2xhc3M9J25vZGUtbGFiZWwgXCIgKyBnZXROb2RlVHlwZShlbCwgaW5mbykgKyBcIic+XCJcbiAgICAgIGxhYmVsVmFsdWUgPSBcIjxkaXYgaHJlZj0nIy9qb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0ZXgvXCIgKyBlbC5pZCArIFwiJyBjbGFzcz0nbm9kZS1sYWJlbCBcIiArIGdldE5vZGVUeXBlKGVsLCBpbmZvKSArIFwiJz5cIlxuXG4gICAgICAjIE5vZGVuYW1lXG4gICAgICBpZiBpbmZvIGlzIFwibWlycm9yXCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoMyBjbGFzcz0nbm9kZS1uYW1lJz5NaXJyb3Igb2YgXCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIlxuICAgICAgZWxzZVxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPlwiICsgZWwub3BlcmF0b3IgKyBcIjwvaDM+XCJcbiAgICAgIGlmIGVsLmRlc2NyaXB0aW9uIGlzIFwiXCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIlwiXG4gICAgICBlbHNlXG4gICAgICAgIHN0ZXBOYW1lID0gZWwuZGVzY3JpcHRpb25cbiAgICAgICAgXG4gICAgICAgICMgY2xlYW4gc3RlcE5hbWVcbiAgICAgICAgc3RlcE5hbWUgPSBzaG9ydGVuU3RyaW5nKHN0ZXBOYW1lKVxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg0IGNsYXNzPSdzdGVwLW5hbWUnPlwiICsgc3RlcE5hbWUgKyBcIjwvaDQ+XCJcbiAgICAgIFxuICAgICAgIyBJZiB0aGlzIG5vZGUgaXMgYW4gXCJpdGVyYXRpb25cIiB3ZSBuZWVkIGEgZGlmZmVyZW50IHBhbmVsLWJvZHlcbiAgICAgIGlmIGVsLnN0ZXBfZnVuY3Rpb24/XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uKGVsLmlkLCBtYXhXLCBtYXhIKVxuICAgICAgZWxzZVxuICAgICAgICBcbiAgICAgICAgIyBPdGhlcndpc2UgYWRkIGluZm9zICAgIFxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlwiICsgaW5mbyArIFwiIE5vZGU8L2g1PlwiICBpZiBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+UGFyYWxsZWxpc206IFwiICsgZWwucGFyYWxsZWxpc20gKyBcIjwvaDU+XCIgIHVubGVzcyBlbC5wYXJhbGxlbGlzbSBpcyBcIlwiXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+T3BlcmF0aW9uOiBcIiArIHNob3J0ZW5TdHJpbmcoZWwub3BlcmF0b3Jfc3RyYXRlZ3kpICsgXCI8L2g1PlwiICB1bmxlc3MgZWwub3BlcmF0b3IgaXMgYHVuZGVmaW5lZGBcbiAgICAgIFxuICAgICAgIyBsYWJlbFZhbHVlICs9IFwiPC9hPlwiXG4gICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCJcbiAgICAgIGxhYmVsVmFsdWVcblxuICAgICMgRXh0ZW5kcyB0aGUgbGFiZWwgb2YgYSBub2RlIHdpdGggYW4gYWRkaXRpb25hbCBzdmcgRWxlbWVudCB0byBwcmVzZW50IHRoZSBpdGVyYXRpb24uXG4gICAgZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uID0gKGlkLCBtYXhXLCBtYXhIKSAtPlxuICAgICAgc3ZnSUQgPSBcInN2Zy1cIiArIGlkXG5cbiAgICAgIGxhYmVsVmFsdWUgPSBcIjxzdmcgY2xhc3M9J1wiICsgc3ZnSUQgKyBcIicgd2lkdGg9XCIgKyBtYXhXICsgXCIgaGVpZ2h0PVwiICsgbWF4SCArIFwiPjxnIC8+PC9zdmc+XCJcbiAgICAgIGxhYmVsVmFsdWVcblxuICAgICMgU3BsaXQgYSBzdHJpbmcgaW50byBtdWx0aXBsZSBsaW5lcyBzbyB0aGF0IGVhY2ggbGluZSBoYXMgbGVzcyB0aGFuIDMwIGxldHRlcnMuXG4gICAgc2hvcnRlblN0cmluZyA9IChzKSAtPlxuICAgICAgIyBtYWtlIHN1cmUgdGhhdCBuYW1lIGRvZXMgbm90IGNvbnRhaW4gYSA8IChiZWNhdXNlIG9mIGh0bWwpXG4gICAgICBpZiBzLmNoYXJBdCgwKSBpcyBcIjxcIlxuICAgICAgICBzID0gcy5yZXBsYWNlKFwiPFwiLCBcIiZsdDtcIilcbiAgICAgICAgcyA9IHMucmVwbGFjZShcIj5cIiwgXCImZ3Q7XCIpXG4gICAgICBzYnIgPSBcIlwiXG4gICAgICB3aGlsZSBzLmxlbmd0aCA+IDMwXG4gICAgICAgIHNiciA9IHNiciArIHMuc3Vic3RyaW5nKDAsIDMwKSArIFwiPGJyPlwiXG4gICAgICAgIHMgPSBzLnN1YnN0cmluZygzMCwgcy5sZW5ndGgpXG4gICAgICBzYnIgPSBzYnIgKyBzXG4gICAgICBzYnJcblxuICAgIGNyZWF0ZU5vZGUgPSAoZywgZGF0YSwgZWwsIGlzUGFyZW50ID0gZmFsc2UsIG1heFcsIG1heEgpIC0+XG4gICAgICAjIGNyZWF0ZSBub2RlLCBzZW5kIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25zIGFib3V0IHRoZSBub2RlIGlmIGl0IGlzIGEgc3BlY2lhbCBvbmVcbiAgICAgIGlmIGVsLmlkIGlzIGRhdGEucGFydGlhbF9zb2x1dGlvblxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIilcblxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLm5leHRfcGFydGlhbF9zb2x1dGlvblxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEud29ya3NldFxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJ3b3Jrc2V0XCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwid29ya3NldFwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEubmV4dF93b3Jrc2V0XG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRXb3Jrc2V0XCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwibmV4dFdvcmtzZXRcIilcblxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLnNvbHV0aW9uX3NldFxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvblNldFwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcInNvbHV0aW9uU2V0XCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS5zb2x1dGlvbl9kZWx0YVxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25EZWx0YVwiKVxuXG4gICAgICBlbHNlXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIlwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcIlwiKVxuXG4gICAgY3JlYXRlRWRnZSA9IChnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZCkgLT5cbiAgICAgIHVubGVzcyBleGlzdGluZ05vZGVzLmluZGV4T2YocHJlZC5pZCkgaXMgLTFcbiAgICAgICAgZy5zZXRFZGdlIHByZWQuaWQsIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UocHJlZClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGFycm93aGVhZDogJ25vcm1hbCdcblxuICAgICAgZWxzZVxuICAgICAgICBtaXNzaW5nTm9kZSA9IHNlYXJjaEZvck5vZGUoZGF0YSwgcHJlZC5pZClcbiAgICAgICAgdW5sZXNzICFtaXNzaW5nTm9kZVxuICAgICAgICAgIGcuc2V0RWRnZSBtaXNzaW5nTm9kZS5pZCwgZWwuaWQsXG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxFZGdlKG1pc3NpbmdOb2RlKVxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcblxuICAgIGxvYWRKc29uVG9EYWdyZSA9IChnLCBkYXRhKSAtPlxuICAgICAgZXhpc3RpbmdOb2RlcyA9IFtdXG5cbiAgICAgIGlmIGRhdGEubm9kZXM/XG4gICAgICAgICMgVGhpcyBpcyB0aGUgbm9ybWFsIGpzb24gZGF0YVxuICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLm5vZGVzXG5cbiAgICAgIGVsc2VcbiAgICAgICAgIyBUaGlzIGlzIGFuIGl0ZXJhdGlvbiwgd2Ugbm93IHN0b3JlIHNwZWNpYWwgaXRlcmF0aW9uIG5vZGVzIGlmIHBvc3NpYmxlXG4gICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEuc3RlcF9mdW5jdGlvblxuICAgICAgICBpc1BhcmVudCA9IHRydWVcblxuICAgICAgZm9yIGVsIGluIHRvSXRlcmF0ZVxuICAgICAgICBtYXhXID0gMFxuICAgICAgICBtYXhIID0gMFxuXG4gICAgICAgIGlmIGVsLnN0ZXBfZnVuY3Rpb25cbiAgICAgICAgICBzZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHsgbXVsdGlncmFwaDogdHJ1ZSwgY29tcG91bmQ6IHRydWUgfSkuc2V0R3JhcGgoe1xuICAgICAgICAgICAgbm9kZXNlcDogMjBcbiAgICAgICAgICAgIGVkZ2VzZXA6IDBcbiAgICAgICAgICAgIHJhbmtzZXA6IDIwXG4gICAgICAgICAgICByYW5rZGlyOiBcIkxSXCJcbiAgICAgICAgICAgIG1hcmdpbng6IDEwXG4gICAgICAgICAgICBtYXJnaW55OiAxMFxuICAgICAgICAgICAgfSlcblxuICAgICAgICAgIHN1YmdyYXBoc1tlbC5pZF0gPSBzZ1xuXG4gICAgICAgICAgbG9hZEpzb25Ub0RhZ3JlKHNnLCBlbClcblxuICAgICAgICAgIHIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKVxuICAgICAgICAgIGQzdG1wU3ZnLnNlbGVjdCgnZycpLmNhbGwociwgc2cpXG4gICAgICAgICAgbWF4VyA9IHNnLmdyYXBoKCkud2lkdGhcbiAgICAgICAgICBtYXhIID0gc2cuZ3JhcGgoKS5oZWlnaHRcblxuICAgICAgICAgIGFuZ3VsYXIuZWxlbWVudChtYWluVG1wRWxlbWVudCkuZW1wdHkoKVxuXG4gICAgICAgIGNyZWF0ZU5vZGUoZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKVxuXG4gICAgICAgIGV4aXN0aW5nTm9kZXMucHVzaCBlbC5pZFxuICAgICAgICBcbiAgICAgICAgIyBjcmVhdGUgZWRnZXMgZnJvbSBpbnB1dHMgdG8gY3VycmVudCBub2RlXG4gICAgICAgIGlmIGVsLmlucHV0cz9cbiAgICAgICAgICBmb3IgcHJlZCBpbiBlbC5pbnB1dHNcbiAgICAgICAgICAgIGNyZWF0ZUVkZ2UoZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQpXG5cbiAgICAgIGdcblxuICAgICMgc2VhcmNoZXMgaW4gdGhlIGdsb2JhbCBKU09ORGF0YSBmb3IgdGhlIG5vZGUgd2l0aCB0aGUgZ2l2ZW4gaWRcbiAgICBzZWFyY2hGb3JOb2RlID0gKGRhdGEsIG5vZGVJRCkgLT5cbiAgICAgIGZvciBpIG9mIGRhdGEubm9kZXNcbiAgICAgICAgZWwgPSBkYXRhLm5vZGVzW2ldXG4gICAgICAgIHJldHVybiBlbCAgaWYgZWwuaWQgaXMgbm9kZUlEXG4gICAgICAgIFxuICAgICAgICAjIGxvb2sgZm9yIG5vZGVzIHRoYXQgYXJlIGluIGl0ZXJhdGlvbnNcbiAgICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvbj9cbiAgICAgICAgICBmb3IgaiBvZiBlbC5zdGVwX2Z1bmN0aW9uXG4gICAgICAgICAgICByZXR1cm4gZWwuc3RlcF9mdW5jdGlvbltqXSAgaWYgZWwuc3RlcF9mdW5jdGlvbltqXS5pZCBpcyBub2RlSURcblxuICAgIGRyYXdHcmFwaCA9IChkYXRhKSAtPlxuICAgICAgZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHsgbXVsdGlncmFwaDogdHJ1ZSwgY29tcG91bmQ6IHRydWUgfSkuc2V0R3JhcGgoe1xuICAgICAgICBub2Rlc2VwOiA3MFxuICAgICAgICBlZGdlc2VwOiAwXG4gICAgICAgIHJhbmtzZXA6IDUwXG4gICAgICAgIHJhbmtkaXI6IFwiTFJcIlxuICAgICAgICBtYXJnaW54OiA0MFxuICAgICAgICBtYXJnaW55OiA0MFxuICAgICAgICB9KVxuXG4gICAgICBsb2FkSnNvblRvRGFncmUoZywgZGF0YSlcblxuICAgICAgcmVuZGVyZXIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKVxuICAgICAgZDNtYWluU3ZnRy5jYWxsKHJlbmRlcmVyLCBnKVxuXG4gICAgICBmb3IgaSwgc2cgb2Ygc3ViZ3JhcGhzXG4gICAgICAgIGQzbWFpblN2Zy5zZWxlY3QoJ3N2Zy5zdmctJyArIGkgKyAnIGcnKS5jYWxsKHJlbmRlcmVyLCBzZylcblxuICAgICAgbmV3U2NhbGUgPSAwLjVcblxuICAgICAgeENlbnRlck9mZnNldCA9IE1hdGguZmxvb3IoKGFuZ3VsYXIuZWxlbWVudChtYWluU3ZnRWxlbWVudCkud2lkdGgoKSAtIGcuZ3JhcGgoKS53aWR0aCAqIG5ld1NjYWxlKSAvIDIpXG4gICAgICB5Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS5oZWlnaHQoKSAtIGcuZ3JhcGgoKS5oZWlnaHQgKiBuZXdTY2FsZSkgLyAyKVxuXG4gICAgICBtYWluWm9vbS5zY2FsZShuZXdTY2FsZSkudHJhbnNsYXRlKFt4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0XSlcblxuICAgICAgZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgeENlbnRlck9mZnNldCArIFwiLCBcIiArIHlDZW50ZXJPZmZzZXQgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpXG5cbiAgICAgIG1haW5ab29tLm9uKFwiem9vbVwiLCAtPlxuICAgICAgICBldiA9IGQzLmV2ZW50XG4gICAgICAgIGQzbWFpblN2Z0cuYXR0ciBcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZSArIFwiKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIpXCJcbiAgICAgIClcbiAgICAgIG1haW5ab29tKGQzbWFpblN2ZylcblxuICAgICAgZDNtYWluU3ZnRy5zZWxlY3RBbGwoJy5ub2RlJykub24gJ2NsaWNrJywgKGQpIC0+XG4gICAgICAgIHNjb3BlLnNldE5vZGUoeyBub2RlaWQ6IGQgfSlcblxuICAgIHNjb3BlLiR3YXRjaCBhdHRycy5wbGFuLCAobmV3UGxhbikgLT5cbiAgICAgIGRyYXdHcmFwaChuZXdQbGFuKSBpZiBuZXdQbGFuXG5cbiAgICByZXR1cm5cbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmRpcmVjdGl2ZSgndmVydGV4JywgZnVuY3Rpb24oJHN0YXRlKSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUgc2Vjb25kYXJ5JyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIixcbiAgICBzY29wZToge1xuICAgICAgZGF0YTogXCI9XCJcbiAgICB9LFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtLCBhdHRycykge1xuICAgICAgdmFyIGFuYWx5emVUaW1lLCBjb250YWluZXJXLCBzdmdFbDtcbiAgICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdO1xuICAgICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKTtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKTtcbiAgICAgIGFuYWx5emVUaW1lID0gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgY2hhcnQsIHN2ZywgdGVzdERhdGE7XG4gICAgICAgIGQzLnNlbGVjdChzdmdFbCkuc2VsZWN0QWxsKFwiKlwiKS5yZW1vdmUoKTtcbiAgICAgICAgdGVzdERhdGEgPSBbXTtcbiAgICAgICAgYW5ndWxhci5mb3JFYWNoKGRhdGEuc3VidGFza3MsIGZ1bmN0aW9uKHN1YnRhc2ssIGkpIHtcbiAgICAgICAgICB2YXIgdGltZXM7XG4gICAgICAgICAgdGltZXMgPSBbXG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgIGxhYmVsOiBcIlNjaGVkdWxlZFwiLFxuICAgICAgICAgICAgICBjb2xvcjogXCIjNjY2XCIsXG4gICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIixcbiAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiU0NIRURVTEVEXCJdLFxuICAgICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiREVQTE9ZSU5HXCJdLFxuICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgIH0sIHtcbiAgICAgICAgICAgICAgbGFiZWw6IFwiRGVwbG95aW5nXCIsXG4gICAgICAgICAgICAgIGNvbG9yOiBcIiNhYWFcIixcbiAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiLFxuICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl0sXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJSVU5OSU5HXCJdLFxuICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgIH1cbiAgICAgICAgICBdO1xuICAgICAgICAgIGlmIChzdWJ0YXNrLnRpbWVzdGFtcHNbXCJGSU5JU0hFRFwiXSA+IDApIHtcbiAgICAgICAgICAgIHRpbWVzLnB1c2goe1xuICAgICAgICAgICAgICBsYWJlbDogXCJSdW5uaW5nXCIsXG4gICAgICAgICAgICAgIGNvbG9yOiBcIiNkZGRcIixcbiAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiLFxuICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJSVU5OSU5HXCJdLFxuICAgICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiRklOSVNIRURcIl0sXG4gICAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuICAgICAgICAgIHJldHVybiB0ZXN0RGF0YS5wdXNoKHtcbiAgICAgICAgICAgIGxhYmVsOiBcIihcIiArIHN1YnRhc2suc3VidGFzayArIFwiKSBcIiArIHN1YnRhc2suaG9zdCxcbiAgICAgICAgICAgIHRpbWVzOiB0aW1lc1xuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKCkudGlja0Zvcm1hdCh7XG4gICAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVMXCIpLFxuICAgICAgICAgIHRpY2tTaXplOiAxXG4gICAgICAgIH0pLnByZWZpeChcInNpbmdsZVwiKS5sYWJlbEZvcm1hdChmdW5jdGlvbihsYWJlbCkge1xuICAgICAgICAgIHJldHVybiBsYWJlbDtcbiAgICAgICAgfSkubWFyZ2luKHtcbiAgICAgICAgICBsZWZ0OiAxMDAsXG4gICAgICAgICAgcmlnaHQ6IDAsXG4gICAgICAgICAgdG9wOiAwLFxuICAgICAgICAgIGJvdHRvbTogMFxuICAgICAgICB9KS5pdGVtSGVpZ2h0KDMwKS5yZWxhdGl2ZVRpbWUoKTtcbiAgICAgICAgcmV0dXJuIHN2ZyA9IGQzLnNlbGVjdChzdmdFbCkuZGF0dW0odGVzdERhdGEpLmNhbGwoY2hhcnQpO1xuICAgICAgfTtcbiAgICAgIGFuYWx5emVUaW1lKHNjb3BlLmRhdGEpO1xuICAgIH1cbiAgfTtcbn0pLmRpcmVjdGl2ZSgndGltZWxpbmUnLCBmdW5jdGlvbigkc3RhdGUpIHtcbiAgcmV0dXJuIHtcbiAgICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSd0aW1lbGluZScgd2lkdGg9JzAnIGhlaWdodD0nMCc+PC9zdmc+XCIsXG4gICAgc2NvcGU6IHtcbiAgICAgIHZlcnRpY2VzOiBcIj1cIixcbiAgICAgIGpvYmlkOiBcIj1cIlxuICAgIH0sXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW0sIGF0dHJzKSB7XG4gICAgICB2YXIgYW5hbHl6ZVRpbWUsIGNvbnRhaW5lclcsIHN2Z0VsLCB0cmFuc2xhdGVMYWJlbDtcbiAgICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdO1xuICAgICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKTtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKTtcbiAgICAgIHRyYW5zbGF0ZUxhYmVsID0gZnVuY3Rpb24obGFiZWwpIHtcbiAgICAgICAgcmV0dXJuIGxhYmVsLnJlcGxhY2UoXCImZ3Q7XCIsIFwiPlwiKTtcbiAgICAgIH07XG4gICAgICBhbmFseXplVGltZSA9IGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGNoYXJ0LCBzdmcsIHRlc3REYXRhO1xuICAgICAgICBkMy5zZWxlY3Qoc3ZnRWwpLnNlbGVjdEFsbChcIipcIikucmVtb3ZlKCk7XG4gICAgICAgIHRlc3REYXRhID0gW107XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLCBmdW5jdGlvbih2ZXJ0ZXgpIHtcbiAgICAgICAgICBpZiAodmVydGV4WydzdGFydC10aW1lJ10gPiAtMSkge1xuICAgICAgICAgICAgaWYgKHZlcnRleC50eXBlID09PSAnc2NoZWR1bGVkJykge1xuICAgICAgICAgICAgICByZXR1cm4gdGVzdERhdGEucHVzaCh7XG4gICAgICAgICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICAgICAgbGFiZWw6IHRyYW5zbGF0ZUxhYmVsKHZlcnRleC5uYW1lKSxcbiAgICAgICAgICAgICAgICAgICAgY29sb3I6IFwiI2NjY2NjY1wiLFxuICAgICAgICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1NTU1XCIsXG4gICAgICAgICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHZlcnRleFsnc3RhcnQtdGltZSddLFxuICAgICAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdmVydGV4WydlbmQtdGltZSddLFxuICAgICAgICAgICAgICAgICAgICB0eXBlOiB2ZXJ0ZXgudHlwZVxuICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIF1cbiAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICByZXR1cm4gdGVzdERhdGEucHVzaCh7XG4gICAgICAgICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICAgICAgbGFiZWw6IHRyYW5zbGF0ZUxhYmVsKHZlcnRleC5uYW1lKSxcbiAgICAgICAgICAgICAgICAgICAgY29sb3I6IFwiI2Q5ZjFmN1wiLFxuICAgICAgICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNjJjZGVhXCIsXG4gICAgICAgICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHZlcnRleFsnc3RhcnQtdGltZSddLFxuICAgICAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdmVydGV4WydlbmQtdGltZSddLFxuICAgICAgICAgICAgICAgICAgICBsaW5rOiB2ZXJ0ZXguaWQsXG4gICAgICAgICAgICAgICAgICAgIHR5cGU6IHZlcnRleC50eXBlXG4gICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgXVxuICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKS5jbGljayhmdW5jdGlvbihkLCBpLCBkYXR1bSkge1xuICAgICAgICAgIGlmIChkLmxpbmspIHtcbiAgICAgICAgICAgIHJldHVybiAkc3RhdGUuZ28oXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLCB7XG4gICAgICAgICAgICAgIGpvYmlkOiBzY29wZS5qb2JpZCxcbiAgICAgICAgICAgICAgdmVydGV4SWQ6IGQubGlua1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuICAgICAgICB9KS50aWNrRm9ybWF0KHtcbiAgICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIiksXG4gICAgICAgICAgdGlja1NpemU6IDFcbiAgICAgICAgfSkucHJlZml4KFwibWFpblwiKS5tYXJnaW4oe1xuICAgICAgICAgIGxlZnQ6IDAsXG4gICAgICAgICAgcmlnaHQ6IDAsXG4gICAgICAgICAgdG9wOiAwLFxuICAgICAgICAgIGJvdHRvbTogMFxuICAgICAgICB9KS5pdGVtSGVpZ2h0KDMwKS5zaG93Qm9yZGVyTGluZSgpLnNob3dIb3VyVGltZWxpbmUoKTtcbiAgICAgICAgcmV0dXJuIHN2ZyA9IGQzLnNlbGVjdChzdmdFbCkuZGF0dW0odGVzdERhdGEpLmNhbGwoY2hhcnQpO1xuICAgICAgfTtcbiAgICAgIHNjb3BlLiR3YXRjaChhdHRycy52ZXJ0aWNlcywgZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICBpZiAoZGF0YSkge1xuICAgICAgICAgIHJldHVybiBhbmFseXplVGltZShkYXRhKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCdqb2JQbGFuJywgZnVuY3Rpb24oJHRpbWVvdXQpIHtcbiAgcmV0dXJuIHtcbiAgICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSdncmFwaCcgd2lkdGg9JzUwMCcgaGVpZ2h0PSc0MDAnPjxnIC8+PC9zdmc+IDxzdmcgY2xhc3M9J3RtcCcgd2lkdGg9JzEnIGhlaWdodD0nMSc+PGcgLz48L3N2Zz4gPGRpdiBjbGFzcz0nYnRuLWdyb3VwIHpvb20tYnV0dG9ucyc+IDxhIGNsYXNzPSdidG4gYnRuLWRlZmF1bHQgem9vbS1pbicgbmctY2xpY2s9J3pvb21JbigpJz48aSBjbGFzcz0nZmEgZmEtcGx1cycgLz48L2E+IDxhIGNsYXNzPSdidG4gYnRuLWRlZmF1bHQgem9vbS1vdXQnIG5nLWNsaWNrPSd6b29tT3V0KCknPjxpIGNsYXNzPSdmYSBmYS1taW51cycgLz48L2E+IDwvZGl2PlwiLFxuICAgIHNjb3BlOiB7XG4gICAgICBwbGFuOiAnPScsXG4gICAgICBzZXROb2RlOiAnJidcbiAgICB9LFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtLCBhdHRycykge1xuICAgICAgdmFyIGNvbnRhaW5lclcsIGNyZWF0ZUVkZ2UsIGNyZWF0ZUxhYmVsRWRnZSwgY3JlYXRlTGFiZWxOb2RlLCBjcmVhdGVOb2RlLCBkM21haW5TdmcsIGQzbWFpblN2Z0csIGQzdG1wU3ZnLCBkcmF3R3JhcGgsIGV4dGVuZExhYmVsTm9kZUZvckl0ZXJhdGlvbiwgZywgZ2V0Tm9kZVR5cGUsIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUsIGpvYmlkLCBsb2FkSnNvblRvRGFncmUsIG1haW5HLCBtYWluU3ZnRWxlbWVudCwgbWFpblRtcEVsZW1lbnQsIG1haW5ab29tLCBzZWFyY2hGb3JOb2RlLCBzaG9ydGVuU3RyaW5nLCBzdWJncmFwaHM7XG4gICAgICBnID0gbnVsbDtcbiAgICAgIG1haW5ab29tID0gZDMuYmVoYXZpb3Iuem9vbSgpO1xuICAgICAgc3ViZ3JhcGhzID0gW107XG4gICAgICBqb2JpZCA9IGF0dHJzLmpvYmlkO1xuICAgICAgbWFpblN2Z0VsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBtYWluRyA9IGVsZW0uY2hpbGRyZW4oKS5jaGlsZHJlbigpWzBdO1xuICAgICAgbWFpblRtcEVsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMV07XG4gICAgICBkM21haW5TdmcgPSBkMy5zZWxlY3QobWFpblN2Z0VsZW1lbnQpO1xuICAgICAgZDNtYWluU3ZnRyA9IGQzLnNlbGVjdChtYWluRyk7XG4gICAgICBkM3RtcFN2ZyA9IGQzLnNlbGVjdChtYWluVG1wRWxlbWVudCk7XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KGVsZW0uY2hpbGRyZW4oKVswXSkud2lkdGgoY29udGFpbmVyVyk7XG4gICAgICBzY29wZS56b29tSW4gPSBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIHRyYW5zbGF0ZSwgdjEsIHYyO1xuICAgICAgICBpZiAobWFpblpvb20uc2NhbGUoKSA8IDIuOTkpIHtcbiAgICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKTtcbiAgICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICBtYWluWm9vbS5zY2FsZShtYWluWm9vbS5zY2FsZSgpICsgMC4xKTtcbiAgICAgICAgICBtYWluWm9vbS50cmFuc2xhdGUoW3YxLCB2Ml0pO1xuICAgICAgICAgIHJldHVybiBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB2MSArIFwiLFwiICsgdjIgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgc2NvcGUuem9vbU91dCA9IGZ1bmN0aW9uKCkge1xuICAgICAgICB2YXIgdHJhbnNsYXRlLCB2MSwgdjI7XG4gICAgICAgIGlmIChtYWluWm9vbS5zY2FsZSgpID4gMC4zMSkge1xuICAgICAgICAgIG1haW5ab29tLnNjYWxlKG1haW5ab29tLnNjYWxlKCkgLSAwLjEpO1xuICAgICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpO1xuICAgICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZShbdjEsIHYyXSk7XG4gICAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIik7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBjcmVhdGVMYWJlbEVkZ2UgPSBmdW5jdGlvbihlbCkge1xuICAgICAgICB2YXIgbGFiZWxWYWx1ZTtcbiAgICAgICAgbGFiZWxWYWx1ZSA9IFwiXCI7XG4gICAgICAgIGlmICgoZWwuc2hpcF9zdHJhdGVneSAhPSBudWxsKSB8fCAoZWwubG9jYWxfc3RyYXRlZ3kgIT0gbnVsbCkpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGRpdiBjbGFzcz0nZWRnZS1sYWJlbCc+XCI7XG4gICAgICAgICAgaWYgKGVsLnNoaXBfc3RyYXRlZ3kgIT0gbnVsbCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBlbC5zaGlwX3N0cmF0ZWd5O1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwudGVtcF9tb2RlICE9PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCIgKFwiICsgZWwudGVtcF9tb2RlICsgXCIpXCI7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5sb2NhbF9zdHJhdGVneSAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiLDxicj5cIiArIGVsLmxvY2FsX3N0cmF0ZWd5O1xuICAgICAgICAgIH1cbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCI7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIGxhYmVsVmFsdWU7XG4gICAgICB9O1xuICAgICAgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZSA9IGZ1bmN0aW9uKGluZm8pIHtcbiAgICAgICAgcmV0dXJuIGluZm8gPT09IFwicGFydGlhbFNvbHV0aW9uXCIgfHwgaW5mbyA9PT0gXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIgfHwgaW5mbyA9PT0gXCJ3b3Jrc2V0XCIgfHwgaW5mbyA9PT0gXCJuZXh0V29ya3NldFwiIHx8IGluZm8gPT09IFwic29sdXRpb25TZXRcIiB8fCBpbmZvID09PSBcInNvbHV0aW9uRGVsdGFcIjtcbiAgICAgIH07XG4gICAgICBnZXROb2RlVHlwZSA9IGZ1bmN0aW9uKGVsLCBpbmZvKSB7XG4gICAgICAgIGlmIChpbmZvID09PSBcIm1pcnJvclwiKSB7XG4gICAgICAgICAgcmV0dXJuICdub2RlLW1pcnJvcic7XG4gICAgICAgIH0gZWxzZSBpZiAoaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKSkge1xuICAgICAgICAgIHJldHVybiAnbm9kZS1pdGVyYXRpb24nO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHJldHVybiAnbm9kZS1ub3JtYWwnO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgY3JlYXRlTGFiZWxOb2RlID0gZnVuY3Rpb24oZWwsIGluZm8sIG1heFcsIG1heEgpIHtcbiAgICAgICAgdmFyIGxhYmVsVmFsdWUsIHN0ZXBOYW1lO1xuICAgICAgICBsYWJlbFZhbHVlID0gXCI8ZGl2IGhyZWY9JyMvam9icy9cIiArIGpvYmlkICsgXCIvdmVydGV4L1wiICsgZWwuaWQgKyBcIicgY2xhc3M9J25vZGUtbGFiZWwgXCIgKyBnZXROb2RlVHlwZShlbCwgaW5mbykgKyBcIic+XCI7XG4gICAgICAgIGlmIChpbmZvID09PSBcIm1pcnJvclwiKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoMyBjbGFzcz0nbm9kZS1uYW1lJz5NaXJyb3Igb2YgXCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIjtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPlwiICsgZWwub3BlcmF0b3IgKyBcIjwvaDM+XCI7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGVsLmRlc2NyaXB0aW9uID09PSBcIlwiKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIlwiO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHN0ZXBOYW1lID0gZWwuZGVzY3JpcHRpb247XG4gICAgICAgICAgc3RlcE5hbWUgPSBzaG9ydGVuU3RyaW5nKHN0ZXBOYW1lKTtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg0IGNsYXNzPSdzdGVwLW5hbWUnPlwiICsgc3RlcE5hbWUgKyBcIjwvaDQ+XCI7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb24gIT0gbnVsbCkge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uKGVsLmlkLCBtYXhXLCBtYXhIKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBpZiAoaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKSkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5cIiArIGluZm8gKyBcIiBOb2RlPC9oNT5cIjtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLnBhcmFsbGVsaXNtICE9PSBcIlwiKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlBhcmFsbGVsaXNtOiBcIiArIGVsLnBhcmFsbGVsaXNtICsgXCI8L2g1PlwiO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwub3BlcmF0b3IgIT09IHVuZGVmaW5lZCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5PcGVyYXRpb246IFwiICsgc2hvcnRlblN0cmluZyhlbC5vcGVyYXRvcl9zdHJhdGVneSkgKyBcIjwvaDU+XCI7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2Rpdj5cIjtcbiAgICAgICAgcmV0dXJuIGxhYmVsVmFsdWU7XG4gICAgICB9O1xuICAgICAgZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uID0gZnVuY3Rpb24oaWQsIG1heFcsIG1heEgpIHtcbiAgICAgICAgdmFyIGxhYmVsVmFsdWUsIHN2Z0lEO1xuICAgICAgICBzdmdJRCA9IFwic3ZnLVwiICsgaWQ7XG4gICAgICAgIGxhYmVsVmFsdWUgPSBcIjxzdmcgY2xhc3M9J1wiICsgc3ZnSUQgKyBcIicgd2lkdGg9XCIgKyBtYXhXICsgXCIgaGVpZ2h0PVwiICsgbWF4SCArIFwiPjxnIC8+PC9zdmc+XCI7XG4gICAgICAgIHJldHVybiBsYWJlbFZhbHVlO1xuICAgICAgfTtcbiAgICAgIHNob3J0ZW5TdHJpbmcgPSBmdW5jdGlvbihzKSB7XG4gICAgICAgIHZhciBzYnI7XG4gICAgICAgIGlmIChzLmNoYXJBdCgwKSA9PT0gXCI8XCIpIHtcbiAgICAgICAgICBzID0gcy5yZXBsYWNlKFwiPFwiLCBcIiZsdDtcIik7XG4gICAgICAgICAgcyA9IHMucmVwbGFjZShcIj5cIiwgXCImZ3Q7XCIpO1xuICAgICAgICB9XG4gICAgICAgIHNiciA9IFwiXCI7XG4gICAgICAgIHdoaWxlIChzLmxlbmd0aCA+IDMwKSB7XG4gICAgICAgICAgc2JyID0gc2JyICsgcy5zdWJzdHJpbmcoMCwgMzApICsgXCI8YnI+XCI7XG4gICAgICAgICAgcyA9IHMuc3Vic3RyaW5nKDMwLCBzLmxlbmd0aCk7XG4gICAgICAgIH1cbiAgICAgICAgc2JyID0gc2JyICsgcztcbiAgICAgICAgcmV0dXJuIHNicjtcbiAgICAgIH07XG4gICAgICBjcmVhdGVOb2RlID0gZnVuY3Rpb24oZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKSB7XG4gICAgICAgIGlmIChpc1BhcmVudCA9PSBudWxsKSB7XG4gICAgICAgICAgaXNQYXJlbnQgPSBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoZWwuaWQgPT09IGRhdGEucGFydGlhbF9zb2x1dGlvbikge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwicGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLm5leHRfcGFydGlhbF9zb2x1dGlvbikge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEud29ya3NldCkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwid29ya3NldFwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJ3b3Jrc2V0XCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEubmV4dF93b3Jrc2V0KSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0V29ya3NldFwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0V29ya3NldFwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLnNvbHV0aW9uX3NldCkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwic29sdXRpb25TZXRcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25TZXRcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5zb2x1dGlvbl9kZWx0YSkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwic29sdXRpb25EZWx0YVwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwiXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBjcmVhdGVFZGdlID0gZnVuY3Rpb24oZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQpIHtcbiAgICAgICAgdmFyIG1pc3NpbmdOb2RlO1xuICAgICAgICBpZiAoZXhpc3RpbmdOb2Rlcy5pbmRleE9mKHByZWQuaWQpICE9PSAtMSkge1xuICAgICAgICAgIHJldHVybiBnLnNldEVkZ2UocHJlZC5pZCwgZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UocHJlZCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIGFycm93aGVhZDogJ25vcm1hbCdcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBtaXNzaW5nTm9kZSA9IHNlYXJjaEZvck5vZGUoZGF0YSwgcHJlZC5pZCk7XG4gICAgICAgICAgaWYgKCEhbWlzc2luZ05vZGUpIHtcbiAgICAgICAgICAgIHJldHVybiBnLnNldEVkZ2UobWlzc2luZ05vZGUuaWQsIGVsLmlkLCB7XG4gICAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UobWlzc2luZ05vZGUpLFxuICAgICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgbG9hZEpzb25Ub0RhZ3JlID0gZnVuY3Rpb24oZywgZGF0YSkge1xuICAgICAgICB2YXIgZWwsIGV4aXN0aW5nTm9kZXMsIGlzUGFyZW50LCBrLCBsLCBsZW4sIGxlbjEsIG1heEgsIG1heFcsIHByZWQsIHIsIHJlZiwgc2csIHRvSXRlcmF0ZTtcbiAgICAgICAgZXhpc3RpbmdOb2RlcyA9IFtdO1xuICAgICAgICBpZiAoZGF0YS5ub2RlcyAhPSBudWxsKSB7XG4gICAgICAgICAgdG9JdGVyYXRlID0gZGF0YS5ub2RlcztcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLnN0ZXBfZnVuY3Rpb247XG4gICAgICAgICAgaXNQYXJlbnQgPSB0cnVlO1xuICAgICAgICB9XG4gICAgICAgIGZvciAoayA9IDAsIGxlbiA9IHRvSXRlcmF0ZS5sZW5ndGg7IGsgPCBsZW47IGsrKykge1xuICAgICAgICAgIGVsID0gdG9JdGVyYXRlW2tdO1xuICAgICAgICAgIG1heFcgPSAwO1xuICAgICAgICAgIG1heEggPSAwO1xuICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uKSB7XG4gICAgICAgICAgICBzZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHtcbiAgICAgICAgICAgICAgbXVsdGlncmFwaDogdHJ1ZSxcbiAgICAgICAgICAgICAgY29tcG91bmQ6IHRydWVcbiAgICAgICAgICAgIH0pLnNldEdyYXBoKHtcbiAgICAgICAgICAgICAgbm9kZXNlcDogMjAsXG4gICAgICAgICAgICAgIGVkZ2VzZXA6IDAsXG4gICAgICAgICAgICAgIHJhbmtzZXA6IDIwLFxuICAgICAgICAgICAgICByYW5rZGlyOiBcIkxSXCIsXG4gICAgICAgICAgICAgIG1hcmdpbng6IDEwLFxuICAgICAgICAgICAgICBtYXJnaW55OiAxMFxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICBzdWJncmFwaHNbZWwuaWRdID0gc2c7XG4gICAgICAgICAgICBsb2FkSnNvblRvRGFncmUoc2csIGVsKTtcbiAgICAgICAgICAgIHIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKTtcbiAgICAgICAgICAgIGQzdG1wU3ZnLnNlbGVjdCgnZycpLmNhbGwociwgc2cpO1xuICAgICAgICAgICAgbWF4VyA9IHNnLmdyYXBoKCkud2lkdGg7XG4gICAgICAgICAgICBtYXhIID0gc2cuZ3JhcGgoKS5oZWlnaHQ7XG4gICAgICAgICAgICBhbmd1bGFyLmVsZW1lbnQobWFpblRtcEVsZW1lbnQpLmVtcHR5KCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGNyZWF0ZU5vZGUoZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKTtcbiAgICAgICAgICBleGlzdGluZ05vZGVzLnB1c2goZWwuaWQpO1xuICAgICAgICAgIGlmIChlbC5pbnB1dHMgIT0gbnVsbCkge1xuICAgICAgICAgICAgcmVmID0gZWwuaW5wdXRzO1xuICAgICAgICAgICAgZm9yIChsID0gMCwgbGVuMSA9IHJlZi5sZW5ndGg7IGwgPCBsZW4xOyBsKyspIHtcbiAgICAgICAgICAgICAgcHJlZCA9IHJlZltsXTtcbiAgICAgICAgICAgICAgY3JlYXRlRWRnZShnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZCk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgIHJldHVybiBnO1xuICAgICAgfTtcbiAgICAgIHNlYXJjaEZvck5vZGUgPSBmdW5jdGlvbihkYXRhLCBub2RlSUQpIHtcbiAgICAgICAgdmFyIGVsLCBpLCBqO1xuICAgICAgICBmb3IgKGkgaW4gZGF0YS5ub2Rlcykge1xuICAgICAgICAgIGVsID0gZGF0YS5ub2Rlc1tpXTtcbiAgICAgICAgICBpZiAoZWwuaWQgPT09IG5vZGVJRCkge1xuICAgICAgICAgICAgcmV0dXJuIGVsO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbiAhPSBudWxsKSB7XG4gICAgICAgICAgICBmb3IgKGogaW4gZWwuc3RlcF9mdW5jdGlvbikge1xuICAgICAgICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbltqXS5pZCA9PT0gbm9kZUlEKSB7XG4gICAgICAgICAgICAgICAgcmV0dXJuIGVsLnN0ZXBfZnVuY3Rpb25bal07XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBkcmF3R3JhcGggPSBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciBpLCBuZXdTY2FsZSwgcmVuZGVyZXIsIHNnLCB4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0O1xuICAgICAgICBnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoe1xuICAgICAgICAgIG11bHRpZ3JhcGg6IHRydWUsXG4gICAgICAgICAgY29tcG91bmQ6IHRydWVcbiAgICAgICAgfSkuc2V0R3JhcGgoe1xuICAgICAgICAgIG5vZGVzZXA6IDcwLFxuICAgICAgICAgIGVkZ2VzZXA6IDAsXG4gICAgICAgICAgcmFua3NlcDogNTAsXG4gICAgICAgICAgcmFua2RpcjogXCJMUlwiLFxuICAgICAgICAgIG1hcmdpbng6IDQwLFxuICAgICAgICAgIG1hcmdpbnk6IDQwXG4gICAgICAgIH0pO1xuICAgICAgICBsb2FkSnNvblRvRGFncmUoZywgZGF0YSk7XG4gICAgICAgIHJlbmRlcmVyID0gbmV3IGRhZ3JlRDMucmVuZGVyKCk7XG4gICAgICAgIGQzbWFpblN2Z0cuY2FsbChyZW5kZXJlciwgZyk7XG4gICAgICAgIGZvciAoaSBpbiBzdWJncmFwaHMpIHtcbiAgICAgICAgICBzZyA9IHN1YmdyYXBoc1tpXTtcbiAgICAgICAgICBkM21haW5Tdmcuc2VsZWN0KCdzdmcuc3ZnLScgKyBpICsgJyBnJykuY2FsbChyZW5kZXJlciwgc2cpO1xuICAgICAgICB9XG4gICAgICAgIG5ld1NjYWxlID0gMC41O1xuICAgICAgICB4Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS53aWR0aCgpIC0gZy5ncmFwaCgpLndpZHRoICogbmV3U2NhbGUpIC8gMik7XG4gICAgICAgIHlDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLmhlaWdodCgpIC0gZy5ncmFwaCgpLmhlaWdodCAqIG5ld1NjYWxlKSAvIDIpO1xuICAgICAgICBtYWluWm9vbS5zY2FsZShuZXdTY2FsZSkudHJhbnNsYXRlKFt4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0XSk7XG4gICAgICAgIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHhDZW50ZXJPZmZzZXQgKyBcIiwgXCIgKyB5Q2VudGVyT2Zmc2V0ICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKTtcbiAgICAgICAgbWFpblpvb20ub24oXCJ6b29tXCIsIGZ1bmN0aW9uKCkge1xuICAgICAgICAgIHZhciBldjtcbiAgICAgICAgICBldiA9IGQzLmV2ZW50O1xuICAgICAgICAgIHJldHVybiBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyBldi50cmFuc2xhdGUgKyBcIikgc2NhbGUoXCIgKyBldi5zY2FsZSArIFwiKVwiKTtcbiAgICAgICAgfSk7XG4gICAgICAgIG1haW5ab29tKGQzbWFpblN2Zyk7XG4gICAgICAgIHJldHVybiBkM21haW5TdmdHLnNlbGVjdEFsbCgnLm5vZGUnKS5vbignY2xpY2snLCBmdW5jdGlvbihkKSB7XG4gICAgICAgICAgcmV0dXJuIHNjb3BlLnNldE5vZGUoe1xuICAgICAgICAgICAgbm9kZWlkOiBkXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICAgIHNjb3BlLiR3YXRjaChhdHRycy5wbGFuLCBmdW5jdGlvbihuZXdQbGFuKSB7XG4gICAgICAgIGlmIChuZXdQbGFuKSB7XG4gICAgICAgICAgcmV0dXJuIGRyYXdHcmFwaChuZXdQbGFuKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuICB9O1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLnNlcnZpY2UgJ0pvYnNTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJGxvZywgYW1Nb21lbnQsICRxLCAkdGltZW91dCkgLT5cbiAgY3VycmVudEpvYiA9IG51bGxcbiAgY3VycmVudFBsYW4gPSBudWxsXG5cbiAgZGVmZXJyZWRzID0ge31cbiAgam9icyA9IHtcbiAgICBydW5uaW5nOiBbXVxuICAgIGZpbmlzaGVkOiBbXVxuICAgIGNhbmNlbGxlZDogW11cbiAgICBmYWlsZWQ6IFtdXG4gIH1cblxuICBqb2JPYnNlcnZlcnMgPSBbXVxuXG4gIG5vdGlmeU9ic2VydmVycyA9IC0+XG4gICAgYW5ndWxhci5mb3JFYWNoIGpvYk9ic2VydmVycywgKGNhbGxiYWNrKSAtPlxuICAgICAgY2FsbGJhY2soKVxuXG4gIEByZWdpc3Rlck9ic2VydmVyID0gKGNhbGxiYWNrKSAtPlxuICAgIGpvYk9ic2VydmVycy5wdXNoKGNhbGxiYWNrKVxuXG4gIEB1blJlZ2lzdGVyT2JzZXJ2ZXIgPSAoY2FsbGJhY2spIC0+XG4gICAgaW5kZXggPSBqb2JPYnNlcnZlcnMuaW5kZXhPZihjYWxsYmFjaylcbiAgICBqb2JPYnNlcnZlcnMuc3BsaWNlKGluZGV4LCAxKVxuXG4gIEBzdGF0ZUxpc3QgPSAtPlxuICAgIFsgXG4gICAgICAjICdDUkVBVEVEJ1xuICAgICAgJ1NDSEVEVUxFRCdcbiAgICAgICdERVBMT1lJTkcnXG4gICAgICAnUlVOTklORydcbiAgICAgICdGSU5JU0hFRCdcbiAgICAgICdGQUlMRUQnXG4gICAgICAnQ0FOQ0VMSU5HJ1xuICAgICAgJ0NBTkNFTEVEJ1xuICAgIF1cblxuICBAdHJhbnNsYXRlTGFiZWxTdGF0ZSA9IChzdGF0ZSkgLT5cbiAgICBzd2l0Y2ggc3RhdGUudG9Mb3dlckNhc2UoKVxuICAgICAgd2hlbiAnZmluaXNoZWQnIHRoZW4gJ3N1Y2Nlc3MnXG4gICAgICB3aGVuICdmYWlsZWQnIHRoZW4gJ2RhbmdlcidcbiAgICAgIHdoZW4gJ3NjaGVkdWxlZCcgdGhlbiAnZGVmYXVsdCdcbiAgICAgIHdoZW4gJ2RlcGxveWluZycgdGhlbiAnaW5mbydcbiAgICAgIHdoZW4gJ3J1bm5pbmcnIHRoZW4gJ3ByaW1hcnknXG4gICAgICB3aGVuICdjYW5jZWxpbmcnIHRoZW4gJ3dhcm5pbmcnXG4gICAgICB3aGVuICdwZW5kaW5nJyB0aGVuICdpbmZvJ1xuICAgICAgd2hlbiAndG90YWwnIHRoZW4gJ2JsYWNrJ1xuICAgICAgZWxzZSAnZGVmYXVsdCdcblxuICBAc2V0RW5kVGltZXMgPSAobGlzdCkgLT5cbiAgICBhbmd1bGFyLmZvckVhY2ggbGlzdCwgKGl0ZW0sIGpvYktleSkgLT5cbiAgICAgIHVubGVzcyBpdGVtWydlbmQtdGltZSddID4gLTFcbiAgICAgICAgaXRlbVsnZW5kLXRpbWUnXSA9IGl0ZW1bJ3N0YXJ0LXRpbWUnXSArIGl0ZW1bJ2R1cmF0aW9uJ11cblxuICBAcHJvY2Vzc1ZlcnRpY2VzID0gKGRhdGEpIC0+XG4gICAgYW5ndWxhci5mb3JFYWNoIGRhdGEudmVydGljZXMsICh2ZXJ0ZXgsIGkpIC0+XG4gICAgICB2ZXJ0ZXgudHlwZSA9ICdyZWd1bGFyJ1xuXG4gICAgZGF0YS52ZXJ0aWNlcy51bnNoaWZ0KHtcbiAgICAgIG5hbWU6ICdTY2hlZHVsZWQnXG4gICAgICAnc3RhcnQtdGltZSc6IGRhdGEudGltZXN0YW1wc1snQ1JFQVRFRCddXG4gICAgICAnZW5kLXRpbWUnOiBkYXRhLnRpbWVzdGFtcHNbJ0NSRUFURUQnXSArIDFcbiAgICAgIHR5cGU6ICdzY2hlZHVsZWQnXG4gICAgfSlcblxuICBAbGlzdEpvYnMgPSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9ib3ZlcnZpZXdcIlxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgPT5cbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLCAobGlzdCwgbGlzdEtleSkgPT5cbiAgICAgICAgc3dpdGNoIGxpc3RLZXlcbiAgICAgICAgICB3aGVuICdydW5uaW5nJyB0aGVuIGpvYnMucnVubmluZyA9IEBzZXRFbmRUaW1lcyhsaXN0KVxuICAgICAgICAgIHdoZW4gJ2ZpbmlzaGVkJyB0aGVuIGpvYnMuZmluaXNoZWQgPSBAc2V0RW5kVGltZXMobGlzdClcbiAgICAgICAgICB3aGVuICdjYW5jZWxsZWQnIHRoZW4gam9icy5jYW5jZWxsZWQgPSBAc2V0RW5kVGltZXMobGlzdClcbiAgICAgICAgICB3aGVuICdmYWlsZWQnIHRoZW4gam9icy5mYWlsZWQgPSBAc2V0RW5kVGltZXMobGlzdClcblxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShqb2JzKVxuICAgICAgbm90aWZ5T2JzZXJ2ZXJzKClcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAZ2V0Sm9icyA9ICh0eXBlKSAtPlxuICAgIGpvYnNbdHlwZV1cblxuICBAZ2V0QWxsSm9icyA9IC0+XG4gICAgam9ic1xuXG4gIEBsb2FkSm9iID0gKGpvYmlkKSAtPlxuICAgIGN1cnJlbnRKb2IgPSBudWxsXG4gICAgZGVmZXJyZWRzLmpvYiA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZFxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgPT5cbiAgICAgIEBzZXRFbmRUaW1lcyhkYXRhLnZlcnRpY2VzKVxuICAgICAgQHByb2Nlc3NWZXJ0aWNlcyhkYXRhKVxuXG4gICAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWQgKyBcIi9jb25maWdcIlxuICAgICAgLnN1Y2Nlc3MgKGpvYkNvbmZpZykgLT5cbiAgICAgICAgZGF0YSA9IGFuZ3VsYXIuZXh0ZW5kKGRhdGEsIGpvYkNvbmZpZylcblxuICAgICAgICBjdXJyZW50Sm9iID0gZGF0YVxuXG4gICAgICAgIGRlZmVycmVkcy5qb2IucmVzb2x2ZShjdXJyZW50Sm9iKVxuXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlXG5cbiAgQGdldE5vZGUgPSAobm9kZWlkKSAtPlxuICAgIHNlZWtOb2RlID0gKG5vZGVpZCwgZGF0YSkgLT5cbiAgICAgIGZvciBub2RlIGluIGRhdGFcbiAgICAgICAgcmV0dXJuIG5vZGUgaWYgbm9kZS5pZCBpcyBub2RlaWRcbiAgICAgICAgc3ViID0gc2Vla05vZGUobm9kZWlkLCBub2RlLnN0ZXBfZnVuY3Rpb24pIGlmIG5vZGUuc3RlcF9mdW5jdGlvblxuICAgICAgICByZXR1cm4gc3ViIGlmIHN1YlxuXG4gICAgICBudWxsXG5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxuICAgICAgZm91bmROb2RlID0gc2Vla05vZGUobm9kZWlkLCBjdXJyZW50Sm9iLnBsYW4ubm9kZXMpXG5cbiAgICAgIGZvdW5kTm9kZS52ZXJ0ZXggPSBAc2Vla1ZlcnRleChub2RlaWQpXG5cbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZm91bmROb2RlKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBzZWVrVmVydGV4ID0gKG5vZGVpZCkgLT5cbiAgICBmb3IgdmVydGV4IGluIGN1cnJlbnRKb2IudmVydGljZXNcbiAgICAgIHJldHVybiB2ZXJ0ZXggaWYgdmVydGV4LmlkIGlzIG5vZGVpZFxuXG4gICAgcmV0dXJuIG51bGxcblxuICBAZ2V0VmVydGV4ID0gKHZlcnRleGlkKSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4gKGRhdGEpID0+XG4gICAgICB2ZXJ0ZXggPSBAc2Vla1ZlcnRleCh2ZXJ0ZXhpZClcblxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL3N1YnRhc2t0aW1lc1wiXG4gICAgICAuc3VjY2VzcyAoZGF0YSkgPT5cbiAgICAgICAgIyBUT0RPOiBjaGFuZ2UgdG8gc3VidGFza3RpbWVzXG4gICAgICAgIHZlcnRleC5zdWJ0YXNrcyA9IGRhdGEuc3VidGFza3NcblxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHZlcnRleClcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAZ2V0U3VidGFza3MgPSAodmVydGV4aWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cbiAgICAgICMgdmVydGV4ID0gQHNlZWtWZXJ0ZXgodmVydGV4aWQpXG5cbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWRcbiAgICAgIC5zdWNjZXNzIChkYXRhKSAtPlxuICAgICAgICBzdWJ0YXNrcyA9IGRhdGEuc3VidGFza3NcblxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHN1YnRhc2tzKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBnZXRUYXNrTWFuYWdlcnMgPSAodmVydGV4aWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cbiAgICAgICMgdmVydGV4ID0gQHNlZWtWZXJ0ZXgodmVydGV4aWQpXG5cbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi90YXNrbWFuYWdlcnNcIlxuICAgICAgLnN1Y2Nlc3MgKGRhdGEpIC0+XG4gICAgICAgIHRhc2ttYW5hZ2VycyA9IGRhdGEudGFza21hbmFnZXJzXG5cbiAgICAgICAgZGVmZXJyZWQucmVzb2x2ZSh0YXNrbWFuYWdlcnMpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQGdldEFjY3VtdWxhdG9ycyA9ICh2ZXJ0ZXhpZCkgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxuICAgICAgIyB2ZXJ0ZXggPSBAc2Vla1ZlcnRleCh2ZXJ0ZXhpZClcblxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL2FjY3VtdWxhdG9yc1wiXG4gICAgICAuc3VjY2VzcyAoZGF0YSkgLT5cbiAgICAgICAgYWNjdW11bGF0b3JzID0gZGF0YVsndXNlci1hY2N1bXVsYXRvcnMnXVxuXG4gICAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9zdWJ0YXNrcy9hY2N1bXVsYXRvcnNcIlxuICAgICAgICAuc3VjY2VzcyAoZGF0YSkgLT5cbiAgICAgICAgICBzdWJ0YXNrQWNjdW11bGF0b3JzID0gZGF0YS5zdWJ0YXNrc1xuXG4gICAgICAgICAgZGVmZXJyZWQucmVzb2x2ZSh7IG1haW46IGFjY3VtdWxhdG9ycywgc3VidGFza3M6IHN1YnRhc2tBY2N1bXVsYXRvcnMgfSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICAjIEpvYi1sZXZlbCBjaGVja3BvaW50IHN0YXRzXG4gIEBnZXRKb2JDaGVja3BvaW50U3RhdHMgPSAoam9iaWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWQgKyBcIi9jaGVja3BvaW50c1wiXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSA9PlxuICAgICAgaWYgKGFuZ3VsYXIuZXF1YWxzKHt9LCBkYXRhKSlcbiAgICAgICAgZGVmZXJyZWQucmVzb2x2ZShkZWZlcnJlZC5yZXNvbHZlKG51bGwpKVxuICAgICAgZWxzZVxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgIyBPcGVyYXRvci1sZXZlbCBjaGVja3BvaW50IHN0YXRzXG4gIEBnZXRPcGVyYXRvckNoZWNrcG9pbnRTdGF0cyA9ICh2ZXJ0ZXhpZCkgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL2NoZWNrcG9pbnRzXCJcbiAgICAgIC5zdWNjZXNzIChkYXRhKSAtPlxuICAgICAgICAjIElmIG5vIGRhdGEgYXZhaWxhYmxlLCB3ZSBhcmUgZG9uZS5cbiAgICAgICAgaWYgKGFuZ3VsYXIuZXF1YWxzKHt9LCBkYXRhKSlcbiAgICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHsgb3BlcmF0b3JTdGF0czogbnVsbCwgc3VidGFza3NTdGF0czogbnVsbCB9KVxuICAgICAgICBlbHNlXG4gICAgICAgICAgb3BlcmF0b3JTdGF0cyA9IHsgaWQ6IGRhdGFbJ2lkJ10sIHRpbWVzdGFtcDogZGF0YVsndGltZXN0YW1wJ10sIGR1cmF0aW9uOiBkYXRhWydkdXJhdGlvbiddLCBzaXplOiBkYXRhWydzaXplJ10gfVxuXG4gICAgICAgICAgaWYgKGFuZ3VsYXIuZXF1YWxzKHt9LCBkYXRhWydzdWJ0YXNrcyddKSlcbiAgICAgICAgICAgIGRlZmVycmVkLnJlc29sdmUoeyBvcGVyYXRvclN0YXRzOiBvcGVyYXRvclN0YXRzLCBzdWJ0YXNrc1N0YXRzOiBudWxsIH0pXG4gICAgICAgICAgZWxzZVxuICAgICAgICAgICAgc3VidGFza1N0YXRzID0gZGF0YVsnc3VidGFza3MnXVxuICAgICAgICAgICAgZGVmZXJyZWQucmVzb2x2ZSh7IG9wZXJhdG9yU3RhdHM6IG9wZXJhdG9yU3RhdHMsIHN1YnRhc2tzU3RhdHM6IHN1YnRhc2tTdGF0cyB9KVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gICMgT3BlcmF0b3ItbGV2ZWwgYmFjayBwcmVzc3VyZSBzdGF0c1xuICBAZ2V0T3BlcmF0b3JCYWNrUHJlc3N1cmUgPSAodmVydGV4aWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvYmFja3ByZXNzdXJlXCJcbiAgICAuc3VjY2VzcyAoZGF0YSkgPT5cbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAdHJhbnNsYXRlQmFja1ByZXNzdXJlTGFiZWxTdGF0ZSA9IChzdGF0ZSkgLT5cbiAgICBzd2l0Y2ggc3RhdGUudG9Mb3dlckNhc2UoKVxuICAgICAgd2hlbiAnaW4tcHJvZ3Jlc3MnIHRoZW4gJ2RhbmdlcidcbiAgICAgIHdoZW4gJ29rJyB0aGVuICdzdWNjZXNzJ1xuICAgICAgd2hlbiAnbG93JyB0aGVuICd3YXJuaW5nJ1xuICAgICAgd2hlbiAnaGlnaCcgdGhlbiAnZGFuZ2VyJ1xuICAgICAgZWxzZSAnZGVmYXVsdCdcblxuICBAbG9hZEV4Y2VwdGlvbnMgPSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4gKGRhdGEpID0+XG5cbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2V4Y2VwdGlvbnNcIlxuICAgICAgLnN1Y2Nlc3MgKGV4Y2VwdGlvbnMpIC0+XG4gICAgICAgIGN1cnJlbnRKb2IuZXhjZXB0aW9ucyA9IGV4Y2VwdGlvbnNcblxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGV4Y2VwdGlvbnMpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQGNhbmNlbEpvYiA9IChqb2JpZCkgLT5cbiAgICAjIHVzZXMgdGhlIG5vbiBSRVNULWNvbXBsaWFudCBHRVQgeWFybi1jYW5jZWwgaGFuZGxlciB3aGljaCBpcyBhdmFpbGFibGUgaW4gYWRkaXRpb24gdG8gdGhlXG4gICAgIyBwcm9wZXIgXCJERUxFVEUgam9icy88am9iaWQ+L1wiXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkICsgXCIveWFybi1jYW5jZWxcIlxuXG4gIEBzdG9wSm9iID0gKGpvYmlkKSAtPlxuICAgICMgdXNlcyB0aGUgbm9uIFJFU1QtY29tcGxpYW50IEdFVCB5YXJuLWNhbmNlbCBoYW5kbGVyIHdoaWNoIGlzIGF2YWlsYWJsZSBpbiBhZGRpdGlvbiB0byB0aGVcbiAgICAjIHByb3BlciBcIkRFTEVURSBqb2JzLzxqb2JpZD4vXCJcbiAgICAkaHR0cC5nZXQgXCJqb2JzL1wiICsgam9iaWQgKyBcIi95YXJuLXN0b3BcIlxuXG4gIEBcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ0pvYnNTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkbG9nLCBhbU1vbWVudCwgJHEsICR0aW1lb3V0KSB7XG4gIHZhciBjdXJyZW50Sm9iLCBjdXJyZW50UGxhbiwgZGVmZXJyZWRzLCBqb2JPYnNlcnZlcnMsIGpvYnMsIG5vdGlmeU9ic2VydmVycztcbiAgY3VycmVudEpvYiA9IG51bGw7XG4gIGN1cnJlbnRQbGFuID0gbnVsbDtcbiAgZGVmZXJyZWRzID0ge307XG4gIGpvYnMgPSB7XG4gICAgcnVubmluZzogW10sXG4gICAgZmluaXNoZWQ6IFtdLFxuICAgIGNhbmNlbGxlZDogW10sXG4gICAgZmFpbGVkOiBbXVxuICB9O1xuICBqb2JPYnNlcnZlcnMgPSBbXTtcbiAgbm90aWZ5T2JzZXJ2ZXJzID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIGFuZ3VsYXIuZm9yRWFjaChqb2JPYnNlcnZlcnMsIGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgICByZXR1cm4gY2FsbGJhY2soKTtcbiAgICB9KTtcbiAgfTtcbiAgdGhpcy5yZWdpc3Rlck9ic2VydmVyID0gZnVuY3Rpb24oY2FsbGJhY2spIHtcbiAgICByZXR1cm4gam9iT2JzZXJ2ZXJzLnB1c2goY2FsbGJhY2spO1xuICB9O1xuICB0aGlzLnVuUmVnaXN0ZXJPYnNlcnZlciA9IGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgdmFyIGluZGV4O1xuICAgIGluZGV4ID0gam9iT2JzZXJ2ZXJzLmluZGV4T2YoY2FsbGJhY2spO1xuICAgIHJldHVybiBqb2JPYnNlcnZlcnMuc3BsaWNlKGluZGV4LCAxKTtcbiAgfTtcbiAgdGhpcy5zdGF0ZUxpc3QgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gWydTQ0hFRFVMRUQnLCAnREVQTE9ZSU5HJywgJ1JVTk5JTkcnLCAnRklOSVNIRUQnLCAnRkFJTEVEJywgJ0NBTkNFTElORycsICdDQU5DRUxFRCddO1xuICB9O1xuICB0aGlzLnRyYW5zbGF0ZUxhYmVsU3RhdGUgPSBmdW5jdGlvbihzdGF0ZSkge1xuICAgIHN3aXRjaCAoc3RhdGUudG9Mb3dlckNhc2UoKSkge1xuICAgICAgY2FzZSAnZmluaXNoZWQnOlxuICAgICAgICByZXR1cm4gJ3N1Y2Nlc3MnO1xuICAgICAgY2FzZSAnZmFpbGVkJzpcbiAgICAgICAgcmV0dXJuICdkYW5nZXInO1xuICAgICAgY2FzZSAnc2NoZWR1bGVkJzpcbiAgICAgICAgcmV0dXJuICdkZWZhdWx0JztcbiAgICAgIGNhc2UgJ2RlcGxveWluZyc6XG4gICAgICAgIHJldHVybiAnaW5mbyc7XG4gICAgICBjYXNlICdydW5uaW5nJzpcbiAgICAgICAgcmV0dXJuICdwcmltYXJ5JztcbiAgICAgIGNhc2UgJ2NhbmNlbGluZyc6XG4gICAgICAgIHJldHVybiAnd2FybmluZyc7XG4gICAgICBjYXNlICdwZW5kaW5nJzpcbiAgICAgICAgcmV0dXJuICdpbmZvJztcbiAgICAgIGNhc2UgJ3RvdGFsJzpcbiAgICAgICAgcmV0dXJuICdibGFjayc7XG4gICAgICBkZWZhdWx0OlxuICAgICAgICByZXR1cm4gJ2RlZmF1bHQnO1xuICAgIH1cbiAgfTtcbiAgdGhpcy5zZXRFbmRUaW1lcyA9IGZ1bmN0aW9uKGxpc3QpIHtcbiAgICByZXR1cm4gYW5ndWxhci5mb3JFYWNoKGxpc3QsIGZ1bmN0aW9uKGl0ZW0sIGpvYktleSkge1xuICAgICAgaWYgKCEoaXRlbVsnZW5kLXRpbWUnXSA+IC0xKSkge1xuICAgICAgICByZXR1cm4gaXRlbVsnZW5kLXRpbWUnXSA9IGl0ZW1bJ3N0YXJ0LXRpbWUnXSArIGl0ZW1bJ2R1cmF0aW9uJ107XG4gICAgICB9XG4gICAgfSk7XG4gIH07XG4gIHRoaXMucHJvY2Vzc1ZlcnRpY2VzID0gZnVuY3Rpb24oZGF0YSkge1xuICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLnZlcnRpY2VzLCBmdW5jdGlvbih2ZXJ0ZXgsIGkpIHtcbiAgICAgIHJldHVybiB2ZXJ0ZXgudHlwZSA9ICdyZWd1bGFyJztcbiAgICB9KTtcbiAgICByZXR1cm4gZGF0YS52ZXJ0aWNlcy51bnNoaWZ0KHtcbiAgICAgIG5hbWU6ICdTY2hlZHVsZWQnLFxuICAgICAgJ3N0YXJ0LXRpbWUnOiBkYXRhLnRpbWVzdGFtcHNbJ0NSRUFURUQnXSxcbiAgICAgICdlbmQtdGltZSc6IGRhdGEudGltZXN0YW1wc1snQ1JFQVRFRCddICsgMSxcbiAgICAgIHR5cGU6ICdzY2hlZHVsZWQnXG4gICAgfSk7XG4gIH07XG4gIHRoaXMubGlzdEpvYnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm92ZXJ2aWV3XCIpLnN1Y2Nlc3MoKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgICAgYW5ndWxhci5mb3JFYWNoKGRhdGEsIGZ1bmN0aW9uKGxpc3QsIGxpc3RLZXkpIHtcbiAgICAgICAgICBzd2l0Y2ggKGxpc3RLZXkpIHtcbiAgICAgICAgICAgIGNhc2UgJ3J1bm5pbmcnOlxuICAgICAgICAgICAgICByZXR1cm4gam9icy5ydW5uaW5nID0gX3RoaXMuc2V0RW5kVGltZXMobGlzdCk7XG4gICAgICAgICAgICBjYXNlICdmaW5pc2hlZCc6XG4gICAgICAgICAgICAgIHJldHVybiBqb2JzLmZpbmlzaGVkID0gX3RoaXMuc2V0RW5kVGltZXMobGlzdCk7XG4gICAgICAgICAgICBjYXNlICdjYW5jZWxsZWQnOlxuICAgICAgICAgICAgICByZXR1cm4gam9icy5jYW5jZWxsZWQgPSBfdGhpcy5zZXRFbmRUaW1lcyhsaXN0KTtcbiAgICAgICAgICAgIGNhc2UgJ2ZhaWxlZCc6XG4gICAgICAgICAgICAgIHJldHVybiBqb2JzLmZhaWxlZCA9IF90aGlzLnNldEVuZFRpbWVzKGxpc3QpO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUoam9icyk7XG4gICAgICAgIHJldHVybiBub3RpZnlPYnNlcnZlcnMoKTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldEpvYnMgPSBmdW5jdGlvbih0eXBlKSB7XG4gICAgcmV0dXJuIGpvYnNbdHlwZV07XG4gIH07XG4gIHRoaXMuZ2V0QWxsSm9icyA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBqb2JzO1xuICB9O1xuICB0aGlzLmxvYWRKb2IgPSBmdW5jdGlvbihqb2JpZCkge1xuICAgIGN1cnJlbnRKb2IgPSBudWxsO1xuICAgIGRlZmVycmVkcy5qb2IgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCkuc3VjY2VzcygoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgICBfdGhpcy5zZXRFbmRUaW1lcyhkYXRhLnZlcnRpY2VzKTtcbiAgICAgICAgX3RoaXMucHJvY2Vzc1ZlcnRpY2VzKGRhdGEpO1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkICsgXCIvY29uZmlnXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oam9iQ29uZmlnKSB7XG4gICAgICAgICAgZGF0YSA9IGFuZ3VsYXIuZXh0ZW5kKGRhdGEsIGpvYkNvbmZpZyk7XG4gICAgICAgICAgY3VycmVudEpvYiA9IGRhdGE7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkcy5qb2IucmVzb2x2ZShjdXJyZW50Sm9iKTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWRzLmpvYi5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldE5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICB2YXIgZGVmZXJyZWQsIHNlZWtOb2RlO1xuICAgIHNlZWtOb2RlID0gZnVuY3Rpb24obm9kZWlkLCBkYXRhKSB7XG4gICAgICB2YXIgaiwgbGVuLCBub2RlLCBzdWI7XG4gICAgICBmb3IgKGogPSAwLCBsZW4gPSBkYXRhLmxlbmd0aDsgaiA8IGxlbjsgaisrKSB7XG4gICAgICAgIG5vZGUgPSBkYXRhW2pdO1xuICAgICAgICBpZiAobm9kZS5pZCA9PT0gbm9kZWlkKSB7XG4gICAgICAgICAgcmV0dXJuIG5vZGU7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKG5vZGUuc3RlcF9mdW5jdGlvbikge1xuICAgICAgICAgIHN1YiA9IHNlZWtOb2RlKG5vZGVpZCwgbm9kZS5zdGVwX2Z1bmN0aW9uKTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoc3ViKSB7XG4gICAgICAgICAgcmV0dXJuIHN1YjtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfTtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgZm91bmROb2RlO1xuICAgICAgICBmb3VuZE5vZGUgPSBzZWVrTm9kZShub2RlaWQsIGN1cnJlbnRKb2IucGxhbi5ub2Rlcyk7XG4gICAgICAgIGZvdW5kTm9kZS52ZXJ0ZXggPSBfdGhpcy5zZWVrVmVydGV4KG5vZGVpZCk7XG4gICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGZvdW5kTm9kZSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5zZWVrVmVydGV4ID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgdmFyIGosIGxlbiwgcmVmLCB2ZXJ0ZXg7XG4gICAgcmVmID0gY3VycmVudEpvYi52ZXJ0aWNlcztcbiAgICBmb3IgKGogPSAwLCBsZW4gPSByZWYubGVuZ3RoOyBqIDwgbGVuOyBqKyspIHtcbiAgICAgIHZlcnRleCA9IHJlZltqXTtcbiAgICAgIGlmICh2ZXJ0ZXguaWQgPT09IG5vZGVpZCkge1xuICAgICAgICByZXR1cm4gdmVydGV4O1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfTtcbiAgdGhpcy5nZXRWZXJ0ZXggPSBmdW5jdGlvbih2ZXJ0ZXhpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgdmVydGV4O1xuICAgICAgICB2ZXJ0ZXggPSBfdGhpcy5zZWVrVmVydGV4KHZlcnRleGlkKTtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9zdWJ0YXNrdGltZXNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICAgdmVydGV4LnN1YnRhc2tzID0gZGF0YS5zdWJ0YXNrcztcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZSh2ZXJ0ZXgpO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldFN1YnRhc2tzID0gZnVuY3Rpb24odmVydGV4aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIHZhciBzdWJ0YXNrcztcbiAgICAgICAgICBzdWJ0YXNrcyA9IGRhdGEuc3VidGFza3M7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoc3VidGFza3MpO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldFRhc2tNYW5hZ2VycyA9IGZ1bmN0aW9uKHZlcnRleGlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvdGFza21hbmFnZXJzXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIHZhciB0YXNrbWFuYWdlcnM7XG4gICAgICAgICAgdGFza21hbmFnZXJzID0gZGF0YS50YXNrbWFuYWdlcnM7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUodGFza21hbmFnZXJzKTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRBY2N1bXVsYXRvcnMgPSBmdW5jdGlvbih2ZXJ0ZXhpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL2FjY3VtdWxhdG9yc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICB2YXIgYWNjdW11bGF0b3JzO1xuICAgICAgICAgIGFjY3VtdWxhdG9ycyA9IGRhdGFbJ3VzZXItYWNjdW11bGF0b3JzJ107XG4gICAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9zdWJ0YXNrcy9hY2N1bXVsYXRvcnNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICAgICB2YXIgc3VidGFza0FjY3VtdWxhdG9ycztcbiAgICAgICAgICAgIHN1YnRhc2tBY2N1bXVsYXRvcnMgPSBkYXRhLnN1YnRhc2tzO1xuICAgICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoe1xuICAgICAgICAgICAgICBtYWluOiBhY2N1bXVsYXRvcnMsXG4gICAgICAgICAgICAgIHN1YnRhc2tzOiBzdWJ0YXNrQWNjdW11bGF0b3JzXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRKb2JDaGVja3BvaW50U3RhdHMgPSBmdW5jdGlvbihqb2JpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkICsgXCIvY2hlY2twb2ludHNcIikuc3VjY2VzcygoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgICBpZiAoYW5ndWxhci5lcXVhbHMoe30sIGRhdGEpKSB7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGVmZXJyZWQucmVzb2x2ZShudWxsKSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldE9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gZnVuY3Rpb24odmVydGV4aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9jaGVja3BvaW50c1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICB2YXIgb3BlcmF0b3JTdGF0cywgc3VidGFza1N0YXRzO1xuICAgICAgICAgIGlmIChhbmd1bGFyLmVxdWFscyh7fSwgZGF0YSkpIHtcbiAgICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKHtcbiAgICAgICAgICAgICAgb3BlcmF0b3JTdGF0czogbnVsbCxcbiAgICAgICAgICAgICAgc3VidGFza3NTdGF0czogbnVsbFxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIG9wZXJhdG9yU3RhdHMgPSB7XG4gICAgICAgICAgICAgIGlkOiBkYXRhWydpZCddLFxuICAgICAgICAgICAgICB0aW1lc3RhbXA6IGRhdGFbJ3RpbWVzdGFtcCddLFxuICAgICAgICAgICAgICBkdXJhdGlvbjogZGF0YVsnZHVyYXRpb24nXSxcbiAgICAgICAgICAgICAgc2l6ZTogZGF0YVsnc2l6ZSddXG4gICAgICAgICAgICB9O1xuICAgICAgICAgICAgaWYgKGFuZ3VsYXIuZXF1YWxzKHt9LCBkYXRhWydzdWJ0YXNrcyddKSkge1xuICAgICAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZSh7XG4gICAgICAgICAgICAgICAgb3BlcmF0b3JTdGF0czogb3BlcmF0b3JTdGF0cyxcbiAgICAgICAgICAgICAgICBzdWJ0YXNrc1N0YXRzOiBudWxsXG4gICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgc3VidGFza1N0YXRzID0gZGF0YVsnc3VidGFza3MnXTtcbiAgICAgICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoe1xuICAgICAgICAgICAgICAgIG9wZXJhdG9yU3RhdHM6IG9wZXJhdG9yU3RhdHMsXG4gICAgICAgICAgICAgICAgc3VidGFza3NTdGF0czogc3VidGFza1N0YXRzXG4gICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRPcGVyYXRvckJhY2tQcmVzc3VyZSA9IGZ1bmN0aW9uKHZlcnRleGlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvYmFja3ByZXNzdXJlXCIpLnN1Y2Nlc3MoKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLnRyYW5zbGF0ZUJhY2tQcmVzc3VyZUxhYmVsU3RhdGUgPSBmdW5jdGlvbihzdGF0ZSkge1xuICAgIHN3aXRjaCAoc3RhdGUudG9Mb3dlckNhc2UoKSkge1xuICAgICAgY2FzZSAnaW4tcHJvZ3Jlc3MnOlxuICAgICAgICByZXR1cm4gJ2Rhbmdlcic7XG4gICAgICBjYXNlICdvayc6XG4gICAgICAgIHJldHVybiAnc3VjY2Vzcyc7XG4gICAgICBjYXNlICdsb3cnOlxuICAgICAgICByZXR1cm4gJ3dhcm5pbmcnO1xuICAgICAgY2FzZSAnaGlnaCc6XG4gICAgICAgIHJldHVybiAnZGFuZ2VyJztcbiAgICAgIGRlZmF1bHQ6XG4gICAgICAgIHJldHVybiAnZGVmYXVsdCc7XG4gICAgfVxuICB9O1xuICB0aGlzLmxvYWRFeGNlcHRpb25zID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi9leGNlcHRpb25zXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZXhjZXB0aW9ucykge1xuICAgICAgICAgIGN1cnJlbnRKb2IuZXhjZXB0aW9ucyA9IGV4Y2VwdGlvbnM7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZXhjZXB0aW9ucyk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuY2FuY2VsSm9iID0gZnVuY3Rpb24oam9iaWQpIHtcbiAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkICsgXCIveWFybi1jYW5jZWxcIik7XG4gIH07XG4gIHRoaXMuc3RvcEpvYiA9IGZ1bmN0aW9uKGpvYmlkKSB7XG4gICAgcmV0dXJuICRodHRwLmdldChcImpvYnMvXCIgKyBqb2JpZCArIFwiL3lhcm4tc3RvcFwiKTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uY29udHJvbGxlciAnQWxsVGFza01hbmFnZXJzQ29udHJvbGxlcicsICgkc2NvcGUsIFRhc2tNYW5hZ2Vyc1NlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIC0+XG4gIFRhc2tNYW5hZ2Vyc1NlcnZpY2UubG9hZE1hbmFnZXJzKCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUubWFuYWdlcnMgPSBkYXRhXG5cbiAgcmVmcmVzaCA9ICRpbnRlcnZhbCAtPlxuICAgIFRhc2tNYW5hZ2Vyc1NlcnZpY2UubG9hZE1hbmFnZXJzKCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5tYW5hZ2VycyA9IGRhdGFcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cblxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxuXG4uY29udHJvbGxlciAnU2luZ2xlVGFza01hbmFnZXJDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlUGFyYW1zLCBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIC0+XG4gICRzY29wZS5tZXRyaWNzID0ge31cbiAgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRNZXRyaWNzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLm1ldHJpY3MgPSBkYXRhWzBdXG5cbiAgICByZWZyZXNoID0gJGludGVydmFsIC0+XG4gICAgICBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UubG9hZE1ldHJpY3MoJHN0YXRlUGFyYW1zLnRhc2ttYW5hZ2VyaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAgICRzY29wZS5tZXRyaWNzID0gZGF0YVswXVxuICAgICwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdXG5cbiAgICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgICAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpXG5cbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ0FsbFRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIFRhc2tNYW5hZ2Vyc1NlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIHtcbiAgdmFyIHJlZnJlc2g7XG4gIFRhc2tNYW5hZ2Vyc1NlcnZpY2UubG9hZE1hbmFnZXJzKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5tYW5hZ2VycyA9IGRhdGE7XG4gIH0pO1xuICByZWZyZXNoID0gJGludGVydmFsKGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBUYXNrTWFuYWdlcnNTZXJ2aWNlLmxvYWRNYW5hZ2VycygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5tYW5hZ2VycyA9IGRhdGE7XG4gICAgfSk7XG4gIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIHJldHVybiAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ1NpbmdsZVRhc2tNYW5hZ2VyQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlUGFyYW1zLCBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIHtcbiAgdmFyIHJlZnJlc2g7XG4gICRzY29wZS5tZXRyaWNzID0ge307XG4gIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZS5sb2FkTWV0cmljcygkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5tZXRyaWNzID0gZGF0YVswXTtcbiAgfSk7XG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZS5sb2FkTWV0cmljcygkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLm1ldHJpY3MgPSBkYXRhWzBdO1xuICAgIH0pO1xuICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICByZXR1cm4gJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJGludGVydmFsLmNhbmNlbChyZWZyZXNoKTtcbiAgfSk7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnVGFza01hbmFnZXJzU2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxuICBAbG9hZE1hbmFnZXJzID0gKCkgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcInRhc2ttYW5hZ2Vyc1wiKVxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YVsndGFza21hbmFnZXJzJ10pXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQFxuXG4uc2VydmljZSAnU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG4gIEBsb2FkTWV0cmljcyA9ICh0YXNrbWFuYWdlcmlkKSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwidGFza21hbmFnZXJzL1wiICsgdGFza21hbmFnZXJpZClcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGFbJ3Rhc2ttYW5hZ2VycyddKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBcblxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnVGFza01hbmFnZXJzU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdGhpcy5sb2FkTWFuYWdlcnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcInRhc2ttYW5hZ2Vyc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhWyd0YXNrbWFuYWdlcnMnXSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSkuc2VydmljZSgnU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkge1xuICB0aGlzLmxvYWRNZXRyaWNzID0gZnVuY3Rpb24odGFza21hbmFnZXJpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwidGFza21hbmFnZXJzL1wiICsgdGFza21hbmFnZXJpZCkuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YVsndGFza21hbmFnZXJzJ10pO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5jb250cm9sbGVyICdKb2JTdWJtaXRDb250cm9sbGVyJywgKCRzY29wZSwgSm9iU3VibWl0U2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZywgJHN0YXRlLCAkbG9jYXRpb24pIC0+XG4gICRzY29wZS55YXJuID0gJGxvY2F0aW9uLmFic1VybCgpLmluZGV4T2YoXCIvcHJveHkvYXBwbGljYXRpb25fXCIpICE9IC0xXG4gICRzY29wZS5sb2FkTGlzdCA9ICgpIC0+XG4gICAgSm9iU3VibWl0U2VydmljZS5sb2FkSmFyTGlzdCgpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUuYWRkcmVzcyA9IGRhdGEuYWRkcmVzc1xuICAgICAgJHNjb3BlLm5vYWNjZXNzID0gZGF0YS5lcnJvclxuICAgICAgJHNjb3BlLmphcnMgPSBkYXRhLmZpbGVzXG5cbiAgJHNjb3BlLmRlZmF1bHRTdGF0ZSA9ICgpIC0+XG4gICAgJHNjb3BlLnBsYW4gPSBudWxsXG4gICAgJHNjb3BlLmVycm9yID0gbnVsbFxuICAgICRzY29wZS5zdGF0ZSA9IHtcbiAgICAgIHNlbGVjdGVkOiBudWxsLFxuICAgICAgcGFyYWxsZWxpc206IFwiXCIsXG4gICAgICAnZW50cnktY2xhc3MnOiBcIlwiLFxuICAgICAgJ3Byb2dyYW0tYXJncyc6IFwiXCIsXG4gICAgICAncGxhbi1idXR0b24nOiBcIlNob3cgUGxhblwiLFxuICAgICAgJ3N1Ym1pdC1idXR0b24nOiBcIlN1Ym1pdFwiLFxuICAgICAgJ2FjdGlvbi10aW1lJzogMFxuICAgIH1cblxuICAkc2NvcGUuZGVmYXVsdFN0YXRlKClcbiAgJHNjb3BlLnVwbG9hZGVyID0ge31cbiAgJHNjb3BlLmxvYWRMaXN0KClcblxuICByZWZyZXNoID0gJGludGVydmFsIC0+XG4gICAgJHNjb3BlLmxvYWRMaXN0KClcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cblxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxuXG4gICRzY29wZS5zZWxlY3RKYXIgPSAoaWQpIC0+XG4gICAgaWYgJHNjb3BlLnN0YXRlLnNlbGVjdGVkID09IGlkXG4gICAgICAkc2NvcGUuZGVmYXVsdFN0YXRlKClcbiAgICBlbHNlXG4gICAgICAkc2NvcGUuZGVmYXVsdFN0YXRlKClcbiAgICAgICRzY29wZS5zdGF0ZS5zZWxlY3RlZCA9IGlkXG5cbiAgJHNjb3BlLmRlbGV0ZUphciA9IChldmVudCwgaWQpIC0+XG4gICAgaWYgJHNjb3BlLnN0YXRlLnNlbGVjdGVkID09IGlkXG4gICAgICAkc2NvcGUuZGVmYXVsdFN0YXRlKClcbiAgICBhbmd1bGFyLmVsZW1lbnQoZXZlbnQuY3VycmVudFRhcmdldCkucmVtb3ZlQ2xhc3MoXCJmYS1yZW1vdmVcIikuYWRkQ2xhc3MoXCJmYS1zcGluIGZhLXNwaW5uZXJcIilcbiAgICBKb2JTdWJtaXRTZXJ2aWNlLmRlbGV0ZUphcihpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgIGFuZ3VsYXIuZWxlbWVudChldmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImZhLXNwaW4gZmEtc3Bpbm5lclwiKS5hZGRDbGFzcyhcImZhLXJlbW92ZVwiKVxuICAgICAgaWYgZGF0YS5lcnJvcj9cbiAgICAgICAgYWxlcnQoZGF0YS5lcnJvcilcblxuICAkc2NvcGUubG9hZEVudHJ5Q2xhc3MgPSAobmFtZSkgLT5cbiAgICAkc2NvcGUuc3RhdGVbJ2VudHJ5LWNsYXNzJ10gPSBuYW1lXG5cbiAgJHNjb3BlLmdldFBsYW4gPSAoKSAtPlxuICAgIGlmICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9PSBcIlNob3cgUGxhblwiXG4gICAgICBhY3Rpb24gPSBuZXcgRGF0ZSgpLmdldFRpbWUoKVxuICAgICAgJHNjb3BlLnN0YXRlWydhY3Rpb24tdGltZSddID0gYWN0aW9uXG4gICAgICAkc2NvcGUuc3RhdGVbJ3N1Ym1pdC1idXR0b24nXSA9IFwiU3VibWl0XCJcbiAgICAgICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9IFwiR2V0dGluZyBQbGFuXCJcbiAgICAgICRzY29wZS5lcnJvciA9IG51bGxcbiAgICAgICRzY29wZS5wbGFuID0gbnVsbFxuICAgICAgSm9iU3VibWl0U2VydmljZS5nZXRQbGFuKFxuICAgICAgICAkc2NvcGUuc3RhdGUuc2VsZWN0ZWQsIHtcbiAgICAgICAgICAnZW50cnktY2xhc3MnOiAkc2NvcGUuc3RhdGVbJ2VudHJ5LWNsYXNzJ10sXG4gICAgICAgICAgcGFyYWxsZWxpc206ICRzY29wZS5zdGF0ZS5wYXJhbGxlbGlzbSxcbiAgICAgICAgICAncHJvZ3JhbS1hcmdzJzogJHNjb3BlLnN0YXRlWydwcm9ncmFtLWFyZ3MnXVxuICAgICAgICB9XG4gICAgICApLnRoZW4gKGRhdGEpIC0+XG4gICAgICAgIGlmIGFjdGlvbiA9PSAkc2NvcGUuc3RhdGVbJ2FjdGlvbi10aW1lJ11cbiAgICAgICAgICAkc2NvcGUuc3RhdGVbJ3BsYW4tYnV0dG9uJ10gPSBcIlNob3cgUGxhblwiXG4gICAgICAgICAgJHNjb3BlLmVycm9yID0gZGF0YS5lcnJvclxuICAgICAgICAgICRzY29wZS5wbGFuID0gZGF0YS5wbGFuXG5cbiAgJHNjb3BlLnJ1bkpvYiA9ICgpIC0+XG4gICAgaWYgJHNjb3BlLnN0YXRlWydzdWJtaXQtYnV0dG9uJ10gPT0gXCJTdWJtaXRcIlxuICAgICAgYWN0aW9uID0gbmV3IERhdGUoKS5nZXRUaW1lKClcbiAgICAgICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSA9IGFjdGlvblxuICAgICAgJHNjb3BlLnN0YXRlWydzdWJtaXQtYnV0dG9uJ10gPSBcIlN1Ym1pdHRpbmdcIlxuICAgICAgJHNjb3BlLnN0YXRlWydwbGFuLWJ1dHRvbiddID0gXCJTaG93IFBsYW5cIlxuICAgICAgJHNjb3BlLmVycm9yID0gbnVsbFxuICAgICAgSm9iU3VibWl0U2VydmljZS5ydW5Kb2IoXG4gICAgICAgICRzY29wZS5zdGF0ZS5zZWxlY3RlZCwge1xuICAgICAgICAgICdlbnRyeS1jbGFzcyc6ICRzY29wZS5zdGF0ZVsnZW50cnktY2xhc3MnXSxcbiAgICAgICAgICBwYXJhbGxlbGlzbTogJHNjb3BlLnN0YXRlLnBhcmFsbGVsaXNtLFxuICAgICAgICAgICdwcm9ncmFtLWFyZ3MnOiAkc2NvcGUuc3RhdGVbJ3Byb2dyYW0tYXJncyddXG4gICAgICAgIH1cbiAgICAgICkudGhlbiAoZGF0YSkgLT5cbiAgICAgICAgaWYgYWN0aW9uID09ICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXVxuICAgICAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXRcIlxuICAgICAgICAgICRzY29wZS5lcnJvciA9IGRhdGEuZXJyb3JcbiAgICAgICAgICBpZiBkYXRhLmpvYmlkP1xuICAgICAgICAgICAgJHN0YXRlLmdvKFwic2luZ2xlLWpvYi5wbGFuLnN1YnRhc2tzXCIsIHtqb2JpZDogZGF0YS5qb2JpZH0pXG5cbiAgIyBqb2IgcGxhbiBkaXNwbGF5IHJlbGF0ZWQgc3R1ZmZcbiAgJHNjb3BlLm5vZGVpZCA9IG51bGxcbiAgJHNjb3BlLmNoYW5nZU5vZGUgPSAobm9kZWlkKSAtPlxuICAgIGlmIG5vZGVpZCAhPSAkc2NvcGUubm9kZWlkXG4gICAgICAkc2NvcGUubm9kZWlkID0gbm9kZWlkXG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbFxuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbFxuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGxcblxuICAgICAgJHNjb3BlLiRicm9hZGNhc3QgJ3JlbG9hZCdcblxuICAgIGVsc2VcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsXG4gICAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2VcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBudWxsXG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsXG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbFxuXG4gICRzY29wZS5jbGVhckZpbGVzID0gKCkgLT5cbiAgICAkc2NvcGUudXBsb2FkZXIgPSB7fVxuXG4gICRzY29wZS51cGxvYWRGaWxlcyA9IChmaWxlcykgLT5cbiAgICAjIG1ha2Ugc3VyZSBldmVyeXRoaW5nIGlzIGNsZWFyIGFnYWluLlxuICAgICRzY29wZS51cGxvYWRlciA9IHt9XG4gICAgaWYgZmlsZXMubGVuZ3RoID09IDFcbiAgICAgICRzY29wZS51cGxvYWRlclsnZmlsZSddID0gZmlsZXNbMF1cbiAgICAgICRzY29wZS51cGxvYWRlclsndXBsb2FkJ10gPSB0cnVlXG4gICAgZWxzZVxuICAgICAgJHNjb3BlLnVwbG9hZGVyWydlcnJvciddID0gXCJEaWQgeWEgZm9yZ2V0IHRvIHNlbGVjdCBhIGZpbGU/XCJcblxuICAkc2NvcGUuc3RhcnRVcGxvYWQgPSAoKSAtPlxuICAgIGlmICRzY29wZS51cGxvYWRlclsnZmlsZSddP1xuICAgICAgZm9ybWRhdGEgPSBuZXcgRm9ybURhdGEoKVxuICAgICAgZm9ybWRhdGEuYXBwZW5kKFwiamFyZmlsZVwiLCAkc2NvcGUudXBsb2FkZXJbJ2ZpbGUnXSlcbiAgICAgICRzY29wZS51cGxvYWRlclsndXBsb2FkJ10gPSBmYWxzZVxuICAgICAgJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBcIkluaXRpYWxpemluZyB1cGxvYWQuLi5cIlxuICAgICAgeGhyID0gbmV3IFhNTEh0dHBSZXF1ZXN0KClcbiAgICAgIHhoci51cGxvYWQub25wcm9ncmVzcyA9IChldmVudCkgLT5cbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBudWxsXG4gICAgICAgICRzY29wZS51cGxvYWRlclsncHJvZ3Jlc3MnXSA9IHBhcnNlSW50KDEwMCAqIGV2ZW50LmxvYWRlZCAvIGV2ZW50LnRvdGFsKVxuICAgICAgeGhyLnVwbG9hZC5vbmVycm9yID0gKGV2ZW50KSAtPlxuICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ3Byb2dyZXNzJ10gPSBudWxsXG4gICAgICAgICRzY29wZS51cGxvYWRlclsnZXJyb3InXSA9IFwiQW4gZXJyb3Igb2NjdXJyZWQgd2hpbGUgdXBsb2FkaW5nIHlvdXIgZmlsZVwiXG4gICAgICB4aHIudXBsb2FkLm9ubG9hZCA9IChldmVudCkgLT5cbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydwcm9ncmVzcyddID0gbnVsbFxuICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IFwiU2F2aW5nLi4uXCJcbiAgICAgIHhoci5vbnJlYWR5c3RhdGVjaGFuZ2UgPSAoKSAtPlxuICAgICAgICBpZiB4aHIucmVhZHlTdGF0ZSA9PSA0XG4gICAgICAgICAgcmVzcG9uc2UgPSBKU09OLnBhcnNlKHhoci5yZXNwb25zZVRleHQpXG4gICAgICAgICAgaWYgcmVzcG9uc2UuZXJyb3I/XG4gICAgICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ2Vycm9yJ10gPSByZXNwb25zZS5lcnJvclxuICAgICAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBudWxsXG4gICAgICAgICAgZWxzZVxuICAgICAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBcIlVwbG9hZGVkIVwiXG4gICAgICB4aHIub3BlbihcIlBPU1RcIiwgXCIvamFycy91cGxvYWRcIilcbiAgICAgIHhoci5zZW5kKGZvcm1kYXRhKVxuICAgIGVsc2VcbiAgICAgIGNvbnNvbGUubG9nKFwiVW5leHBlY3RlZCBFcnJvci4gVGhpcyBzaG91bGQgbm90IGhhcHBlblwiKVxuXG4uZmlsdGVyICdnZXRKYXJTZWxlY3RDbGFzcycsIC0+XG4gIChzZWxlY3RlZCwgYWN0dWFsKSAtPlxuICAgIGlmIHNlbGVjdGVkID09IGFjdHVhbFxuICAgICAgXCJmYS1jaGVjay1zcXVhcmVcIlxuICAgIGVsc2VcbiAgICAgIFwiZmEtc3F1YXJlLW9cIlxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignSm9iU3VibWl0Q29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9iU3VibWl0U2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZywgJHN0YXRlLCAkbG9jYXRpb24pIHtcbiAgdmFyIHJlZnJlc2g7XG4gICRzY29wZS55YXJuID0gJGxvY2F0aW9uLmFic1VybCgpLmluZGV4T2YoXCIvcHJveHkvYXBwbGljYXRpb25fXCIpICE9PSAtMTtcbiAgJHNjb3BlLmxvYWRMaXN0ID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYlN1Ym1pdFNlcnZpY2UubG9hZEphckxpc3QoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICRzY29wZS5hZGRyZXNzID0gZGF0YS5hZGRyZXNzO1xuICAgICAgJHNjb3BlLm5vYWNjZXNzID0gZGF0YS5lcnJvcjtcbiAgICAgIHJldHVybiAkc2NvcGUuamFycyA9IGRhdGEuZmlsZXM7XG4gICAgfSk7XG4gIH07XG4gICRzY29wZS5kZWZhdWx0U3RhdGUgPSBmdW5jdGlvbigpIHtcbiAgICAkc2NvcGUucGxhbiA9IG51bGw7XG4gICAgJHNjb3BlLmVycm9yID0gbnVsbDtcbiAgICByZXR1cm4gJHNjb3BlLnN0YXRlID0ge1xuICAgICAgc2VsZWN0ZWQ6IG51bGwsXG4gICAgICBwYXJhbGxlbGlzbTogXCJcIixcbiAgICAgICdlbnRyeS1jbGFzcyc6IFwiXCIsXG4gICAgICAncHJvZ3JhbS1hcmdzJzogXCJcIixcbiAgICAgICdwbGFuLWJ1dHRvbic6IFwiU2hvdyBQbGFuXCIsXG4gICAgICAnc3VibWl0LWJ1dHRvbic6IFwiU3VibWl0XCIsXG4gICAgICAnYWN0aW9uLXRpbWUnOiAwXG4gICAgfTtcbiAgfTtcbiAgJHNjb3BlLmRlZmF1bHRTdGF0ZSgpO1xuICAkc2NvcGUudXBsb2FkZXIgPSB7fTtcbiAgJHNjb3BlLmxvYWRMaXN0KCk7XG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5sb2FkTGlzdCgpO1xuICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpO1xuICB9KTtcbiAgJHNjb3BlLnNlbGVjdEphciA9IGZ1bmN0aW9uKGlkKSB7XG4gICAgaWYgKCRzY29wZS5zdGF0ZS5zZWxlY3RlZCA9PT0gaWQpIHtcbiAgICAgIHJldHVybiAkc2NvcGUuZGVmYXVsdFN0YXRlKCk7XG4gICAgfSBlbHNlIHtcbiAgICAgICRzY29wZS5kZWZhdWx0U3RhdGUoKTtcbiAgICAgIHJldHVybiAkc2NvcGUuc3RhdGUuc2VsZWN0ZWQgPSBpZDtcbiAgICB9XG4gIH07XG4gICRzY29wZS5kZWxldGVKYXIgPSBmdW5jdGlvbihldmVudCwgaWQpIHtcbiAgICBpZiAoJHNjb3BlLnN0YXRlLnNlbGVjdGVkID09PSBpZCkge1xuICAgICAgJHNjb3BlLmRlZmF1bHRTdGF0ZSgpO1xuICAgIH1cbiAgICBhbmd1bGFyLmVsZW1lbnQoZXZlbnQuY3VycmVudFRhcmdldCkucmVtb3ZlQ2xhc3MoXCJmYS1yZW1vdmVcIikuYWRkQ2xhc3MoXCJmYS1zcGluIGZhLXNwaW5uZXJcIik7XG4gICAgcmV0dXJuIEpvYlN1Ym1pdFNlcnZpY2UuZGVsZXRlSmFyKGlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChldmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImZhLXNwaW4gZmEtc3Bpbm5lclwiKS5hZGRDbGFzcyhcImZhLXJlbW92ZVwiKTtcbiAgICAgIGlmIChkYXRhLmVycm9yICE9IG51bGwpIHtcbiAgICAgICAgcmV0dXJuIGFsZXJ0KGRhdGEuZXJyb3IpO1xuICAgICAgfVxuICAgIH0pO1xuICB9O1xuICAkc2NvcGUubG9hZEVudHJ5Q2xhc3MgPSBmdW5jdGlvbihuYW1lKSB7XG4gICAgcmV0dXJuICRzY29wZS5zdGF0ZVsnZW50cnktY2xhc3MnXSA9IG5hbWU7XG4gIH07XG4gICRzY29wZS5nZXRQbGFuID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGFjdGlvbjtcbiAgICBpZiAoJHNjb3BlLnN0YXRlWydwbGFuLWJ1dHRvbiddID09PSBcIlNob3cgUGxhblwiKSB7XG4gICAgICBhY3Rpb24gPSBuZXcgRGF0ZSgpLmdldFRpbWUoKTtcbiAgICAgICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSA9IGFjdGlvbjtcbiAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXRcIjtcbiAgICAgICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9IFwiR2V0dGluZyBQbGFuXCI7XG4gICAgICAkc2NvcGUuZXJyb3IgPSBudWxsO1xuICAgICAgJHNjb3BlLnBsYW4gPSBudWxsO1xuICAgICAgcmV0dXJuIEpvYlN1Ym1pdFNlcnZpY2UuZ2V0UGxhbigkc2NvcGUuc3RhdGUuc2VsZWN0ZWQsIHtcbiAgICAgICAgJ2VudHJ5LWNsYXNzJzogJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddLFxuICAgICAgICBwYXJhbGxlbGlzbTogJHNjb3BlLnN0YXRlLnBhcmFsbGVsaXNtLFxuICAgICAgICAncHJvZ3JhbS1hcmdzJzogJHNjb3BlLnN0YXRlWydwcm9ncmFtLWFyZ3MnXVxuICAgICAgfSkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIGlmIChhY3Rpb24gPT09ICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSkge1xuICAgICAgICAgICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9IFwiU2hvdyBQbGFuXCI7XG4gICAgICAgICAgJHNjb3BlLmVycm9yID0gZGF0YS5lcnJvcjtcbiAgICAgICAgICByZXR1cm4gJHNjb3BlLnBsYW4gPSBkYXRhLnBsYW47XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH1cbiAgfTtcbiAgJHNjb3BlLnJ1bkpvYiA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBhY3Rpb247XG4gICAgaWYgKCRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID09PSBcIlN1Ym1pdFwiKSB7XG4gICAgICBhY3Rpb24gPSBuZXcgRGF0ZSgpLmdldFRpbWUoKTtcbiAgICAgICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSA9IGFjdGlvbjtcbiAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXR0aW5nXCI7XG4gICAgICAkc2NvcGUuc3RhdGVbJ3BsYW4tYnV0dG9uJ10gPSBcIlNob3cgUGxhblwiO1xuICAgICAgJHNjb3BlLmVycm9yID0gbnVsbDtcbiAgICAgIHJldHVybiBKb2JTdWJtaXRTZXJ2aWNlLnJ1bkpvYigkc2NvcGUuc3RhdGUuc2VsZWN0ZWQsIHtcbiAgICAgICAgJ2VudHJ5LWNsYXNzJzogJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddLFxuICAgICAgICBwYXJhbGxlbGlzbTogJHNjb3BlLnN0YXRlLnBhcmFsbGVsaXNtLFxuICAgICAgICAncHJvZ3JhbS1hcmdzJzogJHNjb3BlLnN0YXRlWydwcm9ncmFtLWFyZ3MnXVxuICAgICAgfSkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIGlmIChhY3Rpb24gPT09ICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSkge1xuICAgICAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXRcIjtcbiAgICAgICAgICAkc2NvcGUuZXJyb3IgPSBkYXRhLmVycm9yO1xuICAgICAgICAgIGlmIChkYXRhLmpvYmlkICE9IG51bGwpIHtcbiAgICAgICAgICAgIHJldHVybiAkc3RhdGUuZ28oXCJzaW5nbGUtam9iLnBsYW4uc3VidGFza3NcIiwge1xuICAgICAgICAgICAgICBqb2JpZDogZGF0YS5qb2JpZFxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG4gICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAkc2NvcGUuY2hhbmdlTm9kZSA9IGZ1bmN0aW9uKG5vZGVpZCkge1xuICAgIGlmIChub2RlaWQgIT09ICRzY29wZS5ub2RlaWQpIHtcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWQ7XG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbDtcbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGw7XG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbDtcbiAgICAgIHJldHVybiAkc2NvcGUuJGJyb2FkY2FzdCgncmVsb2FkJyk7XG4gICAgfSBlbHNlIHtcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAgICAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlO1xuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGw7XG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsO1xuICAgICAgcmV0dXJuICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsO1xuICAgIH1cbiAgfTtcbiAgJHNjb3BlLmNsZWFyRmlsZXMgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLnVwbG9hZGVyID0ge307XG4gIH07XG4gICRzY29wZS51cGxvYWRGaWxlcyA9IGZ1bmN0aW9uKGZpbGVzKSB7XG4gICAgJHNjb3BlLnVwbG9hZGVyID0ge307XG4gICAgaWYgKGZpbGVzLmxlbmd0aCA9PT0gMSkge1xuICAgICAgJHNjb3BlLnVwbG9hZGVyWydmaWxlJ10gPSBmaWxlc1swXTtcbiAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ3VwbG9hZCddID0gdHJ1ZTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuICRzY29wZS51cGxvYWRlclsnZXJyb3InXSA9IFwiRGlkIHlhIGZvcmdldCB0byBzZWxlY3QgYSBmaWxlP1wiO1xuICAgIH1cbiAgfTtcbiAgcmV0dXJuICRzY29wZS5zdGFydFVwbG9hZCA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBmb3JtZGF0YSwgeGhyO1xuICAgIGlmICgkc2NvcGUudXBsb2FkZXJbJ2ZpbGUnXSAhPSBudWxsKSB7XG4gICAgICBmb3JtZGF0YSA9IG5ldyBGb3JtRGF0YSgpO1xuICAgICAgZm9ybWRhdGEuYXBwZW5kKFwiamFyZmlsZVwiLCAkc2NvcGUudXBsb2FkZXJbJ2ZpbGUnXSk7XG4gICAgICAkc2NvcGUudXBsb2FkZXJbJ3VwbG9hZCddID0gZmFsc2U7XG4gICAgICAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IFwiSW5pdGlhbGl6aW5nIHVwbG9hZC4uLlwiO1xuICAgICAgeGhyID0gbmV3IFhNTEh0dHBSZXF1ZXN0KCk7XG4gICAgICB4aHIudXBsb2FkLm9ucHJvZ3Jlc3MgPSBmdW5jdGlvbihldmVudCkge1xuICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IG51bGw7XG4gICAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ3Byb2dyZXNzJ10gPSBwYXJzZUludCgxMDAgKiBldmVudC5sb2FkZWQgLyBldmVudC50b3RhbCk7XG4gICAgICB9O1xuICAgICAgeGhyLnVwbG9hZC5vbmVycm9yID0gZnVuY3Rpb24oZXZlbnQpIHtcbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydwcm9ncmVzcyddID0gbnVsbDtcbiAgICAgICAgcmV0dXJuICRzY29wZS51cGxvYWRlclsnZXJyb3InXSA9IFwiQW4gZXJyb3Igb2NjdXJyZWQgd2hpbGUgdXBsb2FkaW5nIHlvdXIgZmlsZVwiO1xuICAgICAgfTtcbiAgICAgIHhoci51cGxvYWQub25sb2FkID0gZnVuY3Rpb24oZXZlbnQpIHtcbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydwcm9ncmVzcyddID0gbnVsbDtcbiAgICAgICAgcmV0dXJuICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gXCJTYXZpbmcuLi5cIjtcbiAgICAgIH07XG4gICAgICB4aHIub25yZWFkeXN0YXRlY2hhbmdlID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHZhciByZXNwb25zZTtcbiAgICAgICAgaWYgKHhoci5yZWFkeVN0YXRlID09PSA0KSB7XG4gICAgICAgICAgcmVzcG9uc2UgPSBKU09OLnBhcnNlKHhoci5yZXNwb25zZVRleHQpO1xuICAgICAgICAgIGlmIChyZXNwb25zZS5lcnJvciAhPSBudWxsKSB7XG4gICAgICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ2Vycm9yJ10gPSByZXNwb25zZS5lcnJvcjtcbiAgICAgICAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IG51bGw7XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IFwiVXBsb2FkZWQhXCI7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgeGhyLm9wZW4oXCJQT1NUXCIsIFwiL2phcnMvdXBsb2FkXCIpO1xuICAgICAgcmV0dXJuIHhoci5zZW5kKGZvcm1kYXRhKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIGNvbnNvbGUubG9nKFwiVW5leHBlY3RlZCBFcnJvci4gVGhpcyBzaG91bGQgbm90IGhhcHBlblwiKTtcbiAgICB9XG4gIH07XG59KS5maWx0ZXIoJ2dldEphclNlbGVjdENsYXNzJywgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbihzZWxlY3RlZCwgYWN0dWFsKSB7XG4gICAgaWYgKHNlbGVjdGVkID09PSBhY3R1YWwpIHtcbiAgICAgIHJldHVybiBcImZhLWNoZWNrLXNxdWFyZVwiO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gXCJmYS1zcXVhcmUtb1wiO1xuICAgIH1cbiAgfTtcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5zZXJ2aWNlICdKb2JTdWJtaXRTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG5cbiAgQGxvYWRKYXJMaXN0ID0gKCkgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChcImphcnMvXCIpXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBkZWxldGVKYXIgPSAoaWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5kZWxldGUoXCJqYXJzL1wiICsgaWQpXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAZ2V0UGxhbiA9IChpZCwgYXJncykgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChcImphcnMvXCIgKyBpZCArIFwiL3BsYW5cIiwge3BhcmFtczogYXJnc30pXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBydW5Kb2IgPSAoaWQsIGFyZ3MpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5wb3N0KFwiamFycy9cIiArIGlkICsgXCIvcnVuXCIsIHt9LCB7cGFyYW1zOiBhcmdzfSlcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQFxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnSm9iU3VibWl0U2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdGhpcy5sb2FkSmFyTGlzdCA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KFwiamFycy9cIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZGVsZXRlSmFyID0gZnVuY3Rpb24oaWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwW1wiZGVsZXRlXCJdKFwiamFycy9cIiArIGlkKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRQbGFuID0gZnVuY3Rpb24oaWQsIGFyZ3MpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChcImphcnMvXCIgKyBpZCArIFwiL3BsYW5cIiwge1xuICAgICAgcGFyYW1zOiBhcmdzXG4gICAgfSkuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMucnVuSm9iID0gZnVuY3Rpb24oaWQsIGFyZ3MpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLnBvc3QoXCJqYXJzL1wiICsgaWQgKyBcIi9ydW5cIiwge30sIHtcbiAgICAgIHBhcmFtczogYXJnc1xuICAgIH0pLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIl0sInNvdXJjZVJvb3QiOiIvc291cmNlLyJ9
