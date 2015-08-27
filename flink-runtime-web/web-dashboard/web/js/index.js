angular.module('flinkApp', ['ui.router', 'angularMoment']).run(["$rootScope", function($rootScope) {
  $rootScope.sidebarVisible = false;
  return $rootScope.showSidebar = function() {
    $rootScope.sidebarVisible = !$rootScope.sidebarVisible;
    return $rootScope.sidebarClass = 'force-show';
  };
}]).value('flinkConfig', {
  jobServer: 'http://localhost:8081',
  "refresh-interval": 10000
}).run(["JobsService", "MainService", "flinkConfig", "$interval", function(JobsService, MainService, flinkConfig, $interval) {
  return MainService.loadConfig().then(function(config) {
    angular.extend(flinkConfig, config);
    JobsService.listJobs();
    return $interval(function() {
      return JobsService.listJobs();
    }, flinkConfig["refresh-interval"]);
  });
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
    views: {
      details: {
        templateUrl: "partials/jobs/job.plan.html",
        controller: 'JobPlanController'
      }
    }
  }).state("single-job.plan.node", {
    url: "/vertex/{nodeid}",
    abstract: true,
    views: {
      node: {
        templateUrl: "partials/jobs/job.plan.node.html",
        controller: 'JobPlanNodeController'
      }
    }
  }).state("single-job.plan.node.subtasks", {
    url: "",
    views: {
      nodeTab: {
        templateUrl: "partials/jobs/job.plan.node.subtasks.html"
      }
    }
  }).state("single-job.plan.node.properties", {
    url: "/properties",
    views: {
      nodeTab: {
        templateUrl: "partials/jobs/job.plan.node.properties.html"
      }
    }
  }).state("single-job.plan.node.accumulators", {
    url: "/properties",
    views: {
      nodeTab: {
        templateUrl: "partials/jobs/job.plan.node.accumulators.html"
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
  }).state("single-job.statistics", {
    url: "/statistics",
    views: {
      details: {
        templateUrl: "partials/jobs/job.statistics.html"
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
  }).state("single-job.config", {
    url: "/config",
    views: {
      details: {
        templateUrl: "partials/jobs/job.config.html"
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
}]).filter("humanizeTaskName", function() {
  return function(text) {
    if (text) {
      return text.replace("&gt;", ">").replace("<br/>", "");
    } else {
      return '';
    }
  };
});

angular.module('flinkApp').service('MainService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var jobObservers;
  jobObservers = [];
  this.loadConfig = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "/config").success(function(data, status, headers, config) {
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
}]).controller('SingleJobController', ["$scope", "$state", "$stateParams", "JobsService", "$rootScope", function($scope, $state, $stateParams, JobsService, $rootScope) {
  $scope.jobid = $stateParams.jobid;
  $rootScope.job = null;
  JobsService.loadJob($stateParams.jobid).then(function(data) {
    return $rootScope.job = data;
  });
  return $scope.$on('$destroy', function() {
    return $rootScope.job = null;
  });
}]).controller('JobPlanController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  return JobsService.loadPlan($stateParams.jobid).then(function(data) {
    return $scope.plan = data;
  });
}]).controller('JobPlanNodeController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  $scope.nodeid = $stateParams.nodeid;
  $scope.stateList = JobsService.stateList();
  JobsService.getNode($scope.nodeid).then(function(data) {
    return $scope.node = data;
  });
  return JobsService.getVertex($scope.nodeid).then(function(data) {
    return $scope.vertex = data;
  });
}]).controller('JobTimelineVertexController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  return JobsService.getVertex($stateParams.vertexId).then(function(data) {
    return $scope.vertex = data;
  });
}]).controller('JobExceptionsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  return JobsService.loadExceptions().then(function(data) {
    return $scope.exceptions = data;
  });
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
        testData = [];
        angular.forEach(data.subtasks, function(subtask, i) {
          return testData.push({
            label: subtask.host + " (" + subtask.subtask + ")",
            times: [
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
              }, {
                label: "Running",
                color: "#ddd",
                borderColor: "#555",
                starting_time: subtask.timestamps["RUNNING"],
                ending_time: subtask.timestamps["FINISHED"],
                type: 'regular'
              }
            ]
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
      job: "="
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
        testData = [];
        testData.push({
          times: [
            {
              label: "Scheduled",
              color: "#cccccc",
              borderColor: "#555",
              starting_time: data.timestamps["CREATED"],
              ending_time: data.timestamps["CREATED"] + 1,
              type: 'scheduled'
            }
          ]
        });
        angular.forEach(data.vertices, function(vertex) {
          if (vertex['start-time'] > -1) {
            return testData.push({
              times: [
                {
                  label: translateLabel(vertex.name),
                  color: "#d9f1f7",
                  borderColor: "#62cdea",
                  starting_time: vertex['start-time'],
                  ending_time: vertex['end-time'],
                  link: vertex.id,
                  type: 'regular'
                }
              ]
            });
          }
        });
        chart = d3.timeline().stack().click(function(d, i, datum) {
          if (d.link) {
            return $state.go("single-job.timeline.vertex", {
              jobid: data.jid,
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
      scope.$watch(attrs.job, function(data) {
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
      plan: '='
    },
    link: function(scope, elem, attrs) {
      var containerW, createEdge, createLabelEdge, createLabelNode, createNode, d3mainSvg, d3mainSvgG, d3tmpSvg, drawGraph, extendLabelNodeForIteration, getNodeType, isSpecialIterationNode, jobid, loadJsonToDagre, mainG, mainSvgElement, mainTmpElement, mainZoom, searchForNode, shortenString, subgraphs;
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
        labelValue = "<a href='#/jobs/" + jobid + "/vertex/" + el.id + "' class='node-label " + getNodeType(el, info) + "'>";
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
        labelValue += "</a>";
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
          if (!(!missingNode || missingNode.alreadyAdded === true)) {
            missingNode.alreadyAdded = true;
            g.setNode(missingNode.id, {
              label: createLabelNode(missingNode, "mirror"),
              labelType: 'html',
              "class": getNodeType(missingNode, 'mirror')
            });
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
        var g, i, newScale, renderer, sg, xCenterOffset, yCenterOffset;
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
        return mainZoom(d3mainSvg);
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
    return angular.forEach(list, function(job, jobKey) {
      if (!(job['end-time'] > -1)) {
        return job['end-time'] = job['start-time'] + job['duration'];
      }
    });
  };
  this.listJobs = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "/joboverview").success((function(_this) {
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
    $http.get(flinkConfig.jobServer + "/jobs/" + jobid).success((function(_this) {
      return function(data, status, headers, config) {
        _this.setEndTimes(data.vertices);
        return $http.get(flinkConfig.jobServer + "/jobs/" + jobid + "/config").success(function(jobConfig) {
          data = angular.extend(data, jobConfig);
          currentJob = data;
          return deferreds.job.resolve(data);
        });
      };
    })(this));
    return deferreds.job.promise;
  };
  this.loadPlan = function(jobid) {
    currentPlan = null;
    deferreds.plan = $q.defer();
    $http.get(flinkConfig.jobServer + "/jobs/" + jobid + "/plan").success(function(data) {
      currentPlan = data;
      return deferreds.plan.resolve(data);
    });
    return deferreds.plan.promise;
  };
  this.getNode = function(nodeid) {
    var deferred, seekNode;
    seekNode = function(nodeid, data) {
      var i, len, node, sub;
      for (i = 0, len = data.length; i < len; i++) {
        node = data[i];
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
    $q.all([deferreds.plan.promise, deferreds.job.promise]).then((function(_this) {
      return function(data) {
        var foundNode;
        foundNode = seekNode(nodeid, currentPlan.nodes);
        foundNode.vertex = _this.seekVertex(nodeid);
        return deferred.resolve(foundNode);
      };
    })(this));
    return deferred.promise;
  };
  this.seekVertex = function(nodeid) {
    var i, len, ref, vertex;
    ref = currentJob.vertices;
    for (i = 0, len = ref.length; i < len; i++) {
      vertex = ref[i];
      if (vertex.id === nodeid) {
        return vertex;
      }
    }
    return null;
  };
  this.getVertex = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    $q.all([deferreds.job.promise]).then((function(_this) {
      return function(data) {
        var vertex;
        vertex = _this.seekVertex(vertexid);
        return $http.get(flinkConfig.jobServer + "/jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasktimes").success(function(data) {
          vertex.subtasks = data.subtasks;
          return deferred.resolve(vertex);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.loadExceptions = function() {
    var deferred;
    deferred = $q.defer();
    $q.all([deferreds.job.promise]).then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "/jobs/" + currentJob.jid + "/exceptions").success(function(exceptions) {
          currentJob.exceptions = exceptions;
          return deferred.resolve(exceptions);
        });
      };
    })(this));
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
    $http.get(flinkConfig.jobServer + "/overview").success(function(data, status, headers, config) {
      overview = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbImluZGV4LmNvZmZlZSIsImluZGV4LmpzIiwiY29tbW9uL2RpcmVjdGl2ZXMuY29mZmVlIiwiY29tbW9uL2RpcmVjdGl2ZXMuanMiLCJjb21tb24vZmlsdGVycy5jb2ZmZWUiLCJjb21tb24vZmlsdGVycy5qcyIsImNvbW1vbi9zZXJ2aWNlcy5jb2ZmZWUiLCJjb21tb24vc2VydmljZXMuanMiLCJtb2R1bGVzL2pvYnMvam9icy5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvam9icy9qb2JzLmN0cmwuanMiLCJtb2R1bGVzL2pvYnMvam9icy5kaXIuY29mZmVlIiwibW9kdWxlcy9qb2JzL2pvYnMuZGlyLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuc3ZjLmNvZmZlZSIsIm1vZHVsZXMvam9icy9qb2JzLnN2Yy5qcyIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LmN0cmwuanMiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LnN2Yy5qcyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFrQkEsUUFBUSxPQUFPLFlBQVksQ0FBQyxhQUFhLGtCQUl4QyxtQkFBSSxTQUFDLFlBQUQ7RUFDSCxXQUFXLGlCQUFpQjtFQ3JCNUIsT0RzQkEsV0FBVyxjQUFjLFdBQUE7SUFDdkIsV0FBVyxpQkFBaUIsQ0FBQyxXQUFXO0lDckJ4QyxPRHNCQSxXQUFXLGVBQWU7O0lBSTdCLE1BQU0sZUFBZTtFQUNwQixXQUFXO0VBRVgsb0JBQW9CO0dBS3JCLCtEQUFJLFNBQUMsYUFBYSxhQUFhLGFBQWEsV0FBeEM7RUM1QkgsT0Q2QkEsWUFBWSxhQUFhLEtBQUssU0FBQyxRQUFEO0lBQzVCLFFBQVEsT0FBTyxhQUFhO0lBRTVCLFlBQVk7SUM3QlosT0QrQkEsVUFBVSxXQUFBO01DOUJSLE9EK0JBLFlBQVk7T0FDWixZQUFZOztJQUtqQixnREFBTyxT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QUFBLFdBQVc7RUFFWCxLQUFDLGVBQWUsV0FBQTtJQUNkLElBQUE7SUFBQSxXQUFXLEdBQUc7SUFFZCxNQUFNLElBQUksWUFBWSxZQUFZLGFBQ2pDLFFBQVEsU0FBQyxNQUFNLFFBQVEsU0FBUyxRQUF4QjtNQUNQLFdBQVc7TUNwQlgsT0RxQkEsU0FBUyxRQUFROztJQ25CbkIsT0RxQkEsU0FBUzs7RUNuQlgsT0RxQkE7O0FDbkJGIiwiZmlsZSI6ImluZGV4LmpzIiwic291cmNlc0NvbnRlbnQiOlsiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcsIFsndWkucm91dGVyJywgJ2FuZ3VsYXJNb21lbnQnXSlcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4ucnVuICgkcm9vdFNjb3BlKSAtPlxuICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gZmFsc2VcbiAgJHJvb3RTY29wZS5zaG93U2lkZWJhciA9IC0+XG4gICAgJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZSA9ICEkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlXG4gICAgJHJvb3RTY29wZS5zaWRlYmFyQ2xhc3MgPSAnZm9yY2Utc2hvdydcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4udmFsdWUgJ2ZsaW5rQ29uZmlnJywge1xuICBqb2JTZXJ2ZXI6ICdodHRwOi8vbG9jYWxob3N0OjgwODEnXG4jICBqb2JTZXJ2ZXI6ICdodHRwOi8vbG9jYWxob3N0OjMwMDAvbmV3LXNlcnZlcidcbiAgXCJyZWZyZXNoLWludGVydmFsXCI6IDEwMDAwXG59XG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLnJ1biAoSm9ic1NlcnZpY2UsIE1haW5TZXJ2aWNlLCBmbGlua0NvbmZpZywgJGludGVydmFsKSAtPlxuICBNYWluU2VydmljZS5sb2FkQ29uZmlnKCkudGhlbiAoY29uZmlnKSAtPlxuICAgIGFuZ3VsYXIuZXh0ZW5kIGZsaW5rQ29uZmlnLCBjb25maWdcblxuICAgIEpvYnNTZXJ2aWNlLmxpc3RKb2JzKClcblxuICAgICRpbnRlcnZhbCAtPlxuICAgICAgSm9ic1NlcnZpY2UubGlzdEpvYnMoKVxuICAgICwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdXG5cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29uZmlnICgkc3RhdGVQcm92aWRlciwgJHVybFJvdXRlclByb3ZpZGVyKSAtPlxuICAkc3RhdGVQcm92aWRlci5zdGF0ZSBcIm92ZXJ2aWV3XCIsXG4gICAgdXJsOiBcIi9vdmVydmlld1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9vdmVydmlldy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ092ZXJ2aWV3Q29udHJvbGxlcidcblxuICAuc3RhdGUgXCJydW5uaW5nLWpvYnNcIixcbiAgICB1cmw6IFwiL3J1bm5pbmctam9ic1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL3J1bm5pbmctam9icy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ1J1bm5pbmdKb2JzQ29udHJvbGxlcidcbiAgXG4gIC5zdGF0ZSBcImNvbXBsZXRlZC1qb2JzXCIsXG4gICAgdXJsOiBcIi9jb21wbGV0ZWQtam9ic1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2NvbXBsZXRlZC1qb2JzLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnQ29tcGxldGVkSm9ic0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYlwiLFxuICAgIHVybDogXCIvam9icy97am9iaWR9XCJcbiAgICBhYnN0cmFjdDogdHJ1ZVxuICAgIHZpZXdzOlxuICAgICAgbWFpbjpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVKb2JDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhblwiLFxuICAgIHVybDogXCJcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5Db250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi5ub2RlXCIsXG4gICAgdXJsOiBcIi92ZXJ0ZXgve25vZGVpZH1cIlxuICAgIGFic3RyYWN0OiB0cnVlXG4gICAgdmlld3M6XG4gICAgICBub2RlOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuTm9kZUNvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLm5vZGUuc3VidGFza3NcIixcbiAgICB1cmw6IFwiXCJcbiAgICB2aWV3czpcbiAgICAgIG5vZGVUYWI6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS5zdWJ0YXNrcy5odG1sXCJcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4ubm9kZS5wcm9wZXJ0aWVzXCIsXG4gICAgdXJsOiBcIi9wcm9wZXJ0aWVzXCJcbiAgICB2aWV3czpcbiAgICAgIG5vZGVUYWI6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS5wcm9wZXJ0aWVzLmh0bWxcIlxuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi5ub2RlLmFjY3VtdWxhdG9yc1wiLFxuICAgIHVybDogXCIvcHJvcGVydGllc1wiXG4gICAgdmlld3M6XG4gICAgICBub2RlVGFiOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUuYWNjdW11bGF0b3JzLmh0bWxcIlxuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IudGltZWxpbmVcIixcbiAgICB1cmw6IFwiL3RpbWVsaW5lXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLmh0bWxcIlxuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsXG4gICAgdXJsOiBcIi97dmVydGV4SWR9XCJcbiAgICB2aWV3czpcbiAgICAgIHZlcnRleDpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUudmVydGV4Lmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2Iuc3RhdGlzdGljc1wiLFxuICAgIHVybDogXCIvc3RhdGlzdGljc1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5zdGF0aXN0aWNzLmh0bWxcIlxuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IuZXhjZXB0aW9uc1wiLFxuICAgIHVybDogXCIvZXhjZXB0aW9uc1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5leGNlcHRpb25zLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5jb25maWdcIixcbiAgICB1cmw6IFwiL2NvbmZpZ1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5jb25maWcuaHRtbFwiXG5cbiAgJHVybFJvdXRlclByb3ZpZGVyLm90aGVyd2lzZSBcIi9vdmVydmlld1wiXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnLCBbJ3VpLnJvdXRlcicsICdhbmd1bGFyTW9tZW50J10pLnJ1bihmdW5jdGlvbigkcm9vdFNjb3BlKSB7XG4gICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSBmYWxzZTtcbiAgcmV0dXJuICRyb290U2NvcGUuc2hvd1NpZGViYXIgPSBmdW5jdGlvbigpIHtcbiAgICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gISRyb290U2NvcGUuc2lkZWJhclZpc2libGU7XG4gICAgcmV0dXJuICRyb290U2NvcGUuc2lkZWJhckNsYXNzID0gJ2ZvcmNlLXNob3cnO1xuICB9O1xufSkudmFsdWUoJ2ZsaW5rQ29uZmlnJywge1xuICBqb2JTZXJ2ZXI6ICdodHRwOi8vbG9jYWxob3N0OjgwODEnLFxuICBcInJlZnJlc2gtaW50ZXJ2YWxcIjogMTAwMDBcbn0pLnJ1bihmdW5jdGlvbihKb2JzU2VydmljZSwgTWFpblNlcnZpY2UsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIHtcbiAgcmV0dXJuIE1haW5TZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuKGZ1bmN0aW9uKGNvbmZpZykge1xuICAgIGFuZ3VsYXIuZXh0ZW5kKGZsaW5rQ29uZmlnLCBjb25maWcpO1xuICAgIEpvYnNTZXJ2aWNlLmxpc3RKb2JzKCk7XG4gICAgcmV0dXJuICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICAgIHJldHVybiBKb2JzU2VydmljZS5saXN0Sm9icygpO1xuICAgIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIH0pO1xufSkuY29uZmlnKGZ1bmN0aW9uKCRzdGF0ZVByb3ZpZGVyLCAkdXJsUm91dGVyUHJvdmlkZXIpIHtcbiAgJHN0YXRlUHJvdmlkZXIuc3RhdGUoXCJvdmVydmlld1wiLCB7XG4gICAgdXJsOiBcIi9vdmVydmlld1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL292ZXJ2aWV3Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ092ZXJ2aWV3Q29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwicnVubmluZy1qb2JzXCIsIHtcbiAgICB1cmw6IFwiL3J1bm5pbmctam9ic1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvcnVubmluZy1qb2JzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1J1bm5pbmdKb2JzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiY29tcGxldGVkLWpvYnNcIiwge1xuICAgIHVybDogXCIvY29tcGxldGVkLWpvYnNcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2NvbXBsZXRlZC1qb2JzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iXCIsIHtcbiAgICB1cmw6IFwiL2pvYnMve2pvYmlkfVwiLFxuICAgIGFic3RyYWN0OiB0cnVlLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZUpvYkNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhblwiLCB7XG4gICAgdXJsOiBcIlwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4uaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5ub2RlXCIsIHtcbiAgICB1cmw6IFwiL3ZlcnRleC97bm9kZWlkfVwiLFxuICAgIGFic3RyYWN0OiB0cnVlLFxuICAgIHZpZXdzOiB7XG4gICAgICBub2RlOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuTm9kZUNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5ub2RlLnN1YnRhc2tzXCIsIHtcbiAgICB1cmw6IFwiXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG5vZGVUYWI6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLnN1YnRhc2tzLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4ubm9kZS5wcm9wZXJ0aWVzXCIsIHtcbiAgICB1cmw6IFwiL3Byb3BlcnRpZXNcIixcbiAgICB2aWV3czoge1xuICAgICAgbm9kZVRhYjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUucHJvcGVydGllcy5odG1sXCJcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLm5vZGUuYWNjdW11bGF0b3JzXCIsIHtcbiAgICB1cmw6IFwiL3Byb3BlcnRpZXNcIixcbiAgICB2aWV3czoge1xuICAgICAgbm9kZVRhYjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUuYWNjdW11bGF0b3JzLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnRpbWVsaW5lXCIsIHtcbiAgICB1cmw6IFwiL3RpbWVsaW5lXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUuaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsIHtcbiAgICB1cmw6IFwiL3t2ZXJ0ZXhJZH1cIixcbiAgICB2aWV3czoge1xuICAgICAgdmVydGV4OiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLnZlcnRleC5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2Iuc3RhdGlzdGljc1wiLCB7XG4gICAgdXJsOiBcIi9zdGF0aXN0aWNzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2Iuc3RhdGlzdGljcy5odG1sXCJcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5leGNlcHRpb25zXCIsIHtcbiAgICB1cmw6IFwiL2V4Y2VwdGlvbnNcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5leGNlcHRpb25zLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYkV4Y2VwdGlvbnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLmNvbmZpZ1wiLCB7XG4gICAgdXJsOiBcIi9jb25maWdcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5jb25maWcuaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KTtcbiAgcmV0dXJuICR1cmxSb3V0ZXJQcm92aWRlci5vdGhlcndpc2UoXCIvb3ZlcnZpZXdcIik7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAnYnNMYWJlbCcsIChKb2JzU2VydmljZSkgLT5cbiAgdHJhbnNjbHVkZTogdHJ1ZVxuICByZXBsYWNlOiB0cnVlXG4gIHNjb3BlOiBcbiAgICBnZXRMYWJlbENsYXNzOiBcIiZcIlxuICAgIHN0YXR1czogXCJAXCJcblxuICB0ZW1wbGF0ZTogXCI8c3BhbiB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKSc+PG5nLXRyYW5zY2x1ZGU+PC9uZy10cmFuc2NsdWRlPjwvc3Bhbj5cIlxuICBcbiAgbGluazogKHNjb3BlLCBlbGVtZW50LCBhdHRycykgLT5cbiAgICBzY29wZS5nZXRMYWJlbENsYXNzID0gLT5cbiAgICAgICdsYWJlbCBsYWJlbC0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uZGlyZWN0aXZlICdpbmRpY2F0b3JQcmltYXJ5JywgKEpvYnNTZXJ2aWNlKSAtPlxuICByZXBsYWNlOiB0cnVlXG4gIHNjb3BlOiBcbiAgICBnZXRMYWJlbENsYXNzOiBcIiZcIlxuICAgIHN0YXR1czogJ0AnXG5cbiAgdGVtcGxhdGU6IFwiPGkgdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRMYWJlbENsYXNzKCknIC8+XCJcbiAgXG4gIGxpbms6IChzY29wZSwgZWxlbWVudCwgYXR0cnMpIC0+XG4gICAgc2NvcGUuZ2V0TGFiZWxDbGFzcyA9IC0+XG4gICAgICAnZmEgZmEtY2lyY2xlIGluZGljYXRvciBpbmRpY2F0b3ItJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAndGFibGVQcm9wZXJ0eScsIC0+XG4gIHJlcGxhY2U6IHRydWVcbiAgc2NvcGU6XG4gICAgdmFsdWU6ICc9J1xuXG4gIHRlbXBsYXRlOiBcIjx0ZCB0aXRsZT1cXFwie3t2YWx1ZSB8fCAnTm9uZSd9fVxcXCI+e3t2YWx1ZSB8fCAnTm9uZSd9fTwvdGQ+XCJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmRpcmVjdGl2ZSgnYnNMYWJlbCcsIGZ1bmN0aW9uKEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiB7XG4gICAgdHJhbnNjbHVkZTogdHJ1ZSxcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICBnZXRMYWJlbENsYXNzOiBcIiZcIixcbiAgICAgIHN0YXR1czogXCJAXCJcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjxzcGFuIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJz48bmctdHJhbnNjbHVkZT48L25nLXRyYW5zY2x1ZGU+PC9zcGFuPlwiLFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgcmV0dXJuIHNjb3BlLmdldExhYmVsQ2xhc3MgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuICdsYWJlbCBsYWJlbC0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpO1xuICAgICAgfTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ2luZGljYXRvclByaW1hcnknLCBmdW5jdGlvbihKb2JzU2VydmljZSkge1xuICByZXR1cm4ge1xuICAgIHJlcGxhY2U6IHRydWUsXG4gICAgc2NvcGU6IHtcbiAgICAgIGdldExhYmVsQ2xhc3M6IFwiJlwiLFxuICAgICAgc3RhdHVzOiAnQCdcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjxpIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJyAvPlwiLFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgcmV0dXJuIHNjb3BlLmdldExhYmVsQ2xhc3MgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuICdmYSBmYS1jaXJjbGUgaW5kaWNhdG9yIGluZGljYXRvci0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpO1xuICAgICAgfTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ3RhYmxlUHJvcGVydHknLCBmdW5jdGlvbigpIHtcbiAgcmV0dXJuIHtcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICB2YWx1ZTogJz0nXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8dGQgdGl0bGU9XFxcInt7dmFsdWUgfHwgJ05vbmUnfX1cXFwiPnt7dmFsdWUgfHwgJ05vbmUnfX08L3RkPlwiXG4gIH07XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uZmlsdGVyIFwiYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkXCIsIChhbmd1bGFyTW9tZW50Q29uZmlnKSAtPlxuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIgPSAodmFsdWUsIGZvcm1hdCwgZHVyYXRpb25Gb3JtYXQpIC0+XG4gICAgcmV0dXJuIFwiXCIgIGlmIHR5cGVvZiB2YWx1ZSBpcyBcInVuZGVmaW5lZFwiIG9yIHZhbHVlIGlzIG51bGxcblxuICAgIG1vbWVudC5kdXJhdGlvbih2YWx1ZSwgZm9ybWF0KS5mb3JtYXQoZHVyYXRpb25Gb3JtYXQsIHsgdHJpbTogZmFsc2UgfSlcblxuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIuJHN0YXRlZnVsID0gYW5ndWxhck1vbWVudENvbmZpZy5zdGF0ZWZ1bEZpbHRlcnNcblxuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXJcblxuLmZpbHRlciBcImh1bWFuaXplVGFza05hbWVcIiwgLT5cbiAgKHRleHQpIC0+XG4gICAgIyBUT0RPOiBleHRlbmQuLi4gYSBsb3RcbiAgICBpZiB0ZXh0IHRoZW4gdGV4dC5yZXBsYWNlKFwiJmd0O1wiLCBcIj5cIikucmVwbGFjZShcIjxici8+XCIsXCJcIikgZWxzZSAnJ1xuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuZmlsdGVyKFwiYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkXCIsIGZ1bmN0aW9uKGFuZ3VsYXJNb21lbnRDb25maWcpIHtcbiAgdmFyIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlcjtcbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyID0gZnVuY3Rpb24odmFsdWUsIGZvcm1hdCwgZHVyYXRpb25Gb3JtYXQpIHtcbiAgICBpZiAodHlwZW9mIHZhbHVlID09PSBcInVuZGVmaW5lZFwiIHx8IHZhbHVlID09PSBudWxsKSB7XG4gICAgICByZXR1cm4gXCJcIjtcbiAgICB9XG4gICAgcmV0dXJuIG1vbWVudC5kdXJhdGlvbih2YWx1ZSwgZm9ybWF0KS5mb3JtYXQoZHVyYXRpb25Gb3JtYXQsIHtcbiAgICAgIHRyaW06IGZhbHNlXG4gICAgfSk7XG4gIH07XG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlci4kc3RhdGVmdWwgPSBhbmd1bGFyTW9tZW50Q29uZmlnLnN0YXRlZnVsRmlsdGVycztcbiAgcmV0dXJuIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlcjtcbn0pLmZpbHRlcihcImh1bWFuaXplVGFza05hbWVcIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbih0ZXh0KSB7XG4gICAgaWYgKHRleHQpIHtcbiAgICAgIHJldHVybiB0ZXh0LnJlcGxhY2UoXCImZ3Q7XCIsIFwiPlwiKS5yZXBsYWNlKFwiPGJyLz5cIiwgXCJcIik7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiAnJztcbiAgICB9XG4gIH07XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnTWFpblNlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cbiAgam9iT2JzZXJ2ZXJzID0gW11cblxuICBAbG9hZENvbmZpZyA9IC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvY29uZmlnXCJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cblxuICBAXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdNYWluU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdmFyIGpvYk9ic2VydmVycztcbiAgam9iT2JzZXJ2ZXJzID0gW107XG4gIHRoaXMubG9hZENvbmZpZyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2NvbmZpZ1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uY29udHJvbGxlciAnUnVubmluZ0pvYnNDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxuICAgICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygncnVubmluZycpXG5cbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuXG4gICRzY29wZS5qb2JPYnNlcnZlcigpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxuICAgICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKVxuXG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcblxuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdTaW5nbGVKb2JDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlLCAkcm9vdFNjb3BlKSAtPlxuICAkc2NvcGUuam9iaWQgPSAkc3RhdGVQYXJhbXMuam9iaWRcbiAgJHJvb3RTY29wZS5qb2IgPSBudWxsXG5cbiAgSm9ic1NlcnZpY2UubG9hZEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHJvb3RTY29wZS5qb2IgPSBkYXRhXG5cbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgICRyb290U2NvcGUuam9iID0gbnVsbFxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgSm9ic1NlcnZpY2UubG9hZFBsYW4oJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuIChkYXRhKSAtPlxuICAgICRzY29wZS5wbGFuID0gZGF0YVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuTm9kZUNvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XG4gICRzY29wZS5ub2RlaWQgPSAkc3RhdGVQYXJhbXMubm9kZWlkXG4gICRzY29wZS5zdGF0ZUxpc3QgPSBKb2JzU2VydmljZS5zdGF0ZUxpc3QoKVxuXG4gIEpvYnNTZXJ2aWNlLmdldE5vZGUoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUubm9kZSA9IGRhdGFcblxuICBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUudmVydGV4ID0gZGF0YVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XG4gIEpvYnNTZXJ2aWNlLmdldFZlcnRleCgkc3RhdGVQYXJhbXMudmVydGV4SWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLnZlcnRleCA9IGRhdGFcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XG4gIEpvYnNTZXJ2aWNlLmxvYWRFeGNlcHRpb25zKCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUuZXhjZXB0aW9ucyA9IGRhdGFcblxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignUnVubmluZ0pvYnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygncnVubmluZycpO1xuICB9O1xuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS5qb2JPYnNlcnZlcigpO1xufSkuY29udHJvbGxlcignQ29tcGxldGVkSm9ic0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpO1xuICB9O1xuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS5qb2JPYnNlcnZlcigpO1xufSkuY29udHJvbGxlcignU2luZ2xlSm9iQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlLCAkcm9vdFNjb3BlKSB7XG4gICRzY29wZS5qb2JpZCA9ICRzdGF0ZVBhcmFtcy5qb2JpZDtcbiAgJHJvb3RTY29wZS5qb2IgPSBudWxsO1xuICBKb2JzU2VydmljZS5sb2FkSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRyb290U2NvcGUuam9iID0gZGF0YTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkcm9vdFNjb3BlLmpvYiA9IG51bGw7XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUGxhbkNvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICByZXR1cm4gSm9ic1NlcnZpY2UubG9hZFBsYW4oJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLnBsYW4gPSBkYXRhO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5Ob2RlQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gICRzY29wZS5ub2RlaWQgPSAkc3RhdGVQYXJhbXMubm9kZWlkO1xuICAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KCk7XG4gIEpvYnNTZXJ2aWNlLmdldE5vZGUoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5ub2RlID0gZGF0YTtcbiAgfSk7XG4gIHJldHVybiBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS52ZXJ0ZXggPSBkYXRhO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHN0YXRlUGFyYW1zLnZlcnRleElkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLnZlcnRleCA9IGRhdGE7XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICByZXR1cm4gSm9ic1NlcnZpY2UubG9hZEV4Y2VwdGlvbnMoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLmV4Y2VwdGlvbnMgPSBkYXRhO1xuICB9KTtcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uZGlyZWN0aXZlICd2ZXJ0ZXgnLCAoJHN0YXRlKSAtPlxuICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSd0aW1lbGluZSBzZWNvbmRhcnknIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiXG5cbiAgc2NvcGU6XG4gICAgZGF0YTogXCI9XCJcblxuICBsaW5rOiAoc2NvcGUsIGVsZW0sIGF0dHJzKSAtPlxuICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdXG5cbiAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpXG4gICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcpXG5cbiAgICBhbmFseXplVGltZSA9IChkYXRhKSAtPlxuICAgICAgdGVzdERhdGEgPSBbXVxuXG4gICAgICBhbmd1bGFyLmZvckVhY2ggZGF0YS5zdWJ0YXNrcywgKHN1YnRhc2ssIGkpIC0+XG4gICAgICAgIHRlc3REYXRhLnB1c2gge1xuICAgICAgICAgIGxhYmVsOiBcIiN7c3VidGFzay5ob3N0fSAoI3tzdWJ0YXNrLnN1YnRhc2t9KVwiXG4gICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgbGFiZWw6IFwiU2NoZWR1bGVkXCJcbiAgICAgICAgICAgICAgY29sb3I6IFwiIzY2NlwiXG4gICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIlxuICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJTQ0hFRFVMRURcIl1cbiAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkRFUExPWUlOR1wiXVxuICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgbGFiZWw6IFwiRGVwbG95aW5nXCJcbiAgICAgICAgICAgICAgY29sb3I6IFwiI2FhYVwiXG4gICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIlxuICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl1cbiAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl1cbiAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICB9XG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgIGxhYmVsOiBcIlJ1bm5pbmdcIlxuICAgICAgICAgICAgICBjb2xvcjogXCIjZGRkXCJcbiAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl1cbiAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkZJTklTSEVEXCJdXG4gICAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgICAgfVxuICAgICAgICAgIF1cbiAgICAgICAgfVxuXG4gICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKVxuICAgICAgLnRpY2tGb3JtYXQoe1xuICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIilcbiAgICAgICAgIyB0aWNrSW50ZXJ2YWw6IDFcbiAgICAgICAgdGlja1NpemU6IDFcbiAgICAgIH0pXG4gICAgICAucHJlZml4KFwic2luZ2xlXCIpXG4gICAgICAubGFiZWxGb3JtYXQoKGxhYmVsKSAtPlxuICAgICAgICBsYWJlbFxuICAgICAgKVxuICAgICAgLm1hcmdpbih7IGxlZnQ6IDEwMCwgcmlnaHQ6IDAsIHRvcDogMCwgYm90dG9tOiAwIH0pXG4gICAgICAuaXRlbUhlaWdodCgzMClcbiAgICAgIC5yZWxhdGl2ZVRpbWUoKVxuXG4gICAgICBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpXG4gICAgICAuZGF0dW0odGVzdERhdGEpXG4gICAgICAuY2FsbChjaGFydClcblxuICAgIGFuYWx5emVUaW1lKHNjb3BlLmRhdGEpXG5cbiAgICByZXR1cm5cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ3RpbWVsaW5lJywgKCRzdGF0ZSkgLT5cbiAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUnIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiXG5cbiAgc2NvcGU6XG4gICAgam9iOiBcIj1cIlxuXG4gIGxpbms6IChzY29wZSwgZWxlbSwgYXR0cnMpIC0+XG4gICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF1cblxuICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKClcbiAgICBhbmd1bGFyLmVsZW1lbnQoc3ZnRWwpLmF0dHIoJ3dpZHRoJywgY29udGFpbmVyVylcblxuICAgIHRyYW5zbGF0ZUxhYmVsID0gKGxhYmVsKSAtPlxuICAgICAgbGFiZWwucmVwbGFjZShcIiZndDtcIiwgXCI+XCIpXG5cbiAgICBhbmFseXplVGltZSA9IChkYXRhKSAtPlxuICAgICAgdGVzdERhdGEgPSBbXVxuXG4gICAgICB0ZXN0RGF0YS5wdXNoIFxuICAgICAgICB0aW1lczogW1xuICAgICAgICAgIGxhYmVsOiBcIlNjaGVkdWxlZFwiXG4gICAgICAgICAgY29sb3I6IFwiI2NjY2NjY1wiXG4gICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiXG4gICAgICAgICAgc3RhcnRpbmdfdGltZTogZGF0YS50aW1lc3RhbXBzW1wiQ1JFQVRFRFwiXVxuICAgICAgICAgIGVuZGluZ190aW1lOiBkYXRhLnRpbWVzdGFtcHNbXCJDUkVBVEVEXCJdICsgMVxuICAgICAgICAgIHR5cGU6ICdzY2hlZHVsZWQnXG4gICAgICAgIF1cblxuXG4gICAgICBhbmd1bGFyLmZvckVhY2ggZGF0YS52ZXJ0aWNlcywgKHZlcnRleCkgLT5cbiAgICAgICAgaWYgdmVydGV4WydzdGFydC10aW1lJ10gPiAtMVxuICAgICAgICAgIHRlc3REYXRhLnB1c2ggXG4gICAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAgICBsYWJlbDogdHJhbnNsYXRlTGFiZWwodmVydGV4Lm5hbWUpXG4gICAgICAgICAgICAgIGNvbG9yOiBcIiNkOWYxZjdcIlxuICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNjJjZGVhXCJcbiAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdmVydGV4WydzdGFydC10aW1lJ11cbiAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZlcnRleFsnZW5kLXRpbWUnXVxuICAgICAgICAgICAgICBsaW5rOiB2ZXJ0ZXguaWRcbiAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICBdXG5cbiAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpLmNsaWNrKChkLCBpLCBkYXR1bSkgLT5cbiAgICAgICAgaWYgZC5saW5rXG4gICAgICAgICAgJHN0YXRlLmdvIFwic2luZ2xlLWpvYi50aW1lbGluZS52ZXJ0ZXhcIiwgeyBqb2JpZDogZGF0YS5qaWQsIHZlcnRleElkOiBkLmxpbmsgfVxuXG4gICAgICApXG4gICAgICAudGlja0Zvcm1hdCh7XG4gICAgICAgIGZvcm1hdDogZDMudGltZS5mb3JtYXQoXCIlTFwiKVxuICAgICAgICAjIHRpY2tUaW1lOiBkMy50aW1lLnNlY29uZFxuICAgICAgICAjIHRpY2tJbnRlcnZhbDogMC41XG4gICAgICAgIHRpY2tTaXplOiAxXG4gICAgICB9KVxuICAgICAgLnByZWZpeChcIm1haW5cIilcbiAgICAgIC5tYXJnaW4oeyBsZWZ0OiAwLCByaWdodDogMCwgdG9wOiAwLCBib3R0b206IDAgfSlcbiAgICAgIC5pdGVtSGVpZ2h0KDMwKVxuICAgICAgLnNob3dCb3JkZXJMaW5lKClcbiAgICAgIC5zaG93SG91clRpbWVsaW5lKClcblxuICAgICAgc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKVxuICAgICAgLmRhdHVtKHRlc3REYXRhKVxuICAgICAgLmNhbGwoY2hhcnQpXG5cbiAgICBzY29wZS4kd2F0Y2ggYXR0cnMuam9iLCAoZGF0YSkgLT5cbiAgICAgIGFuYWx5emVUaW1lKGRhdGEpIGlmIGRhdGFcblxuICAgIHJldHVyblxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAnam9iUGxhbicsICgkdGltZW91dCkgLT5cbiAgdGVtcGxhdGU6IFwiXG4gICAgPHN2ZyBjbGFzcz0nZ3JhcGgnIHdpZHRoPSc1MDAnIGhlaWdodD0nNDAwJz48ZyAvPjwvc3ZnPlxuICAgIDxzdmcgY2xhc3M9J3RtcCcgd2lkdGg9JzEnIGhlaWdodD0nMSc+PGcgLz48L3N2Zz5cbiAgICA8ZGl2IGNsYXNzPSdidG4tZ3JvdXAgem9vbS1idXR0b25zJz5cbiAgICAgIDxhIGNsYXNzPSdidG4gYnRuLWRlZmF1bHQgem9vbS1pbicgbmctY2xpY2s9J3pvb21JbigpJz48aSBjbGFzcz0nZmEgZmEtcGx1cycgLz48L2E+XG4gICAgICA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20tb3V0JyBuZy1jbGljaz0nem9vbU91dCgpJz48aSBjbGFzcz0nZmEgZmEtbWludXMnIC8+PC9hPlxuICAgIDwvZGl2PlwiXG5cbiAgc2NvcGU6XG4gICAgcGxhbjogJz0nXG5cbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cbiAgICBtYWluWm9vbSA9IGQzLmJlaGF2aW9yLnpvb20oKVxuICAgIHN1YmdyYXBocyA9IFtdXG4gICAgam9iaWQgPSBhdHRycy5qb2JpZFxuXG4gICAgbWFpblN2Z0VsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMF1cbiAgICBtYWluRyA9IGVsZW0uY2hpbGRyZW4oKS5jaGlsZHJlbigpWzBdXG4gICAgbWFpblRtcEVsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMV1cblxuICAgIGQzbWFpblN2ZyA9IGQzLnNlbGVjdChtYWluU3ZnRWxlbWVudClcbiAgICBkM21haW5TdmdHID0gZDMuc2VsZWN0KG1haW5HKVxuICAgIGQzdG1wU3ZnID0gZDMuc2VsZWN0KG1haW5UbXBFbGVtZW50KVxuXG4gICAgIyBhbmd1bGFyLmVsZW1lbnQobWFpbkcpLmVtcHR5KClcblxuICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKClcbiAgICBhbmd1bGFyLmVsZW1lbnQoZWxlbS5jaGlsZHJlbigpWzBdKS53aWR0aChjb250YWluZXJXKVxuXG4gICAgc2NvcGUuem9vbUluID0gLT5cbiAgICAgIGlmIG1haW5ab29tLnNjYWxlKCkgPCAyLjk5XG4gICAgICAgIFxuICAgICAgICAjIENhbGN1bGF0ZSBhbmQgc3RvcmUgbmV3IHZhbHVlcyBpbiB6b29tIG9iamVjdFxuICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKVxuICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxuICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxuICAgICAgICBtYWluWm9vbS5zY2FsZSBtYWluWm9vbS5zY2FsZSgpICsgMC4xXG4gICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZSBbIHYxLCB2MiBdXG4gICAgICAgIFxuICAgICAgICAjIFRyYW5zZm9ybSBzdmdcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyIFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiXG5cbiAgICBzY29wZS56b29tT3V0ID0gLT5cbiAgICAgIGlmIG1haW5ab29tLnNjYWxlKCkgPiAwLjMxXG4gICAgICAgIFxuICAgICAgICAjIENhbGN1bGF0ZSBhbmQgc3RvcmUgbmV3IHZhbHVlcyBpbiBtYWluWm9vbSBvYmplY3RcbiAgICAgICAgbWFpblpvb20uc2NhbGUgbWFpblpvb20uc2NhbGUoKSAtIDAuMVxuICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKVxuICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxuICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxuICAgICAgICBtYWluWm9vbS50cmFuc2xhdGUgWyB2MSwgdjIgXVxuICAgICAgICBcbiAgICAgICAgIyBUcmFuc2Zvcm0gc3ZnXG4gICAgICAgIGQzbWFpblN2Z0cuYXR0ciBcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIlxuXG4gICAgI2NyZWF0ZSBhIGxhYmVsIG9mIGFuIGVkZ2VcbiAgICBjcmVhdGVMYWJlbEVkZ2UgPSAoZWwpIC0+XG4gICAgICBsYWJlbFZhbHVlID0gXCJcIlxuICAgICAgaWYgZWwuc2hpcF9zdHJhdGVneT8gb3IgZWwubG9jYWxfc3RyYXRlZ3k/XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8ZGl2IGNsYXNzPSdlZGdlLWxhYmVsJz5cIlxuICAgICAgICBsYWJlbFZhbHVlICs9IGVsLnNoaXBfc3RyYXRlZ3kgIGlmIGVsLnNoaXBfc3RyYXRlZ3k/XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCIgKFwiICsgZWwudGVtcF9tb2RlICsgXCIpXCIgIHVubGVzcyBlbC50ZW1wX21vZGUgaXMgYHVuZGVmaW5lZGBcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIiw8YnI+XCIgKyBlbC5sb2NhbF9zdHJhdGVneSAgdW5sZXNzIGVsLmxvY2FsX3N0cmF0ZWd5IGlzIGB1bmRlZmluZWRgXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2Rpdj5cIlxuICAgICAgbGFiZWxWYWx1ZVxuXG5cbiAgICAjIHRydWUsIGlmIHRoZSBub2RlIGlzIGEgc3BlY2lhbCBub2RlIGZyb20gYW4gaXRlcmF0aW9uXG4gICAgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZSA9IChpbmZvKSAtPlxuICAgICAgKGluZm8gaXMgXCJwYXJ0aWFsU29sdXRpb25cIiBvciBpbmZvIGlzIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiIG9yIGluZm8gaXMgXCJ3b3Jrc2V0XCIgb3IgaW5mbyBpcyBcIm5leHRXb3Jrc2V0XCIgb3IgaW5mbyBpcyBcInNvbHV0aW9uU2V0XCIgb3IgaW5mbyBpcyBcInNvbHV0aW9uRGVsdGFcIilcblxuICAgIGdldE5vZGVUeXBlID0gKGVsLCBpbmZvKSAtPlxuICAgICAgaWYgaW5mbyBpcyBcIm1pcnJvclwiXG4gICAgICAgICdub2RlLW1pcnJvcidcblxuICAgICAgZWxzZSBpZiBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pXG4gICAgICAgICdub2RlLWl0ZXJhdGlvbidcblxuICAgICAgZWxzZVxuICAgICAgICAgICdub2RlLW5vcm1hbCdcbiAgICAgIFxuICAgICMgY3JlYXRlcyB0aGUgbGFiZWwgb2YgYSBub2RlLCBpbiBpbmZvIGlzIHN0b3JlZCwgd2hldGhlciBpdCBpcyBhIHNwZWNpYWwgbm9kZSAobGlrZSBhIG1pcnJvciBpbiBhbiBpdGVyYXRpb24pXG4gICAgY3JlYXRlTGFiZWxOb2RlID0gKGVsLCBpbmZvLCBtYXhXLCBtYXhIKSAtPlxuICAgICAgbGFiZWxWYWx1ZSA9IFwiPGEgaHJlZj0nIy9qb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0ZXgvXCIgKyBlbC5pZCArIFwiJyBjbGFzcz0nbm9kZS1sYWJlbCBcIiArIGdldE5vZGVUeXBlKGVsLCBpbmZvKSArIFwiJz5cIlxuXG4gICAgICAjIE5vZGVuYW1lXG4gICAgICBpZiBpbmZvIGlzIFwibWlycm9yXCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoMyBjbGFzcz0nbm9kZS1uYW1lJz5NaXJyb3Igb2YgXCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIlxuICAgICAgZWxzZVxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPlwiICsgZWwub3BlcmF0b3IgKyBcIjwvaDM+XCJcbiAgICAgIGlmIGVsLmRlc2NyaXB0aW9uIGlzIFwiXCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIlwiXG4gICAgICBlbHNlXG4gICAgICAgIHN0ZXBOYW1lID0gZWwuZGVzY3JpcHRpb25cbiAgICAgICAgXG4gICAgICAgICMgY2xlYW4gc3RlcE5hbWVcbiAgICAgICAgc3RlcE5hbWUgPSBzaG9ydGVuU3RyaW5nKHN0ZXBOYW1lKVxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg0IGNsYXNzPSdzdGVwLW5hbWUnPlwiICsgc3RlcE5hbWUgKyBcIjwvaDQ+XCJcbiAgICAgIFxuICAgICAgIyBJZiB0aGlzIG5vZGUgaXMgYW4gXCJpdGVyYXRpb25cIiB3ZSBuZWVkIGEgZGlmZmVyZW50IHBhbmVsLWJvZHlcbiAgICAgIGlmIGVsLnN0ZXBfZnVuY3Rpb24/XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uKGVsLmlkLCBtYXhXLCBtYXhIKVxuICAgICAgZWxzZVxuICAgICAgICBcbiAgICAgICAgIyBPdGhlcndpc2UgYWRkIGluZm9zICAgIFxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlwiICsgaW5mbyArIFwiIE5vZGU8L2g1PlwiICBpZiBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+UGFyYWxsZWxpc206IFwiICsgZWwucGFyYWxsZWxpc20gKyBcIjwvaDU+XCIgIHVubGVzcyBlbC5wYXJhbGxlbGlzbSBpcyBcIlwiXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+T3BlcmF0aW9uOiBcIiArIHNob3J0ZW5TdHJpbmcoZWwub3BlcmF0b3Jfc3RyYXRlZ3kpICsgXCI8L2g1PlwiICB1bmxlc3MgZWwub3BlcmF0b3IgaXMgYHVuZGVmaW5lZGBcbiAgICAgIFxuICAgICAgbGFiZWxWYWx1ZSArPSBcIjwvYT5cIlxuICAgICAgbGFiZWxWYWx1ZVxuXG4gICAgIyBFeHRlbmRzIHRoZSBsYWJlbCBvZiBhIG5vZGUgd2l0aCBhbiBhZGRpdGlvbmFsIHN2ZyBFbGVtZW50IHRvIHByZXNlbnQgdGhlIGl0ZXJhdGlvbi5cbiAgICBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24gPSAoaWQsIG1heFcsIG1heEgpIC0+XG4gICAgICBzdmdJRCA9IFwic3ZnLVwiICsgaWRcblxuICAgICAgbGFiZWxWYWx1ZSA9IFwiPHN2ZyBjbGFzcz0nXCIgKyBzdmdJRCArIFwiJyB3aWR0aD1cIiArIG1heFcgKyBcIiBoZWlnaHQ9XCIgKyBtYXhIICsgXCI+PGcgLz48L3N2Zz5cIlxuICAgICAgbGFiZWxWYWx1ZVxuXG4gICAgIyBTcGxpdCBhIHN0cmluZyBpbnRvIG11bHRpcGxlIGxpbmVzIHNvIHRoYXQgZWFjaCBsaW5lIGhhcyBsZXNzIHRoYW4gMzAgbGV0dGVycy5cbiAgICBzaG9ydGVuU3RyaW5nID0gKHMpIC0+XG4gICAgICAjIG1ha2Ugc3VyZSB0aGF0IG5hbWUgZG9lcyBub3QgY29udGFpbiBhIDwgKGJlY2F1c2Ugb2YgaHRtbClcbiAgICAgIGlmIHMuY2hhckF0KDApIGlzIFwiPFwiXG4gICAgICAgIHMgPSBzLnJlcGxhY2UoXCI8XCIsIFwiJmx0O1wiKVxuICAgICAgICBzID0gcy5yZXBsYWNlKFwiPlwiLCBcIiZndDtcIilcbiAgICAgIHNiciA9IFwiXCJcbiAgICAgIHdoaWxlIHMubGVuZ3RoID4gMzBcbiAgICAgICAgc2JyID0gc2JyICsgcy5zdWJzdHJpbmcoMCwgMzApICsgXCI8YnI+XCJcbiAgICAgICAgcyA9IHMuc3Vic3RyaW5nKDMwLCBzLmxlbmd0aClcbiAgICAgIHNiciA9IHNiciArIHNcbiAgICAgIHNiclxuXG4gICAgY3JlYXRlTm9kZSA9IChnLCBkYXRhLCBlbCwgaXNQYXJlbnQgPSBmYWxzZSwgbWF4VywgbWF4SCkgLT5cbiAgICAgICMgY3JlYXRlIG5vZGUsIHNlbmQgYWRkaXRpb25hbCBpbmZvcm1hdGlvbnMgYWJvdXQgdGhlIG5vZGUgaWYgaXQgaXMgYSBzcGVjaWFsIG9uZVxuICAgICAgaWYgZWwuaWQgaXMgZGF0YS5wYXJ0aWFsX3NvbHV0aW9uXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEubmV4dF9wYXJ0aWFsX3NvbHV0aW9uXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS53b3Jrc2V0XG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIndvcmtzZXRcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJ3b3Jrc2V0XCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS5uZXh0X3dvcmtzZXRcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFdvcmtzZXRcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0V29ya3NldFwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEuc29sdXRpb25fc2V0XG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uU2V0XCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25TZXRcIilcblxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLnNvbHV0aW9uX2RlbHRhXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uRGVsdGFcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIpXG5cbiAgICAgIGVsc2VcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwiXCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwiXCIpXG5cbiAgICBjcmVhdGVFZGdlID0gKGcsIGRhdGEsIGVsLCBleGlzdGluZ05vZGVzLCBwcmVkKSAtPlxuICAgICAgdW5sZXNzIGV4aXN0aW5nTm9kZXMuaW5kZXhPZihwcmVkLmlkKSBpcyAtMVxuICAgICAgICBnLnNldEVkZ2UgcHJlZC5pZCwgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShwcmVkKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgYXJyb3doZWFkOiAnbm9ybWFsJ1xuXG4gICAgICBlbHNlXG4gICAgICAgIG1pc3NpbmdOb2RlID0gc2VhcmNoRm9yTm9kZShkYXRhLCBwcmVkLmlkKVxuICAgICAgICB1bmxlc3MgIW1pc3NpbmdOb2RlIG9yIG1pc3NpbmdOb2RlLmFscmVhZHlBZGRlZCBpcyB0cnVlXG4gICAgICAgICAgbWlzc2luZ05vZGUuYWxyZWFkeUFkZGVkID0gdHJ1ZVxuICAgICAgICAgIGcuc2V0Tm9kZSBtaXNzaW5nTm9kZS5pZCxcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUobWlzc2luZ05vZGUsIFwibWlycm9yXCIpXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKG1pc3NpbmdOb2RlLCAnbWlycm9yJylcblxuICAgICAgICAgIGcuc2V0RWRnZSBtaXNzaW5nTm9kZS5pZCwgZWwuaWQsXG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxFZGdlKG1pc3NpbmdOb2RlKVxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcblxuICAgIGxvYWRKc29uVG9EYWdyZSA9IChnLCBkYXRhKSAtPlxuICAgICAgZXhpc3RpbmdOb2RlcyA9IFtdXG5cbiAgICAgIGlmIGRhdGEubm9kZXM/XG4gICAgICAgICMgVGhpcyBpcyB0aGUgbm9ybWFsIGpzb24gZGF0YVxuICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLm5vZGVzXG5cbiAgICAgIGVsc2VcbiAgICAgICAgIyBUaGlzIGlzIGFuIGl0ZXJhdGlvbiwgd2Ugbm93IHN0b3JlIHNwZWNpYWwgaXRlcmF0aW9uIG5vZGVzIGlmIHBvc3NpYmxlXG4gICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEuc3RlcF9mdW5jdGlvblxuICAgICAgICBpc1BhcmVudCA9IHRydWVcblxuICAgICAgZm9yIGVsIGluIHRvSXRlcmF0ZVxuICAgICAgICBtYXhXID0gMFxuICAgICAgICBtYXhIID0gMFxuXG4gICAgICAgIGlmIGVsLnN0ZXBfZnVuY3Rpb25cbiAgICAgICAgICBzZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHsgbXVsdGlncmFwaDogdHJ1ZSwgY29tcG91bmQ6IHRydWUgfSkuc2V0R3JhcGgoe1xuICAgICAgICAgICAgbm9kZXNlcDogMjBcbiAgICAgICAgICAgIGVkZ2VzZXA6IDBcbiAgICAgICAgICAgIHJhbmtzZXA6IDIwXG4gICAgICAgICAgICByYW5rZGlyOiBcIkxSXCJcbiAgICAgICAgICAgIG1hcmdpbng6IDEwXG4gICAgICAgICAgICBtYXJnaW55OiAxMFxuICAgICAgICAgICAgfSlcblxuICAgICAgICAgIHN1YmdyYXBoc1tlbC5pZF0gPSBzZ1xuXG4gICAgICAgICAgbG9hZEpzb25Ub0RhZ3JlKHNnLCBlbClcblxuICAgICAgICAgIHIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKVxuICAgICAgICAgIGQzdG1wU3ZnLnNlbGVjdCgnZycpLmNhbGwociwgc2cpXG4gICAgICAgICAgbWF4VyA9IHNnLmdyYXBoKCkud2lkdGhcbiAgICAgICAgICBtYXhIID0gc2cuZ3JhcGgoKS5oZWlnaHRcblxuICAgICAgICAgIGFuZ3VsYXIuZWxlbWVudChtYWluVG1wRWxlbWVudCkuZW1wdHkoKVxuXG4gICAgICAgIGNyZWF0ZU5vZGUoZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKVxuXG4gICAgICAgIGV4aXN0aW5nTm9kZXMucHVzaCBlbC5pZFxuICAgICAgICBcbiAgICAgICAgIyBjcmVhdGUgZWRnZXMgZnJvbSBpbnB1dHMgdG8gY3VycmVudCBub2RlXG4gICAgICAgIGlmIGVsLmlucHV0cz9cbiAgICAgICAgICBmb3IgcHJlZCBpbiBlbC5pbnB1dHNcbiAgICAgICAgICAgIGNyZWF0ZUVkZ2UoZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQpXG5cbiAgICAgIGdcblxuICAgICMgc2VhcmNoZXMgaW4gdGhlIGdsb2JhbCBKU09ORGF0YSBmb3IgdGhlIG5vZGUgd2l0aCB0aGUgZ2l2ZW4gaWRcbiAgICBzZWFyY2hGb3JOb2RlID0gKGRhdGEsIG5vZGVJRCkgLT5cbiAgICAgIGZvciBpIG9mIGRhdGEubm9kZXNcbiAgICAgICAgZWwgPSBkYXRhLm5vZGVzW2ldXG4gICAgICAgIHJldHVybiBlbCAgaWYgZWwuaWQgaXMgbm9kZUlEXG4gICAgICAgIFxuICAgICAgICAjIGxvb2sgZm9yIG5vZGVzIHRoYXQgYXJlIGluIGl0ZXJhdGlvbnNcbiAgICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvbj9cbiAgICAgICAgICBmb3IgaiBvZiBlbC5zdGVwX2Z1bmN0aW9uXG4gICAgICAgICAgICByZXR1cm4gZWwuc3RlcF9mdW5jdGlvbltqXSAgaWYgZWwuc3RlcF9mdW5jdGlvbltqXS5pZCBpcyBub2RlSURcblxuICAgIGRyYXdHcmFwaCA9IChkYXRhKSAtPlxuICAgICAgZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHsgbXVsdGlncmFwaDogdHJ1ZSwgY29tcG91bmQ6IHRydWUgfSkuc2V0R3JhcGgoe1xuICAgICAgICBub2Rlc2VwOiA3MFxuICAgICAgICBlZGdlc2VwOiAwXG4gICAgICAgIHJhbmtzZXA6IDUwXG4gICAgICAgIHJhbmtkaXI6IFwiTFJcIlxuICAgICAgICBtYXJnaW54OiA0MFxuICAgICAgICBtYXJnaW55OiA0MFxuICAgICAgICB9KVxuXG4gICAgICBsb2FkSnNvblRvRGFncmUoZywgZGF0YSlcblxuICAgICAgcmVuZGVyZXIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKVxuICAgICAgZDNtYWluU3ZnRy5jYWxsKHJlbmRlcmVyLCBnKVxuXG4gICAgICBmb3IgaSwgc2cgb2Ygc3ViZ3JhcGhzXG4gICAgICAgIGQzbWFpblN2Zy5zZWxlY3QoJ3N2Zy5zdmctJyArIGkgKyAnIGcnKS5jYWxsKHJlbmRlcmVyLCBzZylcblxuICAgICAgbmV3U2NhbGUgPSAwLjVcblxuICAgICAgeENlbnRlck9mZnNldCA9IE1hdGguZmxvb3IoKGFuZ3VsYXIuZWxlbWVudChtYWluU3ZnRWxlbWVudCkud2lkdGgoKSAtIGcuZ3JhcGgoKS53aWR0aCAqIG5ld1NjYWxlKSAvIDIpXG4gICAgICB5Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS5oZWlnaHQoKSAtIGcuZ3JhcGgoKS5oZWlnaHQgKiBuZXdTY2FsZSkgLyAyKVxuXG4gICAgICBtYWluWm9vbS5zY2FsZShuZXdTY2FsZSkudHJhbnNsYXRlKFt4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0XSlcblxuICAgICAgZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgeENlbnRlck9mZnNldCArIFwiLCBcIiArIHlDZW50ZXJPZmZzZXQgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpXG5cbiAgICAgIG1haW5ab29tLm9uKFwiem9vbVwiLCAtPlxuICAgICAgICBldiA9IGQzLmV2ZW50XG4gICAgICAgIGQzbWFpblN2Z0cuYXR0ciBcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZSArIFwiKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIpXCJcbiAgICAgIClcbiAgICAgIG1haW5ab29tKGQzbWFpblN2ZylcblxuICAgIHNjb3BlLiR3YXRjaCBhdHRycy5wbGFuLCAobmV3UGxhbikgLT5cbiAgICAgIGRyYXdHcmFwaChuZXdQbGFuKSBpZiBuZXdQbGFuXG5cbiAgICByZXR1cm5cbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmRpcmVjdGl2ZSgndmVydGV4JywgZnVuY3Rpb24oJHN0YXRlKSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUgc2Vjb25kYXJ5JyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIixcbiAgICBzY29wZToge1xuICAgICAgZGF0YTogXCI9XCJcbiAgICB9LFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtLCBhdHRycykge1xuICAgICAgdmFyIGFuYWx5emVUaW1lLCBjb250YWluZXJXLCBzdmdFbDtcbiAgICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdO1xuICAgICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKTtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKTtcbiAgICAgIGFuYWx5emVUaW1lID0gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgY2hhcnQsIHN2ZywgdGVzdERhdGE7XG4gICAgICAgIHRlc3REYXRhID0gW107XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLnN1YnRhc2tzLCBmdW5jdGlvbihzdWJ0YXNrLCBpKSB7XG4gICAgICAgICAgcmV0dXJuIHRlc3REYXRhLnB1c2goe1xuICAgICAgICAgICAgbGFiZWw6IHN1YnRhc2suaG9zdCArIFwiIChcIiArIHN1YnRhc2suc3VidGFzayArIFwiKVwiLFxuICAgICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIGxhYmVsOiBcIlNjaGVkdWxlZFwiLFxuICAgICAgICAgICAgICAgIGNvbG9yOiBcIiM2NjZcIixcbiAgICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCIsXG4gICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiU0NIRURVTEVEXCJdLFxuICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl0sXG4gICAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICAgIH0sIHtcbiAgICAgICAgICAgICAgICBsYWJlbDogXCJEZXBsb3lpbmdcIixcbiAgICAgICAgICAgICAgICBjb2xvcjogXCIjYWFhXCIsXG4gICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiLFxuICAgICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkRFUExPWUlOR1wiXSxcbiAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiUlVOTklOR1wiXSxcbiAgICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgICAgfSwge1xuICAgICAgICAgICAgICAgIGxhYmVsOiBcIlJ1bm5pbmdcIixcbiAgICAgICAgICAgICAgICBjb2xvcjogXCIjZGRkXCIsXG4gICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiLFxuICAgICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl0sXG4gICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkZJTklTSEVEXCJdLFxuICAgICAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICBdXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0pO1xuICAgICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKS50aWNrRm9ybWF0KHtcbiAgICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIiksXG4gICAgICAgICAgdGlja1NpemU6IDFcbiAgICAgICAgfSkucHJlZml4KFwic2luZ2xlXCIpLmxhYmVsRm9ybWF0KGZ1bmN0aW9uKGxhYmVsKSB7XG4gICAgICAgICAgcmV0dXJuIGxhYmVsO1xuICAgICAgICB9KS5tYXJnaW4oe1xuICAgICAgICAgIGxlZnQ6IDEwMCxcbiAgICAgICAgICByaWdodDogMCxcbiAgICAgICAgICB0b3A6IDAsXG4gICAgICAgICAgYm90dG9tOiAwXG4gICAgICAgIH0pLml0ZW1IZWlnaHQoMzApLnJlbGF0aXZlVGltZSgpO1xuICAgICAgICByZXR1cm4gc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKS5kYXR1bSh0ZXN0RGF0YSkuY2FsbChjaGFydCk7XG4gICAgICB9O1xuICAgICAgYW5hbHl6ZVRpbWUoc2NvcGUuZGF0YSk7XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCd0aW1lbGluZScsIGZ1bmN0aW9uKCRzdGF0ZSkge1xuICByZXR1cm4ge1xuICAgIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lJyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIixcbiAgICBzY29wZToge1xuICAgICAgam9iOiBcIj1cIlxuICAgIH0sXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW0sIGF0dHJzKSB7XG4gICAgICB2YXIgYW5hbHl6ZVRpbWUsIGNvbnRhaW5lclcsIHN2Z0VsLCB0cmFuc2xhdGVMYWJlbDtcbiAgICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdO1xuICAgICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKTtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKTtcbiAgICAgIHRyYW5zbGF0ZUxhYmVsID0gZnVuY3Rpb24obGFiZWwpIHtcbiAgICAgICAgcmV0dXJuIGxhYmVsLnJlcGxhY2UoXCImZ3Q7XCIsIFwiPlwiKTtcbiAgICAgIH07XG4gICAgICBhbmFseXplVGltZSA9IGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGNoYXJ0LCBzdmcsIHRlc3REYXRhO1xuICAgICAgICB0ZXN0RGF0YSA9IFtdO1xuICAgICAgICB0ZXN0RGF0YS5wdXNoKHtcbiAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAge1xuICAgICAgICAgICAgICBsYWJlbDogXCJTY2hlZHVsZWRcIixcbiAgICAgICAgICAgICAgY29sb3I6IFwiI2NjY2NjY1wiLFxuICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCIsXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IGRhdGEudGltZXN0YW1wc1tcIkNSRUFURURcIl0sXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiBkYXRhLnRpbWVzdGFtcHNbXCJDUkVBVEVEXCJdICsgMSxcbiAgICAgICAgICAgICAgdHlwZTogJ3NjaGVkdWxlZCdcbiAgICAgICAgICAgIH1cbiAgICAgICAgICBdXG4gICAgICAgIH0pO1xuICAgICAgICBhbmd1bGFyLmZvckVhY2goZGF0YS52ZXJ0aWNlcywgZnVuY3Rpb24odmVydGV4KSB7XG4gICAgICAgICAgaWYgKHZlcnRleFsnc3RhcnQtdGltZSddID4gLTEpIHtcbiAgICAgICAgICAgIHJldHVybiB0ZXN0RGF0YS5wdXNoKHtcbiAgICAgICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgICBsYWJlbDogdHJhbnNsYXRlTGFiZWwodmVydGV4Lm5hbWUpLFxuICAgICAgICAgICAgICAgICAgY29sb3I6IFwiI2Q5ZjFmN1wiLFxuICAgICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzYyY2RlYVwiLFxuICAgICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdmVydGV4WydzdGFydC10aW1lJ10sXG4gICAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdmVydGV4WydlbmQtdGltZSddLFxuICAgICAgICAgICAgICAgICAgbGluazogdmVydGV4LmlkLFxuICAgICAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICBdXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKS5jbGljayhmdW5jdGlvbihkLCBpLCBkYXR1bSkge1xuICAgICAgICAgIGlmIChkLmxpbmspIHtcbiAgICAgICAgICAgIHJldHVybiAkc3RhdGUuZ28oXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLCB7XG4gICAgICAgICAgICAgIGpvYmlkOiBkYXRhLmppZCxcbiAgICAgICAgICAgICAgdmVydGV4SWQ6IGQubGlua1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuICAgICAgICB9KS50aWNrRm9ybWF0KHtcbiAgICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIiksXG4gICAgICAgICAgdGlja1NpemU6IDFcbiAgICAgICAgfSkucHJlZml4KFwibWFpblwiKS5tYXJnaW4oe1xuICAgICAgICAgIGxlZnQ6IDAsXG4gICAgICAgICAgcmlnaHQ6IDAsXG4gICAgICAgICAgdG9wOiAwLFxuICAgICAgICAgIGJvdHRvbTogMFxuICAgICAgICB9KS5pdGVtSGVpZ2h0KDMwKS5zaG93Qm9yZGVyTGluZSgpLnNob3dIb3VyVGltZWxpbmUoKTtcbiAgICAgICAgcmV0dXJuIHN2ZyA9IGQzLnNlbGVjdChzdmdFbCkuZGF0dW0odGVzdERhdGEpLmNhbGwoY2hhcnQpO1xuICAgICAgfTtcbiAgICAgIHNjb3BlLiR3YXRjaChhdHRycy5qb2IsIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgaWYgKGRhdGEpIHtcbiAgICAgICAgICByZXR1cm4gYW5hbHl6ZVRpbWUoZGF0YSk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH1cbiAgfTtcbn0pLmRpcmVjdGl2ZSgnam9iUGxhbicsIGZ1bmN0aW9uKCR0aW1lb3V0KSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0nZ3JhcGgnIHdpZHRoPSc1MDAnIGhlaWdodD0nNDAwJz48ZyAvPjwvc3ZnPiA8c3ZnIGNsYXNzPSd0bXAnIHdpZHRoPScxJyBoZWlnaHQ9JzEnPjxnIC8+PC9zdmc+IDxkaXYgY2xhc3M9J2J0bi1ncm91cCB6b29tLWJ1dHRvbnMnPiA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20taW4nIG5nLWNsaWNrPSd6b29tSW4oKSc+PGkgY2xhc3M9J2ZhIGZhLXBsdXMnIC8+PC9hPiA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20tb3V0JyBuZy1jbGljaz0nem9vbU91dCgpJz48aSBjbGFzcz0nZmEgZmEtbWludXMnIC8+PC9hPiA8L2Rpdj5cIixcbiAgICBzY29wZToge1xuICAgICAgcGxhbjogJz0nXG4gICAgfSxcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbSwgYXR0cnMpIHtcbiAgICAgIHZhciBjb250YWluZXJXLCBjcmVhdGVFZGdlLCBjcmVhdGVMYWJlbEVkZ2UsIGNyZWF0ZUxhYmVsTm9kZSwgY3JlYXRlTm9kZSwgZDNtYWluU3ZnLCBkM21haW5TdmdHLCBkM3RtcFN2ZywgZHJhd0dyYXBoLCBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24sIGdldE5vZGVUeXBlLCBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlLCBqb2JpZCwgbG9hZEpzb25Ub0RhZ3JlLCBtYWluRywgbWFpblN2Z0VsZW1lbnQsIG1haW5UbXBFbGVtZW50LCBtYWluWm9vbSwgc2VhcmNoRm9yTm9kZSwgc2hvcnRlblN0cmluZywgc3ViZ3JhcGhzO1xuICAgICAgbWFpblpvb20gPSBkMy5iZWhhdmlvci56b29tKCk7XG4gICAgICBzdWJncmFwaHMgPSBbXTtcbiAgICAgIGpvYmlkID0gYXR0cnMuam9iaWQ7XG4gICAgICBtYWluU3ZnRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVswXTtcbiAgICAgIG1haW5HID0gZWxlbS5jaGlsZHJlbigpLmNoaWxkcmVuKClbMF07XG4gICAgICBtYWluVG1wRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVsxXTtcbiAgICAgIGQzbWFpblN2ZyA9IGQzLnNlbGVjdChtYWluU3ZnRWxlbWVudCk7XG4gICAgICBkM21haW5TdmdHID0gZDMuc2VsZWN0KG1haW5HKTtcbiAgICAgIGQzdG1wU3ZnID0gZDMuc2VsZWN0KG1haW5UbXBFbGVtZW50KTtcbiAgICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKCk7XG4gICAgICBhbmd1bGFyLmVsZW1lbnQoZWxlbS5jaGlsZHJlbigpWzBdKS53aWR0aChjb250YWluZXJXKTtcbiAgICAgIHNjb3BlLnpvb21JbiA9IGZ1bmN0aW9uKCkge1xuICAgICAgICB2YXIgdHJhbnNsYXRlLCB2MSwgdjI7XG4gICAgICAgIGlmIChtYWluWm9vbS5zY2FsZSgpIDwgMi45OSkge1xuICAgICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpO1xuICAgICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIG1haW5ab29tLnNjYWxlKG1haW5ab29tLnNjYWxlKCkgKyAwLjEpO1xuICAgICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZShbdjEsIHYyXSk7XG4gICAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIik7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBzY29wZS56b29tT3V0ID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHZhciB0cmFuc2xhdGUsIHYxLCB2MjtcbiAgICAgICAgaWYgKG1haW5ab29tLnNjYWxlKCkgPiAwLjMxKSB7XG4gICAgICAgICAgbWFpblpvb20uc2NhbGUobWFpblpvb20uc2NhbGUoKSAtIDAuMSk7XG4gICAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKCk7XG4gICAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSk7XG4gICAgICAgICAgdjIgPSB0cmFuc2xhdGVbMV0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSk7XG4gICAgICAgICAgbWFpblpvb20udHJhbnNsYXRlKFt2MSwgdjJdKTtcbiAgICAgICAgICByZXR1cm4gZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGNyZWF0ZUxhYmVsRWRnZSA9IGZ1bmN0aW9uKGVsKSB7XG4gICAgICAgIHZhciBsYWJlbFZhbHVlO1xuICAgICAgICBsYWJlbFZhbHVlID0gXCJcIjtcbiAgICAgICAgaWYgKChlbC5zaGlwX3N0cmF0ZWd5ICE9IG51bGwpIHx8IChlbC5sb2NhbF9zdHJhdGVneSAhPSBudWxsKSkge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8ZGl2IGNsYXNzPSdlZGdlLWxhYmVsJz5cIjtcbiAgICAgICAgICBpZiAoZWwuc2hpcF9zdHJhdGVneSAhPSBudWxsKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IGVsLnNoaXBfc3RyYXRlZ3k7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC50ZW1wX21vZGUgIT09IHVuZGVmaW5lZCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIiAoXCIgKyBlbC50ZW1wX21vZGUgKyBcIilcIjtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLmxvY2FsX3N0cmF0ZWd5ICE9PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCIsPGJyPlwiICsgZWwubG9jYWxfc3RyYXRlZ3k7XG4gICAgICAgICAgfVxuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2Rpdj5cIjtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gbGFiZWxWYWx1ZTtcbiAgICAgIH07XG4gICAgICBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlID0gZnVuY3Rpb24oaW5mbykge1xuICAgICAgICByZXR1cm4gaW5mbyA9PT0gXCJwYXJ0aWFsU29sdXRpb25cIiB8fCBpbmZvID09PSBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiB8fCBpbmZvID09PSBcIndvcmtzZXRcIiB8fCBpbmZvID09PSBcIm5leHRXb3Jrc2V0XCIgfHwgaW5mbyA9PT0gXCJzb2x1dGlvblNldFwiIHx8IGluZm8gPT09IFwic29sdXRpb25EZWx0YVwiO1xuICAgICAgfTtcbiAgICAgIGdldE5vZGVUeXBlID0gZnVuY3Rpb24oZWwsIGluZm8pIHtcbiAgICAgICAgaWYgKGluZm8gPT09IFwibWlycm9yXCIpIHtcbiAgICAgICAgICByZXR1cm4gJ25vZGUtbWlycm9yJztcbiAgICAgICAgfSBlbHNlIGlmIChpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pKSB7XG4gICAgICAgICAgcmV0dXJuICdub2RlLWl0ZXJhdGlvbic7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuICdub2RlLW5vcm1hbCc7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBjcmVhdGVMYWJlbE5vZGUgPSBmdW5jdGlvbihlbCwgaW5mbywgbWF4VywgbWF4SCkge1xuICAgICAgICB2YXIgbGFiZWxWYWx1ZSwgc3RlcE5hbWU7XG4gICAgICAgIGxhYmVsVmFsdWUgPSBcIjxhIGhyZWY9JyMvam9icy9cIiArIGpvYmlkICsgXCIvdmVydGV4L1wiICsgZWwuaWQgKyBcIicgY2xhc3M9J25vZGUtbGFiZWwgXCIgKyBnZXROb2RlVHlwZShlbCwgaW5mbykgKyBcIic+XCI7XG4gICAgICAgIGlmIChpbmZvID09PSBcIm1pcnJvclwiKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoMyBjbGFzcz0nbm9kZS1uYW1lJz5NaXJyb3Igb2YgXCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIjtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPlwiICsgZWwub3BlcmF0b3IgKyBcIjwvaDM+XCI7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGVsLmRlc2NyaXB0aW9uID09PSBcIlwiKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIlwiO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHN0ZXBOYW1lID0gZWwuZGVzY3JpcHRpb247XG4gICAgICAgICAgc3RlcE5hbWUgPSBzaG9ydGVuU3RyaW5nKHN0ZXBOYW1lKTtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg0IGNsYXNzPSdzdGVwLW5hbWUnPlwiICsgc3RlcE5hbWUgKyBcIjwvaDQ+XCI7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb24gIT0gbnVsbCkge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uKGVsLmlkLCBtYXhXLCBtYXhIKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBpZiAoaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKSkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5cIiArIGluZm8gKyBcIiBOb2RlPC9oNT5cIjtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLnBhcmFsbGVsaXNtICE9PSBcIlwiKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlBhcmFsbGVsaXNtOiBcIiArIGVsLnBhcmFsbGVsaXNtICsgXCI8L2g1PlwiO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwub3BlcmF0b3IgIT09IHVuZGVmaW5lZCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5PcGVyYXRpb246IFwiICsgc2hvcnRlblN0cmluZyhlbC5vcGVyYXRvcl9zdHJhdGVneSkgKyBcIjwvaDU+XCI7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2E+XCI7XG4gICAgICAgIHJldHVybiBsYWJlbFZhbHVlO1xuICAgICAgfTtcbiAgICAgIGV4dGVuZExhYmVsTm9kZUZvckl0ZXJhdGlvbiA9IGZ1bmN0aW9uKGlkLCBtYXhXLCBtYXhIKSB7XG4gICAgICAgIHZhciBsYWJlbFZhbHVlLCBzdmdJRDtcbiAgICAgICAgc3ZnSUQgPSBcInN2Zy1cIiArIGlkO1xuICAgICAgICBsYWJlbFZhbHVlID0gXCI8c3ZnIGNsYXNzPSdcIiArIHN2Z0lEICsgXCInIHdpZHRoPVwiICsgbWF4VyArIFwiIGhlaWdodD1cIiArIG1heEggKyBcIj48ZyAvPjwvc3ZnPlwiO1xuICAgICAgICByZXR1cm4gbGFiZWxWYWx1ZTtcbiAgICAgIH07XG4gICAgICBzaG9ydGVuU3RyaW5nID0gZnVuY3Rpb24ocykge1xuICAgICAgICB2YXIgc2JyO1xuICAgICAgICBpZiAocy5jaGFyQXQoMCkgPT09IFwiPFwiKSB7XG4gICAgICAgICAgcyA9IHMucmVwbGFjZShcIjxcIiwgXCImbHQ7XCIpO1xuICAgICAgICAgIHMgPSBzLnJlcGxhY2UoXCI+XCIsIFwiJmd0O1wiKTtcbiAgICAgICAgfVxuICAgICAgICBzYnIgPSBcIlwiO1xuICAgICAgICB3aGlsZSAocy5sZW5ndGggPiAzMCkge1xuICAgICAgICAgIHNiciA9IHNiciArIHMuc3Vic3RyaW5nKDAsIDMwKSArIFwiPGJyPlwiO1xuICAgICAgICAgIHMgPSBzLnN1YnN0cmluZygzMCwgcy5sZW5ndGgpO1xuICAgICAgICB9XG4gICAgICAgIHNiciA9IHNiciArIHM7XG4gICAgICAgIHJldHVybiBzYnI7XG4gICAgICB9O1xuICAgICAgY3JlYXRlTm9kZSA9IGZ1bmN0aW9uKGcsIGRhdGEsIGVsLCBpc1BhcmVudCwgbWF4VywgbWF4SCkge1xuICAgICAgICBpZiAoaXNQYXJlbnQgPT0gbnVsbCkge1xuICAgICAgICAgIGlzUGFyZW50ID0gZmFsc2U7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGVsLmlkID09PSBkYXRhLnBhcnRpYWxfc29sdXRpb24pIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5uZXh0X3BhcnRpYWxfc29sdXRpb24pIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLndvcmtzZXQpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIndvcmtzZXRcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwid29ya3NldFwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLm5leHRfd29ya3NldCkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFdvcmtzZXRcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwibmV4dFdvcmtzZXRcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5zb2x1dGlvbl9zZXQpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uU2V0XCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcInNvbHV0aW9uU2V0XCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEuc29sdXRpb25fZGVsdGEpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uRGVsdGFcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25EZWx0YVwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwiXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIlwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgY3JlYXRlRWRnZSA9IGZ1bmN0aW9uKGcsIGRhdGEsIGVsLCBleGlzdGluZ05vZGVzLCBwcmVkKSB7XG4gICAgICAgIHZhciBtaXNzaW5nTm9kZTtcbiAgICAgICAgaWYgKGV4aXN0aW5nTm9kZXMuaW5kZXhPZihwcmVkLmlkKSAhPT0gLTEpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXRFZGdlKHByZWQuaWQsIGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxFZGdlKHByZWQpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBhcnJvd2hlYWQ6ICdub3JtYWwnXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgbWlzc2luZ05vZGUgPSBzZWFyY2hGb3JOb2RlKGRhdGEsIHByZWQuaWQpO1xuICAgICAgICAgIGlmICghKCFtaXNzaW5nTm9kZSB8fCBtaXNzaW5nTm9kZS5hbHJlYWR5QWRkZWQgPT09IHRydWUpKSB7XG4gICAgICAgICAgICBtaXNzaW5nTm9kZS5hbHJlYWR5QWRkZWQgPSB0cnVlO1xuICAgICAgICAgICAgZy5zZXROb2RlKG1pc3NpbmdOb2RlLmlkLCB7XG4gICAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUobWlzc2luZ05vZGUsIFwibWlycm9yXCIpLFxuICAgICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShtaXNzaW5nTm9kZSwgJ21pcnJvcicpXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIHJldHVybiBnLnNldEVkZ2UobWlzc2luZ05vZGUuaWQsIGVsLmlkLCB7XG4gICAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UobWlzc2luZ05vZGUpLFxuICAgICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgbG9hZEpzb25Ub0RhZ3JlID0gZnVuY3Rpb24oZywgZGF0YSkge1xuICAgICAgICB2YXIgZWwsIGV4aXN0aW5nTm9kZXMsIGlzUGFyZW50LCBrLCBsLCBsZW4sIGxlbjEsIG1heEgsIG1heFcsIHByZWQsIHIsIHJlZiwgc2csIHRvSXRlcmF0ZTtcbiAgICAgICAgZXhpc3RpbmdOb2RlcyA9IFtdO1xuICAgICAgICBpZiAoZGF0YS5ub2RlcyAhPSBudWxsKSB7XG4gICAgICAgICAgdG9JdGVyYXRlID0gZGF0YS5ub2RlcztcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLnN0ZXBfZnVuY3Rpb247XG4gICAgICAgICAgaXNQYXJlbnQgPSB0cnVlO1xuICAgICAgICB9XG4gICAgICAgIGZvciAoayA9IDAsIGxlbiA9IHRvSXRlcmF0ZS5sZW5ndGg7IGsgPCBsZW47IGsrKykge1xuICAgICAgICAgIGVsID0gdG9JdGVyYXRlW2tdO1xuICAgICAgICAgIG1heFcgPSAwO1xuICAgICAgICAgIG1heEggPSAwO1xuICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uKSB7XG4gICAgICAgICAgICBzZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHtcbiAgICAgICAgICAgICAgbXVsdGlncmFwaDogdHJ1ZSxcbiAgICAgICAgICAgICAgY29tcG91bmQ6IHRydWVcbiAgICAgICAgICAgIH0pLnNldEdyYXBoKHtcbiAgICAgICAgICAgICAgbm9kZXNlcDogMjAsXG4gICAgICAgICAgICAgIGVkZ2VzZXA6IDAsXG4gICAgICAgICAgICAgIHJhbmtzZXA6IDIwLFxuICAgICAgICAgICAgICByYW5rZGlyOiBcIkxSXCIsXG4gICAgICAgICAgICAgIG1hcmdpbng6IDEwLFxuICAgICAgICAgICAgICBtYXJnaW55OiAxMFxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICBzdWJncmFwaHNbZWwuaWRdID0gc2c7XG4gICAgICAgICAgICBsb2FkSnNvblRvRGFncmUoc2csIGVsKTtcbiAgICAgICAgICAgIHIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKTtcbiAgICAgICAgICAgIGQzdG1wU3ZnLnNlbGVjdCgnZycpLmNhbGwociwgc2cpO1xuICAgICAgICAgICAgbWF4VyA9IHNnLmdyYXBoKCkud2lkdGg7XG4gICAgICAgICAgICBtYXhIID0gc2cuZ3JhcGgoKS5oZWlnaHQ7XG4gICAgICAgICAgICBhbmd1bGFyLmVsZW1lbnQobWFpblRtcEVsZW1lbnQpLmVtcHR5KCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGNyZWF0ZU5vZGUoZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKTtcbiAgICAgICAgICBleGlzdGluZ05vZGVzLnB1c2goZWwuaWQpO1xuICAgICAgICAgIGlmIChlbC5pbnB1dHMgIT0gbnVsbCkge1xuICAgICAgICAgICAgcmVmID0gZWwuaW5wdXRzO1xuICAgICAgICAgICAgZm9yIChsID0gMCwgbGVuMSA9IHJlZi5sZW5ndGg7IGwgPCBsZW4xOyBsKyspIHtcbiAgICAgICAgICAgICAgcHJlZCA9IHJlZltsXTtcbiAgICAgICAgICAgICAgY3JlYXRlRWRnZShnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZCk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgIHJldHVybiBnO1xuICAgICAgfTtcbiAgICAgIHNlYXJjaEZvck5vZGUgPSBmdW5jdGlvbihkYXRhLCBub2RlSUQpIHtcbiAgICAgICAgdmFyIGVsLCBpLCBqO1xuICAgICAgICBmb3IgKGkgaW4gZGF0YS5ub2Rlcykge1xuICAgICAgICAgIGVsID0gZGF0YS5ub2Rlc1tpXTtcbiAgICAgICAgICBpZiAoZWwuaWQgPT09IG5vZGVJRCkge1xuICAgICAgICAgICAgcmV0dXJuIGVsO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbiAhPSBudWxsKSB7XG4gICAgICAgICAgICBmb3IgKGogaW4gZWwuc3RlcF9mdW5jdGlvbikge1xuICAgICAgICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbltqXS5pZCA9PT0gbm9kZUlEKSB7XG4gICAgICAgICAgICAgICAgcmV0dXJuIGVsLnN0ZXBfZnVuY3Rpb25bal07XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBkcmF3R3JhcGggPSBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciBnLCBpLCBuZXdTY2FsZSwgcmVuZGVyZXIsIHNnLCB4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0O1xuICAgICAgICBnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoe1xuICAgICAgICAgIG11bHRpZ3JhcGg6IHRydWUsXG4gICAgICAgICAgY29tcG91bmQ6IHRydWVcbiAgICAgICAgfSkuc2V0R3JhcGgoe1xuICAgICAgICAgIG5vZGVzZXA6IDcwLFxuICAgICAgICAgIGVkZ2VzZXA6IDAsXG4gICAgICAgICAgcmFua3NlcDogNTAsXG4gICAgICAgICAgcmFua2RpcjogXCJMUlwiLFxuICAgICAgICAgIG1hcmdpbng6IDQwLFxuICAgICAgICAgIG1hcmdpbnk6IDQwXG4gICAgICAgIH0pO1xuICAgICAgICBsb2FkSnNvblRvRGFncmUoZywgZGF0YSk7XG4gICAgICAgIHJlbmRlcmVyID0gbmV3IGRhZ3JlRDMucmVuZGVyKCk7XG4gICAgICAgIGQzbWFpblN2Z0cuY2FsbChyZW5kZXJlciwgZyk7XG4gICAgICAgIGZvciAoaSBpbiBzdWJncmFwaHMpIHtcbiAgICAgICAgICBzZyA9IHN1YmdyYXBoc1tpXTtcbiAgICAgICAgICBkM21haW5Tdmcuc2VsZWN0KCdzdmcuc3ZnLScgKyBpICsgJyBnJykuY2FsbChyZW5kZXJlciwgc2cpO1xuICAgICAgICB9XG4gICAgICAgIG5ld1NjYWxlID0gMC41O1xuICAgICAgICB4Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS53aWR0aCgpIC0gZy5ncmFwaCgpLndpZHRoICogbmV3U2NhbGUpIC8gMik7XG4gICAgICAgIHlDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLmhlaWdodCgpIC0gZy5ncmFwaCgpLmhlaWdodCAqIG5ld1NjYWxlKSAvIDIpO1xuICAgICAgICBtYWluWm9vbS5zY2FsZShuZXdTY2FsZSkudHJhbnNsYXRlKFt4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0XSk7XG4gICAgICAgIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHhDZW50ZXJPZmZzZXQgKyBcIiwgXCIgKyB5Q2VudGVyT2Zmc2V0ICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKTtcbiAgICAgICAgbWFpblpvb20ub24oXCJ6b29tXCIsIGZ1bmN0aW9uKCkge1xuICAgICAgICAgIHZhciBldjtcbiAgICAgICAgICBldiA9IGQzLmV2ZW50O1xuICAgICAgICAgIHJldHVybiBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyBldi50cmFuc2xhdGUgKyBcIikgc2NhbGUoXCIgKyBldi5zY2FsZSArIFwiKVwiKTtcbiAgICAgICAgfSk7XG4gICAgICAgIHJldHVybiBtYWluWm9vbShkM21haW5TdmcpO1xuICAgICAgfTtcbiAgICAgIHNjb3BlLiR3YXRjaChhdHRycy5wbGFuLCBmdW5jdGlvbihuZXdQbGFuKSB7XG4gICAgICAgIGlmIChuZXdQbGFuKSB7XG4gICAgICAgICAgcmV0dXJuIGRyYXdHcmFwaChuZXdQbGFuKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuICB9O1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLnNlcnZpY2UgJ0pvYnNTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJGxvZywgYW1Nb21lbnQsICRxLCAkdGltZW91dCkgLT5cbiAgY3VycmVudEpvYiA9IG51bGxcbiAgY3VycmVudFBsYW4gPSBudWxsXG4gIGRlZmVycmVkcyA9IHt9XG4gIGpvYnMgPSB7XG4gICAgcnVubmluZzogW11cbiAgICBmaW5pc2hlZDogW11cbiAgICBjYW5jZWxsZWQ6IFtdXG4gICAgZmFpbGVkOiBbXVxuICB9XG5cbiAgam9iT2JzZXJ2ZXJzID0gW11cblxuICBub3RpZnlPYnNlcnZlcnMgPSAtPlxuICAgIGFuZ3VsYXIuZm9yRWFjaCBqb2JPYnNlcnZlcnMsIChjYWxsYmFjaykgLT5cbiAgICAgIGNhbGxiYWNrKClcblxuICBAcmVnaXN0ZXJPYnNlcnZlciA9IChjYWxsYmFjaykgLT5cbiAgICBqb2JPYnNlcnZlcnMucHVzaChjYWxsYmFjaylcblxuICBAdW5SZWdpc3Rlck9ic2VydmVyID0gKGNhbGxiYWNrKSAtPlxuICAgIGluZGV4ID0gam9iT2JzZXJ2ZXJzLmluZGV4T2YoY2FsbGJhY2spXG4gICAgam9iT2JzZXJ2ZXJzLnNwbGljZShpbmRleCwgMSlcblxuICBAc3RhdGVMaXN0ID0gLT5cbiAgICBbIFxuICAgICAgIyAnQ1JFQVRFRCdcbiAgICAgICdTQ0hFRFVMRUQnXG4gICAgICAnREVQTE9ZSU5HJ1xuICAgICAgJ1JVTk5JTkcnXG4gICAgICAnRklOSVNIRUQnXG4gICAgICAnRkFJTEVEJ1xuICAgICAgJ0NBTkNFTElORydcbiAgICAgICdDQU5DRUxFRCdcbiAgICBdXG5cbiAgQHRyYW5zbGF0ZUxhYmVsU3RhdGUgPSAoc3RhdGUpIC0+XG4gICAgc3dpdGNoIHN0YXRlLnRvTG93ZXJDYXNlKClcbiAgICAgIHdoZW4gJ2ZpbmlzaGVkJyB0aGVuICdzdWNjZXNzJ1xuICAgICAgd2hlbiAnZmFpbGVkJyB0aGVuICdkYW5nZXInXG4gICAgICB3aGVuICdzY2hlZHVsZWQnIHRoZW4gJ2RlZmF1bHQnXG4gICAgICB3aGVuICdkZXBsb3lpbmcnIHRoZW4gJ2luZm8nXG4gICAgICB3aGVuICdydW5uaW5nJyB0aGVuICdwcmltYXJ5J1xuICAgICAgd2hlbiAnY2FuY2VsaW5nJyB0aGVuICd3YXJuaW5nJ1xuICAgICAgd2hlbiAncGVuZGluZycgdGhlbiAnaW5mbydcbiAgICAgIHdoZW4gJ3RvdGFsJyB0aGVuICdibGFjaydcbiAgICAgIGVsc2UgJ2RlZmF1bHQnXG5cbiAgQHNldEVuZFRpbWVzID0gKGxpc3QpIC0+XG4gICAgYW5ndWxhci5mb3JFYWNoIGxpc3QsIChqb2IsIGpvYktleSkgLT5cbiAgICAgIHVubGVzcyBqb2JbJ2VuZC10aW1lJ10gPiAtMVxuICAgICAgICBqb2JbJ2VuZC10aW1lJ10gPSBqb2JbJ3N0YXJ0LXRpbWUnXSArIGpvYlsnZHVyYXRpb24nXVxuXG4gIEBsaXN0Sm9icyA9IC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9ib3ZlcnZpZXdcIlxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgPT5cbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLCAobGlzdCwgbGlzdEtleSkgPT5cbiAgICAgICAgc3dpdGNoIGxpc3RLZXlcbiAgICAgICAgICB3aGVuICdydW5uaW5nJyB0aGVuIGpvYnMucnVubmluZyA9IEBzZXRFbmRUaW1lcyhsaXN0KVxuICAgICAgICAgIHdoZW4gJ2ZpbmlzaGVkJyB0aGVuIGpvYnMuZmluaXNoZWQgPSBAc2V0RW5kVGltZXMobGlzdClcbiAgICAgICAgICB3aGVuICdjYW5jZWxsZWQnIHRoZW4gam9icy5jYW5jZWxsZWQgPSBAc2V0RW5kVGltZXMobGlzdClcbiAgICAgICAgICB3aGVuICdmYWlsZWQnIHRoZW4gam9icy5mYWlsZWQgPSBAc2V0RW5kVGltZXMobGlzdClcblxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShqb2JzKVxuICAgICAgbm90aWZ5T2JzZXJ2ZXJzKClcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAZ2V0Sm9icyA9ICh0eXBlKSAtPlxuICAgIGpvYnNbdHlwZV1cblxuICBAZ2V0QWxsSm9icyA9IC0+XG4gICAgam9ic1xuXG4gIEBsb2FkSm9iID0gKGpvYmlkKSAtPlxuICAgIGN1cnJlbnRKb2IgPSBudWxsXG4gICAgZGVmZXJyZWRzLmpvYiA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9qb2JzL1wiICsgam9iaWRcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpID0+XG4gICAgICAjIGNvbnNvbGUubG9nIGRhdGEudmVydGljZXNcbiAgICAgIEBzZXRFbmRUaW1lcyhkYXRhLnZlcnRpY2VzKVxuICAgICAgIyBjb25zb2xlLmxvZyBkYXRhLnZlcnRpY2VzXG5cbiAgICAgICMgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRpY2VzXCJcbiAgICAgICMgLnN1Y2Nlc3MgKHZlcnRpY2VzKSAtPlxuICAgICAgIyAgIGRhdGEgPSBhbmd1bGFyLmV4dGVuZChkYXRhLCB2ZXJ0aWNlcylcblxuICAgICAgICAjICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9qb2JzSW5mbz9nZXQ9am9iJmpvYj1cIiArIGpvYmlkXG4gICAgICAgICMgLnN1Y2Nlc3MgKG9sZFZlcnRpY2VzKSAtPlxuICAgICAgICAjICAgZGF0YS5vbGRWID0gb2xkVmVydGljZXNbMF1cblxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZCArIFwiL2NvbmZpZ1wiXG4gICAgICAuc3VjY2VzcyAoam9iQ29uZmlnKSAtPlxuICAgICAgICBkYXRhID0gYW5ndWxhci5leHRlbmQoZGF0YSwgam9iQ29uZmlnKVxuXG4gICAgICAgIGN1cnJlbnRKb2IgPSBkYXRhXG5cbiAgICAgICAgZGVmZXJyZWRzLmpvYi5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2VcblxuICBAbG9hZFBsYW4gPSAoam9iaWQpIC0+XG4gICAgY3VycmVudFBsYW4gPSBudWxsXG4gICAgZGVmZXJyZWRzLnBsYW4gPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGpvYmlkICsgXCIvcGxhblwiXG4gICAgLnN1Y2Nlc3MgKGRhdGEpIC0+XG4gICAgICBjdXJyZW50UGxhbiA9IGRhdGFcblxuICAgICAgZGVmZXJyZWRzLnBsYW4ucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWRzLnBsYW4ucHJvbWlzZVxuXG4gIEBnZXROb2RlID0gKG5vZGVpZCkgLT5cbiAgICBzZWVrTm9kZSA9IChub2RlaWQsIGRhdGEpIC0+XG4gICAgICBmb3Igbm9kZSBpbiBkYXRhXG4gICAgICAgIHJldHVybiBub2RlIGlmIG5vZGUuaWQgaXMgbm9kZWlkXG4gICAgICAgIHN1YiA9IHNlZWtOb2RlKG5vZGVpZCwgbm9kZS5zdGVwX2Z1bmN0aW9uKSBpZiBub2RlLnN0ZXBfZnVuY3Rpb25cbiAgICAgICAgcmV0dXJuIHN1YiBpZiBzdWJcblxuICAgICAgbnVsbFxuXG5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRxLmFsbChbZGVmZXJyZWRzLnBsYW4ucHJvbWlzZSwgZGVmZXJyZWRzLmpvYi5wcm9taXNlXSkudGhlbiAoZGF0YSkgPT5cbiAgICAgIGZvdW5kTm9kZSA9IHNlZWtOb2RlKG5vZGVpZCwgY3VycmVudFBsYW4ubm9kZXMpXG5cbiAgICAgICMgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnNJbmZvP2dldD1ncm91cHZlcnRleCZqb2I9XCIgKyBjdXJyZW50Sm9iLmppZCArIFwiJmdyb3VwdmVydGV4PVwiICsgbm9kZWlkXG5cbiAgICAgIGZvdW5kTm9kZS52ZXJ0ZXggPSBAc2Vla1ZlcnRleChub2RlaWQpXG5cbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZm91bmROb2RlKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBzZWVrVmVydGV4ID0gKG5vZGVpZCkgLT5cbiAgICBmb3IgdmVydGV4IGluIGN1cnJlbnRKb2IudmVydGljZXNcbiAgICAgIHJldHVybiB2ZXJ0ZXggaWYgdmVydGV4LmlkIGlzIG5vZGVpZFxuXG4gICAgcmV0dXJuIG51bGxcblxuICBAZ2V0VmVydGV4ID0gKHZlcnRleGlkKSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJHEuYWxsKFtkZWZlcnJlZHMuam9iLnByb21pc2VdKS50aGVuIChkYXRhKSA9PlxuICAgICAgdmVydGV4ID0gQHNlZWtWZXJ0ZXgodmVydGV4aWQpXG5cbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9qb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvc3VidGFza3RpbWVzXCJcbiAgICAgIC5zdWNjZXNzIChkYXRhKSAtPlxuICAgICAgICB2ZXJ0ZXguc3VidGFza3MgPSBkYXRhLnN1YnRhc2tzXG5cbiAgICAgICAgZGVmZXJyZWQucmVzb2x2ZSh2ZXJ0ZXgpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQGxvYWRFeGNlcHRpb25zID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRxLmFsbChbZGVmZXJyZWRzLmpvYi5wcm9taXNlXSkudGhlbiAoZGF0YSkgPT5cblxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2V4Y2VwdGlvbnNcIlxuICAgICAgLnN1Y2Nlc3MgKGV4Y2VwdGlvbnMpIC0+XG4gICAgICAgIGN1cnJlbnRKb2IuZXhjZXB0aW9ucyA9IGV4Y2VwdGlvbnNcblxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGV4Y2VwdGlvbnMpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQFxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnSm9ic1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRsb2csIGFtTW9tZW50LCAkcSwgJHRpbWVvdXQpIHtcbiAgdmFyIGN1cnJlbnRKb2IsIGN1cnJlbnRQbGFuLCBkZWZlcnJlZHMsIGpvYk9ic2VydmVycywgam9icywgbm90aWZ5T2JzZXJ2ZXJzO1xuICBjdXJyZW50Sm9iID0gbnVsbDtcbiAgY3VycmVudFBsYW4gPSBudWxsO1xuICBkZWZlcnJlZHMgPSB7fTtcbiAgam9icyA9IHtcbiAgICBydW5uaW5nOiBbXSxcbiAgICBmaW5pc2hlZDogW10sXG4gICAgY2FuY2VsbGVkOiBbXSxcbiAgICBmYWlsZWQ6IFtdXG4gIH07XG4gIGpvYk9ic2VydmVycyA9IFtdO1xuICBub3RpZnlPYnNlcnZlcnMgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gYW5ndWxhci5mb3JFYWNoKGpvYk9ic2VydmVycywgZnVuY3Rpb24oY2FsbGJhY2spIHtcbiAgICAgIHJldHVybiBjYWxsYmFjaygpO1xuICAgIH0pO1xuICB9O1xuICB0aGlzLnJlZ2lzdGVyT2JzZXJ2ZXIgPSBmdW5jdGlvbihjYWxsYmFjaykge1xuICAgIHJldHVybiBqb2JPYnNlcnZlcnMucHVzaChjYWxsYmFjayk7XG4gIH07XG4gIHRoaXMudW5SZWdpc3Rlck9ic2VydmVyID0gZnVuY3Rpb24oY2FsbGJhY2spIHtcbiAgICB2YXIgaW5kZXg7XG4gICAgaW5kZXggPSBqb2JPYnNlcnZlcnMuaW5kZXhPZihjYWxsYmFjayk7XG4gICAgcmV0dXJuIGpvYk9ic2VydmVycy5zcGxpY2UoaW5kZXgsIDEpO1xuICB9O1xuICB0aGlzLnN0YXRlTGlzdCA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBbJ1NDSEVEVUxFRCcsICdERVBMT1lJTkcnLCAnUlVOTklORycsICdGSU5JU0hFRCcsICdGQUlMRUQnLCAnQ0FOQ0VMSU5HJywgJ0NBTkNFTEVEJ107XG4gIH07XG4gIHRoaXMudHJhbnNsYXRlTGFiZWxTdGF0ZSA9IGZ1bmN0aW9uKHN0YXRlKSB7XG4gICAgc3dpdGNoIChzdGF0ZS50b0xvd2VyQ2FzZSgpKSB7XG4gICAgICBjYXNlICdmaW5pc2hlZCc6XG4gICAgICAgIHJldHVybiAnc3VjY2Vzcyc7XG4gICAgICBjYXNlICdmYWlsZWQnOlxuICAgICAgICByZXR1cm4gJ2Rhbmdlcic7XG4gICAgICBjYXNlICdzY2hlZHVsZWQnOlxuICAgICAgICByZXR1cm4gJ2RlZmF1bHQnO1xuICAgICAgY2FzZSAnZGVwbG95aW5nJzpcbiAgICAgICAgcmV0dXJuICdpbmZvJztcbiAgICAgIGNhc2UgJ3J1bm5pbmcnOlxuICAgICAgICByZXR1cm4gJ3ByaW1hcnknO1xuICAgICAgY2FzZSAnY2FuY2VsaW5nJzpcbiAgICAgICAgcmV0dXJuICd3YXJuaW5nJztcbiAgICAgIGNhc2UgJ3BlbmRpbmcnOlxuICAgICAgICByZXR1cm4gJ2luZm8nO1xuICAgICAgY2FzZSAndG90YWwnOlxuICAgICAgICByZXR1cm4gJ2JsYWNrJztcbiAgICAgIGRlZmF1bHQ6XG4gICAgICAgIHJldHVybiAnZGVmYXVsdCc7XG4gICAgfVxuICB9O1xuICB0aGlzLnNldEVuZFRpbWVzID0gZnVuY3Rpb24obGlzdCkge1xuICAgIHJldHVybiBhbmd1bGFyLmZvckVhY2gobGlzdCwgZnVuY3Rpb24oam9iLCBqb2JLZXkpIHtcbiAgICAgIGlmICghKGpvYlsnZW5kLXRpbWUnXSA+IC0xKSkge1xuICAgICAgICByZXR1cm4gam9iWydlbmQtdGltZSddID0gam9iWydzdGFydC10aW1lJ10gKyBqb2JbJ2R1cmF0aW9uJ107XG4gICAgICB9XG4gICAgfSk7XG4gIH07XG4gIHRoaXMubGlzdEpvYnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9qb2JvdmVydmlld1wiKS5zdWNjZXNzKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLCBmdW5jdGlvbihsaXN0LCBsaXN0S2V5KSB7XG4gICAgICAgICAgc3dpdGNoIChsaXN0S2V5KSB7XG4gICAgICAgICAgICBjYXNlICdydW5uaW5nJzpcbiAgICAgICAgICAgICAgcmV0dXJuIGpvYnMucnVubmluZyA9IF90aGlzLnNldEVuZFRpbWVzKGxpc3QpO1xuICAgICAgICAgICAgY2FzZSAnZmluaXNoZWQnOlxuICAgICAgICAgICAgICByZXR1cm4gam9icy5maW5pc2hlZCA9IF90aGlzLnNldEVuZFRpbWVzKGxpc3QpO1xuICAgICAgICAgICAgY2FzZSAnY2FuY2VsbGVkJzpcbiAgICAgICAgICAgICAgcmV0dXJuIGpvYnMuY2FuY2VsbGVkID0gX3RoaXMuc2V0RW5kVGltZXMobGlzdCk7XG4gICAgICAgICAgICBjYXNlICdmYWlsZWQnOlxuICAgICAgICAgICAgICByZXR1cm4gam9icy5mYWlsZWQgPSBfdGhpcy5zZXRFbmRUaW1lcyhsaXN0KTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGpvYnMpO1xuICAgICAgICByZXR1cm4gbm90aWZ5T2JzZXJ2ZXJzKCk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRKb2JzID0gZnVuY3Rpb24odHlwZSkge1xuICAgIHJldHVybiBqb2JzW3R5cGVdO1xuICB9O1xuICB0aGlzLmdldEFsbEpvYnMgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gam9icztcbiAgfTtcbiAgdGhpcy5sb2FkSm9iID0gZnVuY3Rpb24oam9iaWQpIHtcbiAgICBjdXJyZW50Sm9iID0gbnVsbDtcbiAgICBkZWZlcnJlZHMuam9iID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGpvYmlkKS5zdWNjZXNzKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICAgIF90aGlzLnNldEVuZFRpbWVzKGRhdGEudmVydGljZXMpO1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZCArIFwiL2NvbmZpZ1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGpvYkNvbmZpZykge1xuICAgICAgICAgIGRhdGEgPSBhbmd1bGFyLmV4dGVuZChkYXRhLCBqb2JDb25maWcpO1xuICAgICAgICAgIGN1cnJlbnRKb2IgPSBkYXRhO1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZHMuam9iLnJlc29sdmUoZGF0YSk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkcy5qb2IucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5sb2FkUGxhbiA9IGZ1bmN0aW9uKGpvYmlkKSB7XG4gICAgY3VycmVudFBsYW4gPSBudWxsO1xuICAgIGRlZmVycmVkcy5wbGFuID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGpvYmlkICsgXCIvcGxhblwiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIGN1cnJlbnRQbGFuID0gZGF0YTtcbiAgICAgIHJldHVybiBkZWZlcnJlZHMucGxhbi5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZHMucGxhbi5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldE5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICB2YXIgZGVmZXJyZWQsIHNlZWtOb2RlO1xuICAgIHNlZWtOb2RlID0gZnVuY3Rpb24obm9kZWlkLCBkYXRhKSB7XG4gICAgICB2YXIgaSwgbGVuLCBub2RlLCBzdWI7XG4gICAgICBmb3IgKGkgPSAwLCBsZW4gPSBkYXRhLmxlbmd0aDsgaSA8IGxlbjsgaSsrKSB7XG4gICAgICAgIG5vZGUgPSBkYXRhW2ldO1xuICAgICAgICBpZiAobm9kZS5pZCA9PT0gbm9kZWlkKSB7XG4gICAgICAgICAgcmV0dXJuIG5vZGU7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKG5vZGUuc3RlcF9mdW5jdGlvbikge1xuICAgICAgICAgIHN1YiA9IHNlZWtOb2RlKG5vZGVpZCwgbm9kZS5zdGVwX2Z1bmN0aW9uKTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoc3ViKSB7XG4gICAgICAgICAgcmV0dXJuIHN1YjtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfTtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJHEuYWxsKFtkZWZlcnJlZHMucGxhbi5wcm9taXNlLCBkZWZlcnJlZHMuam9iLnByb21pc2VdKS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGZvdW5kTm9kZTtcbiAgICAgICAgZm91bmROb2RlID0gc2Vla05vZGUobm9kZWlkLCBjdXJyZW50UGxhbi5ub2Rlcyk7XG4gICAgICAgIGZvdW5kTm9kZS52ZXJ0ZXggPSBfdGhpcy5zZWVrVmVydGV4KG5vZGVpZCk7XG4gICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGZvdW5kTm9kZSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5zZWVrVmVydGV4ID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgdmFyIGksIGxlbiwgcmVmLCB2ZXJ0ZXg7XG4gICAgcmVmID0gY3VycmVudEpvYi52ZXJ0aWNlcztcbiAgICBmb3IgKGkgPSAwLCBsZW4gPSByZWYubGVuZ3RoOyBpIDwgbGVuOyBpKyspIHtcbiAgICAgIHZlcnRleCA9IHJlZltpXTtcbiAgICAgIGlmICh2ZXJ0ZXguaWQgPT09IG5vZGVpZCkge1xuICAgICAgICByZXR1cm4gdmVydGV4O1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfTtcbiAgdGhpcy5nZXRWZXJ0ZXggPSBmdW5jdGlvbih2ZXJ0ZXhpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJHEuYWxsKFtkZWZlcnJlZHMuam9iLnByb21pc2VdKS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIHZlcnRleDtcbiAgICAgICAgdmVydGV4ID0gX3RoaXMuc2Vla1ZlcnRleCh2ZXJ0ZXhpZCk7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL3N1YnRhc2t0aW1lc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICB2ZXJ0ZXguc3VidGFza3MgPSBkYXRhLnN1YnRhc2tzO1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKHZlcnRleCk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMubG9hZEV4Y2VwdGlvbnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRxLmFsbChbZGVmZXJyZWRzLmpvYi5wcm9taXNlXSkudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvZXhjZXB0aW9uc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGV4Y2VwdGlvbnMpIHtcbiAgICAgICAgICBjdXJyZW50Sm9iLmV4Y2VwdGlvbnMgPSBleGNlcHRpb25zO1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGV4Y2VwdGlvbnMpO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5jb250cm9sbGVyICdPdmVydmlld0NvbnRyb2xsZXInLCAoJHNjb3BlLCBPdmVydmlld1NlcnZpY2UsIEpvYnNTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSAtPlxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxuICAgICRzY29wZS5ydW5uaW5nSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKVxuICAgICRzY29wZS5maW5pc2hlZEpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpXG5cbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuXG4gICRzY29wZS5qb2JPYnNlcnZlcigpXG5cbiAgT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLm92ZXJ2aWV3ID0gZGF0YVxuXG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwgLT5cbiAgICBPdmVydmlld1NlcnZpY2UubG9hZE92ZXJ2aWV3KCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5vdmVydmlldyA9IGRhdGFcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cblxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignT3ZlcnZpZXdDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBPdmVydmlld1NlcnZpY2UsIEpvYnNTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSB7XG4gIHZhciByZWZyZXNoO1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSBmdW5jdGlvbigpIHtcbiAgICAkc2NvcGUucnVubmluZ0pvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJyk7XG4gICAgcmV0dXJuICRzY29wZS5maW5pc2hlZEpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpO1xuICB9O1xuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICB9KTtcbiAgJHNjb3BlLmpvYk9ic2VydmVyKCk7XG4gIE92ZXJ2aWV3U2VydmljZS5sb2FkT3ZlcnZpZXcoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLm92ZXJ2aWV3ID0gZGF0YTtcbiAgfSk7XG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIE92ZXJ2aWV3U2VydmljZS5sb2FkT3ZlcnZpZXcoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhO1xuICAgIH0pO1xuICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICByZXR1cm4gJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJGludGVydmFsLmNhbmNlbChyZWZyZXNoKTtcbiAgfSk7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnT3ZlcnZpZXdTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG4gIG92ZXJ2aWV3ID0ge31cblxuICBAbG9hZE92ZXJ2aWV3ID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9vdmVydmlld1wiKVxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIG92ZXJ2aWV3ID0gZGF0YVxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ092ZXJ2aWV3U2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdmFyIG92ZXJ2aWV3O1xuICBvdmVydmlldyA9IHt9O1xuICB0aGlzLmxvYWRPdmVydmlldyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL292ZXJ2aWV3XCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIG92ZXJ2aWV3ID0gZGF0YTtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIl0sInNvdXJjZVJvb3QiOiIvc291cmNlLyJ9