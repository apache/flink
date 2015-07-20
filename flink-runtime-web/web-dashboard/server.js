var Hapi = require('hapi');

var server = new Hapi.Server();
var remotes = [
  { port: 8080, path: 'web-server' },
  { port: 8081, path: 'job-server' },
  { port: 8082, path: 'new-server' }
]

server.connection({ port: 3000 });

remotes.forEach(function (remote) {
  server.route([
    {
      method: 'GET',
      path: '/' + remote.path + '/{params*}',
      handler: {
        proxy: {
          mapUri: function(request, callback) {
            var url = "http://localhost:" + remote.port + "/" + request.url.href.replace('/' + remote.path + '/', '');
            console.log(request.url.href, '->', url);
            callback(null, url);
          },
          passThrough: true,
          xforward: true
        }
      }
    }
  ]);
});

server.route({
  method: 'GET',
  path: '/{param*}',
  handler: {
    directory: {
      path: 'web',
      listing: true
    }
  }
});

server.start(function () {
  console.log('Server running at:', server.info.uri);
});
