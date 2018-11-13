Flink Runtime Web
=================
[![Travis branch](https://travis-ci.com/vthinkxie/flink-runtime-web.svg?branch=master)](https://travis-ci.com/vthinkxie/flink-runtime-web)

Flink Runtime Web is an open source, dashboard and metric monitor for [Flink](https://flink.apache.org/).

![](https://img.alicdn.com/tfs/TB1AHpPnlLoK1RjSZFuXXXn0XXa-2740-1920.png)
![](https://img.alicdn.com/tfs/TB1gVpYngHqK1RjSZFEXXcGMXXa-2790-1872.png)
![](https://img.alicdn.com/tfs/TB1SGJOnhTpK1RjSZR0XXbEwXXa-2790-1872.png)
![](https://img.alicdn.com/tfs/TB1Y0GdniLaK1RjSZFxXXamPFXa-2624-1850.png)
![](https://img.alicdn.com/tfs/TB1g8pQngHqK1RjSZFPXXcwapXa-2628-1798.png)

## Development & Debugging

### 1.Install Dependencies

Clone this git to local, and install dependencies

```bash
$ npm install
```

### 2.Start a Local Flink Cluster

More information can be found [here](https://ci.apache.org/projects/flink/flink-docs-release-1.6/quickstart/setup_quickstart.html).

```bash
$ ./bin/start-cluster.sh
```

### 3.Proxy the frontend to the backend.

You can modify the proxy target in the `proxy.conf.json`, the default proxy target is `localhost:8081`.

```bash
$ npm run proxy
```

## CodeStyle & Lint

```bash
$ npm run lint
```

## Building & Deployment

```bash
$ npm run build
```

Entry files will be built and generated in `web-dashboard/web/next` directory, where you can deploy it to different environments.

You can config the `outputPath` in `angular.json`.


## Dependency

- Framework: [Angular](https://angular.io)
- CLI Tools: [Angular CLI](https://cli.angular.io)
- UI Components: [NG-ZORRO](https://github.com/NG-ZORRO/ng-zorro-antd)
