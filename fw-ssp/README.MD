# FW-SSP

Frank-Wolfe Lasso regression Flink job, using stale synchronous parallel iterations and an implementation of parameter server

Sample input data can be generated with `DataGenerator.scala`. Example of input data are provided in the `resources` folder.

The job parameters are defined in the `resources/job.conf` file.

The main class to run is in `DorotheaLassoRegression.scala`