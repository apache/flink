Flink-ML constitutes the machine learning library of Apache Flink.
Our vision is to make machine learning easily accessible to a wide audience and yet to achieve extraordinary performance.
For this purpose, Flink-ML is based on two pillars:

Flink-ML contains implementations of popular ML algorithms which are highly optimized for Apache Flink.
Theses implementations allow to scale to data sizes which vastly exceed the memory of a single computer.
Flink-ML currently comprises the following algorithms:

* Classification
** Soft-margin SVM
* Regression
** Multiple linear regression
* Recommendation
** Alternating least squares (ALS)

Since most of the work in data analytics is related to post- and pre-processing of data where the performance is not crucial, Flink wants to offer a simple abstraction to do that.
Linear algebra, as common ground of many ML algorithms, represents such a high-level abstraction.
Therefore, Flink will support the Mahout DSL as a execution engine and provide tools to neatly integrate the optimized algorithms into a linear algebra program.

Flink-ML has just been recently started.
As part of Apache Flink, it heavily relies on the active work and contributions of its community and others.
Thus, if you want to add a new algorithm to the library, then find out [how to contribute]((http://flink.apache.org/how-to-contribute.html)) and open a pull request!