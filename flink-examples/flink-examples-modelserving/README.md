# Flink Model Serving Example

This is implementation of the a wine quality example based on the Flink Model Serving Library library. Implementation includes 3 modules:

* Flink model serving example shared
* Flink model serving example Java
* Flink model serving example Scala

Implementation demonstrates how to use library to build Flink Model serving implementation.

When building a new implementation you first need to define data that is used for model serving. An example is using [wine quality](https://www.kaggle.com/vishalyo990/prediction-of-quality-of-wine) example. 
Data definition is for wine is provided in model serving example shared. We are using protobuf encoding for data here, but other encoding can be used as well. 
Additionally Shared namespace contains implementation of embedded Kafka server (for local testing) and a Kafka provider periodically publishing both data and model, 
that can be used for testing the example.

There are two implementations of example - Java and Scala, that works exactly the same but are using corresponding version of Library.

Lets walk through the Scala implementation. 
It is located in the namespace *org.apache.flink.examples.modelserving.scala* and is comprised of three namespaces:
* Model
* Query
* Server

Model namespace contains three classes, extending library and implementing specific operations for a given data type.
* *WineTensorFlowModel* extends *TensorFlowModel* class by implementing processing specific to Wine quality data
* *WineTensorFlowBundeledModel* extends *TensorFlowBundelModel* class by implementing processing specific to Wine quality data
* *WineFactoryResolver* extends *ModelFactoryResolver* class by specifying above two classes as available factories

Server namespace implements 2 supporting classes : 
*DataRecord* implementing *DataToServe* trait for Wine type and *BadDataHandler* - simple data error handler implementation

It also provides complete Flink implementation for both key based join (*ModelServingKeyedJob*) and partition base join (*ModelServingFlatJob*).

To run the example first start *org.apache.flink.examples.modelserving.client.client.DataProvider* class, that will publish both data and model messages 
to Kafka (to test that publication works correctly you can use *org.apache.flink.examples.modelserving.client.client.DataReader*; do not forget 
to stop it before you proceed). Once data provider is running you can start actual model serving (*ModelServingKeyedJob* or *ModelServingFlatJob*). 
If you are using keyed version, you can also run *ModelStateQueryJob* from the query namespace to see execution statistics 
(Flink only supports query for keyed join). When running query, do not forget to update jobID with the actual ID of your run

