# Flink Model Serving

This is implementation of the base library independent from the type of the messages that particular solution is using. 
It is strongly typed, implemented using [generics](https://en.wikipedia.org/wiki/Generics_in_Java). Library implementation includes 3 modules:
* [Flink model serving shared](flink-modelserving-shared)
* [Flink model serving Java](flink-modelserving-java)
* [Flink model serving Scala](flink-modelserving-scala)

Here Flink Model Serving shared contains [protobuf definition](https://cwiki.apache.org/confluence/display/FLINK/FLIP-23+-+Model+Serving) 
and Flink model serving Java and Flink model serving Scala provides the same implementation in both Java and Scala. 
Theoretically it is possible to combine the two, but Java and Scala syntax is sufficiently different, so the 2 parallel implementations are provided.

In addition to this both Java and Scala implementation contain a set of unit tests for validating implementation

##Flink Model Serving Java

The implementation is contained in the namespace *org.apache.flink.modelserving.java*, which contains 3 namespaces:

* model - code containing definition of model and its transformation implementation
* query - code containing base implementation for the Flink queryable state query
* server - code containing basic implementation of the Flink specific components of the overall implementation 

**Model** implementation is split into generic and tensorflow based implementation, such implementation allows to add other model types support in the future without disrupting the base code. Generic model implementation includes the following classes:

* *DataConverter* - a set of model transformation methods
* *DataToServe* - a trait defining generic container for data used for model serving and its behavior
* *Model* - a trait defining generic model and its behavior (see above)
* *ModelFactory* - a trait defining generic model factory and its behavior (see above)
* *ModelFactoriesResolver* - a trait defining generic model factory resolver and its behavior. The purpose of this trait is to get the model factory based on the model type. 
* *ModelToServe* - an intermediate representation of the model
* *ModelToServeStats* - model usage statistics
* *ModelWithType* - a combined container for model and its type used by Flink implementation
* *ServingResult* - generic representation of model serving result

A tensorflow namespace inside model namespace contains 4 classes:

* *TensorFlowModel* extends Model by adding Tensorflow specific functionality for the case of optimized Tensorflow model
* *TensorBundelFlowModel* extends Model by adding Tensorflow specific functionality for the case of bundled Tensorflow model
* *TField* a definition of the field in the tensorfow saved model
* *TSignature* a definition of the signature in the tensorfow saved model

**Query** namespace contains a single class - *ModelStateQuery* - implementing Flink Queryable state client for the model state

**Server** namespace contains 3 namespaces:

* Keyed - contains *DataProcessorKeyed* - implementation of the model serving using key based join (see above) and based on Flink's *CoProcessFunction*
* Partitioned - contains *DataProcessorMap* - implementation of the model serving using partion based join (see above) and based on Flink's *RichCoFlatMapFunction*
* Typeshema contains support classes used for type manipulation and includes the following:
    * *ByteArraySchema* - deserialization schema for byte arrays used for reading protobuf based data from Kafka
    * *ModelTypeSerializer* - type serializer used for checkpointing
    * *ModelSerializerConfigSnapshot* - type serializer snapshot configuration for *ModelTypeSerializer*
    * *ModelWithTypeSerializer* - type serializer used for checkpointing 
    * *ModelWithTypeSerializerConfigSnapshot* - type serializer snapshot configuration for *ModelWithTypeSerializer*


##Flink Model Serving Scala

The implementation provides identical functionality to the Java one and is contained in the namespace *org.apache.flink.modelserving.scala*, which contains 3 namespaces:

* model - code containing definition of model and its transformation implementation
* query - code containing base implementation for the Flink queryable state query
* server - code containing basic implementation of the Flink specific components of the overall implementation 

**Model** implementation is split into generic and tensorflow based implementation, such implementation allows to add other model types support in the future without disrupting the base code. Generic model implementation includes the following classes:

* *DataToServe* - a trait defining generic container for data used for model serving and its behavior
* *Model* - a trait defining generic model and its behavior (see above)
* *ModelFactory* - a trait defining generic model factory and its behavior (see above)
* *ModelFactoryResolver* - a trait defining generic model factory resolver and its behavior. The purpose of this trait is to get the model factory based on the model type. 
* *ModelToServe* defines additional case classes used for the overall implementation and a set of data transformations used for model manipulation and transforming it between different implementations. Additional classes included here include ServingResult - a container for model serving result; ModelToServeStats - model to serve statistics (see above) and ModelToServe - internal generic representation of the model
* *ModelWithType* - a combined container for model and its type used by Flink implementation

A tensorflow namespace inside model namespace contains 2 abstract classes:

* *TensorFlowModel* extends Model by adding Tensorflow specific functionality for the case of optimized Tensorflow model
* *TensorBundelFlowModel* extends Model by adding Tensorflow specific functionality for the case of bundled Tensorflow model

**Query** namespace contains a single class - *ModelStateQuery* - implementing Flink Queryable state client for the model state

**Server** namespace contains 3 namespaces:

* Keyed - contains *DataProcessorKeyed* - implementation of the model serving using key based join (see above) and based on Flink's *CoProcessFunction*
* Partitioned - contains *DataProcessorMap* - implementation of the model serving using partion based join (see above) and based on Flink's *RichCoFlatMapFunction*
* Typeshema contains support classes used for type manipulation and includes the following:
    * *ByteArraySchema* - deserialization schema for byte arrays used for reading protobuf based data from Kafka
    * *ModelTypeSerializer* - type serializer used for checkpointing
    * *ModelWithTypeSerializer* - type serializer used for checkpointing 

