#!/usr/bin/env python3
# -*- coding: utf-8 -*-

################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""
Example: Text Embedding with PyFlink AI Inference

This example demonstrates how to use the infer() method to generate text embeddings
using a pre-trained model from HuggingFace.
"""

from pyflink.datastream import StreamExecutionEnvironment


def text_embedding_example():
    """Example of generating text embeddings."""
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Sample text data
    text_data = [
        {"id": 1, "text": "Apache Flink is a framework for stateful computations"},
        {"id": 2, "text": "PyFlink brings Python support to Apache Flink"},
        {"id": 3, "text": "Machine learning inference in streaming applications"},
        {"id": 4, "text": "Real-time AI model serving with Flink"},
        {"id": 5, "text": "Distributed machine learning at scale"}
    ]
    
    # Create data stream
    data_stream = env.from_collection(text_data)
    
    # Apply inference - generate embeddings
    # Using a small, fast model for demonstration
    embeddings = data_stream.infer(
        model="sentence-transformers/all-MiniLM-L6-v2",
        input_col="text",
        output_col="embedding",
        batch_size=2,
        device="cpu",
        model_warmup=True
    )
    
    # Print results
    embeddings.print()
    
    # Execute
    env.execute("Text Embedding Example")


def sentiment_classification_example():
    """Example of sentiment classification."""
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Sample reviews
    reviews = [
        {"id": 1, "review": "This product is amazing! I love it!"},
        {"id": 2, "review": "Terrible quality, very disappointed."},
        {"id": 3, "review": "It's okay, nothing special."},
        {"id": 4, "review": "Exceeded my expectations! Highly recommended."},
        {"id": 5, "review": "Waste of money, do not buy."}
    ]
    
    data_stream = env.from_collection(reviews)
    
    # Apply sentiment classification
    sentiments = data_stream.infer(
        model="distilbert-base-uncased-finetuned-sst-2-english",
        input_col="review",
        output_col="sentiment",
        task_type="classification",
        batch_size=2,
        device="cpu"
    )
    
    sentiments.print()
    
    env.execute("Sentiment Classification Example")


def kafka_realtime_inference_example():
    """Example of real-time inference with Kafka."""
    from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
    from pyflink.common.serialization import SimpleStringSchema
    import json
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Kafka consumer configuration
    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink-inference-group'
    }
    
    # Create Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics='input-texts',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    
    # Read from Kafka
    kafka_stream = env.add_source(kafka_consumer)
    
    # Parse JSON
    parsed_stream = kafka_stream.map(lambda x: json.loads(x))
    
    # Apply inference
    inference_result = parsed_stream.infer(
        model="sentence-transformers/all-MiniLM-L6-v2",
        input_col="text",
        output_col="embedding",
        batch_size=32,
        max_batch_timeout_ms=50,
        device="cuda:0",  # Use GPU for faster inference
        model_warmup=True
    )
    
    # Convert back to JSON
    output_stream = inference_result.map(lambda x: json.dumps(x))
    
    # Write to Kafka
    kafka_producer = FlinkKafkaProducer(
        topic='output-embeddings',
        serialization_schema=SimpleStringSchema(),
        producer_config=kafka_props
    )
    
    output_stream.add_sink(kafka_producer)
    
    env.execute("Kafka Real-time Inference")


def custom_model_example():
    """Example using a local custom model."""
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    data = [
        {"id": 1, "text": "Example text 1"},
        {"id": 2, "text": "Example text 2"}
    ]
    
    data_stream = env.from_collection(data)
    
    # Use local model path
    result = data_stream.infer(
        model="/path/to/local/model",  # Local model directory
        input_col="text",
        output_col="features",
        device="cpu",
        model_cache_dir="/tmp/model-cache"  # Cache directory
    )
    
    result.print()
    
    env.execute("Custom Model Example")


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python text_embedding_example.py <example>")
        print("Examples:")
        print("  - embedding: Text embedding generation")
        print("  - sentiment: Sentiment classification")
        print("  - kafka: Kafka real-time inference")
        print("  - custom: Custom local model")
        sys.exit(1)
    
    example = sys.argv[1]
    
    if example == "embedding":
        text_embedding_example()
    elif example == "sentiment":
        sentiment_classification_example()
    elif example == "kafka":
        kafka_realtime_inference_example()
    elif example == "custom":
        custom_model_example()
    else:
        print(f"Unknown example: {example}")
        sys.exit(1)
