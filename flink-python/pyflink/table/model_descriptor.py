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
from typing import Dict, Union, Optional

from pyflink.common.config_options import ConfigOption
from pyflink.java_gateway import get_gateway
from pyflink.table.schema import Schema

__all__ = ['ModelDescriptor']


class ModelDescriptor(object):
    """
    Describes a CatalogModel representing a model.

    ModelDescriptor is a template for creating a CatalogModel instance. It closely resembles the
    "CREATE MODEL" SQL DDL statement, containing input schema, output schema, and other
    characteristics.

    This can be used to register a model in the Table API.
    """
    def __init__(self, j_model_descriptor):
        self._j_model_descriptor = j_model_descriptor

    @staticmethod
    def for_provider(provider: str) -> 'ModelDescriptor.Builder':
        """
        Creates a new :class:`~pyflink.table.ModelDescriptor.Builder` for a model using the given
        provider value.

        :param provider: The provider value for the model.
        """
        gateway = get_gateway()
        j_builder = gateway.jvm.ModelDescriptor.forProvider(provider)
        return ModelDescriptor.Builder(j_builder)

    def get_options(self) -> Dict[str, str]:
        return self._j_model_descriptor.getOptions()

    def get_input_schema(self) -> Optional[Schema]:
        j_input_schema = self._j_model_descriptor.getInputSchema()
        if j_input_schema.isPresent():
            return Schema(j_input_schema.get())
        else:
            return None

    def get_output_schema(self) -> Optional[Schema]:
        j_output_schema = self._j_model_descriptor.getOutputSchema()
        if j_output_schema.isPresent():
            return Schema(j_output_schema.get())
        else:
            return None

    def get_comment(self) -> Optional[str]:
        j_comment = self._j_model_descriptor.getComment()
        if j_comment.isPresent():
            return j_comment.get()
        else:
            return None

    def __str__(self):
        return self._j_model_descriptor.toString()

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self._j_model_descriptor.equals(other._j_model_descriptor))

    def __hash__(self):
        return self._j_model_descriptor.hashCode()

    class Builder(object):
        """
        Builder for ModelDescriptor.
        """

        def __init__(self, j_builder):
            self._j_builder = j_builder

        def input_schema(self, input_schema: Schema) -> 'ModelDescriptor.Builder':
            """
            Define the input schema of the ModelDescriptor.
            """
            self._j_builder.inputSchema(input_schema._j_schema)
            return self

        def output_schema(self, output_schema: Schema) -> 'ModelDescriptor.Builder':
            """
            Define the output schema of the ModelDescriptor.
            """
            self._j_builder.outputSchema(output_schema._j_schema)
            return self

        def option(self, key: Union[str, ConfigOption], value) -> 'ModelDescriptor.Builder':
            """
            Sets the given option on the model.

            Option keys must be fully specified.

            Example:
            ::

                >>> ModelDescriptor.for_connector("OPENAI")\
                ...     .input_schema(input_schema)\
                ...     .output_schema(output_schema)\
                ...     .option("task", "regression")\
                ...     .build()

            """
            if isinstance(key, str):
                self._j_builder.option(key, value)
            else:
                self._j_builder.option(key._j_config_option, value)
            return self

        def comment(self, comment: str) -> 'ModelDescriptor.Builder':
            """
            Define the comment for this model.
            """
            self._j_builder.comment(comment)
            return self

        def build(self) -> 'ModelDescriptor':
            """
            Returns an immutable instance of :class:`~pyflink.table.ModelDescriptor`.
            """
            return ModelDescriptor(self._j_builder.build())
