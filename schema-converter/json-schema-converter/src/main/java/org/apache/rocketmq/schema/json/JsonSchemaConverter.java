/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.schema.json;

import com.fasterxml.jackson.databind.JsonNode;
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.commons.lang3.SerializationException;
import org.apache.rocketmq.schema.registry.client.NormalSchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.rest.RestService;
import org.apache.rocketmq.schema.registry.client.serde.json.JsonDeserializer;
import org.apache.rocketmq.schema.registry.client.serde.json.JsonSerializer;

import java.util.Map;

/**
 * json schema converter
 */
public class JsonSchemaConverter implements RecordConverter {

    private SchemaRegistryClient schemaRegistry;

    private JsonSerializer serializer;
    private JsonDeserializer deserializer;
    private  JsonSchemaData jsonSchemaData;
    @Override
    public void configure(Map<String, ?> configs) {

        JsonSchemaConverterConfig jsonSchemaConfig = new JsonSchemaConverterConfig(configs);
        if (schemaRegistry == null) {
            schemaRegistry = new NormalSchemaRegistryClient(
                    new RestService(""));
        }
        serializer = new JsonSerializer(schemaRegistry);
        serializer.configure(configs);
        deserializer = new JsonDeserializer(schemaRegistry);
        deserializer.configure(configs);
        jsonSchemaData = new JsonSchemaData(jsonSchemaConfig);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema == null && value == null) {
            return null;
        }
        org.everit.json.schema.Schema jsonSchema = jsonSchemaData.fromJsonSchema(schema);
        JsonNode jsonValue = jsonSchemaData.fromConnectData(schema, value);

        // to byte[]
        return new byte[0];
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return null;
    }
}
