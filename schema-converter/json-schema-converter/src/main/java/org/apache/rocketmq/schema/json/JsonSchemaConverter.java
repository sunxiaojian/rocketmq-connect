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

import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import lombok.SneakyThrows;
import org.apache.rocketmq.schema.json.serde.JsonSchemaDeserializer;
import org.apache.rocketmq.schema.json.serde.JsonSchemaSerializer;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClientFactory;

import java.util.Map;

/**
 * json schema converter
 */
public class JsonSchemaConverter implements RecordConverter {
    private SchemaRegistryClient schemaRegistry;
    private JsonSchemaSerializer serializer;
    private JsonSchemaDeserializer deserializer;
    /**
     * is key
     */
    private boolean isKey = false;

    @Override
    public void configure(Map<String, ?> configs) {
        if (!configs.containsKey(JsonSchemaConverterConfig.IS_KEY)) {
            throw new IllegalArgumentException("Configuration item [isKey] can not empty!");
        }
        // convert key
        this.isKey = Boolean.parseBoolean(configs.get(JsonSchemaConverterConfig.IS_KEY).toString());

        JsonSchemaConverterConfig jsonSchemaConfig = new JsonSchemaConverterConfig(configs);
        if (schemaRegistry == null) {
            schemaRegistry = SchemaRegistryClientFactory.newClient(jsonSchemaConfig.getSchemaRegistryUrl(), null);
        }
        serializer = new JsonSchemaSerializer(configs, schemaRegistry);
        deserializer = new JsonSchemaDeserializer(configs, schemaRegistry);
    }

    @SneakyThrows
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema == null && value == null) {
            return null;
        }
        return serializer.serialize(topic, isKey, schema, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        if (value == null) {
            return SchemaAndValue.NULL;
        }
        return deserializer.deserialize(topic, isKey, value);
    }

}
