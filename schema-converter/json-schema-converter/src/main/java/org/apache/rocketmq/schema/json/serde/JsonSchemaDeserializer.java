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
package org.apache.rocketmq.schema.json.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openmessaging.connector.api.data.SchemaAndValue;
import org.apache.rocketmq.schema.json.JsonSchemaConverterConfig;
import org.apache.rocketmq.schema.json.JsonSchemaData;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.rest.JacksonMapper;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * json schema deserializer
 */
public class JsonSchemaDeserializer {

    protected static final int idSize = 8;
    private final SchemaRegistryClient registryClient;
    private final ObjectMapper OBJECT_MAPPER = JacksonMapper.INSTANCE;
    private final JsonSchemaData jsonSchemaData;
    private final JsonSchemaConverterConfig jsonSchemaConverterConfig;

    public JsonSchemaDeserializer(Map<String, ?> props, SchemaRegistryClient registryClient) {
        this.registryClient = registryClient;
        this.jsonSchemaConverterConfig = new JsonSchemaConverterConfig(props);
        this.jsonSchemaData = new JsonSchemaData(this.jsonSchemaConverterConfig);
    }


    /**
     * deserialize
     *
     * @param topic
     * @param isKey
     * @param payload
     * @return
     */
    public SchemaAndValue deserialize(String topic, boolean isKey, byte[] payload) {
        if (payload == null) {
            return null;
        }
        String subjectName = TopicNameStrategy.subjectName(topic, isKey);
        try {
            GetSchemaResponse response = registryClient.getSchemaBySubject(subjectName);
            ByteBuffer buffer = ByteBuffer.wrap(payload);
            long schemaId = buffer.getLong();
            if (schemaId != response.getRecordId()) {
                throw new RuntimeException("Deserialization schema id cannot match, ser schemaId " + schemaId + ", DeSer schema id" + response.getRecordId());
            }
            int length = buffer.limit() - idSize;
            int start = buffer.position() + buffer.arrayOffset();

            // Return JsonNode if type is null
            JsonNode value = OBJECT_MAPPER.readTree(new ByteArrayInputStream(buffer.array(), start, length));

            // load json schema
            SchemaLoader.SchemaLoaderBuilder builder = SchemaLoader
                    .builder()
                    .useDefaults(true)
                    .draftV7Support();
            JSONObject jsonObject = new JSONObject(response.getIdl());
            builder.schemaJson(jsonObject);
            SchemaLoader loader = builder.build();
            Schema schema = loader.load().build();
            // validate schema
            if (jsonSchemaConverterConfig.validate()) {
                schema.validate(value);
            }
            io.openmessaging.connector.api.data.Schema connectSchema = jsonSchemaData.toConnectSchema(schema, 1);
            return new SchemaAndValue(connectSchema, value);
        } catch (RestClientException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
