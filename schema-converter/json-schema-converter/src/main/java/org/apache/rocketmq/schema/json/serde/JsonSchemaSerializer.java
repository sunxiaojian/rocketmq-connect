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
import io.openmessaging.connector.api.data.Schema;
import org.apache.rocketmq.schema.json.JsonSchemaConverterConfig;
import org.apache.rocketmq.schema.json.JsonSchemaData;
import org.apache.rocketmq.schema.json.JsonSchemaUtils;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.client.rest.JacksonMapper;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaResponse;
import org.apache.rocketmq.schema.registry.common.model.Compatibility;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;


/**
 * json schema serializer
 */
public class JsonSchemaSerializer{
    protected static final int idSize = 8;
    private SchemaRegistryClient registryClient;
    private final ObjectMapper OBJECT_MAPPER = JacksonMapper.INSTANCE;
    private final JsonSchemaData jsonSchemaData;
    private final JsonSchemaConverterConfig jsonSchemaConverterConfig;

    public JsonSchemaSerializer(Map<String, ?> configs,SchemaRegistryClient registryClient) {
        this.registryClient = registryClient;
        this.jsonSchemaConverterConfig = new JsonSchemaConverterConfig(configs);
        this.jsonSchemaData = new JsonSchemaData(jsonSchemaConverterConfig);
    }

    /**
     * serialize
     * @param topic
     * @param isKey
     * @param value
     * @param schema
     * @return
     */
    public byte[] serialize(String topic, boolean isKey, Schema schema, Object value) {
        if (value == null){
            return null;
        }
        String subjectName = TopicNameStrategy.subjectName(topic, isKey);
        org.everit.json.schema.Schema jsonSchema = jsonSchemaData.fromJsonSchema(schema);
        try {
            long schemaId = getSchemaId(schema, subjectName, jsonSchema);
            JsonNode jsonValue = jsonSchemaData.fromConnectData(schema, value);
            // validate json value
            if (jsonSchemaConverterConfig.validate()) {
                JsonSchemaUtils.validate(jsonSchema, jsonValue);
            }
            // serialize value
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(ByteBuffer.allocate(idSize).putLong(schemaId).array());
            out.write(OBJECT_MAPPER.writeValueAsBytes(jsonValue));
            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (IOException e) {
            throw new SerializationException("Error serializing JSON message", e);
        } catch (RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * get schema id
     * @param schema
     * @param subjectName
     * @param jsonSchema
     * @return
     * @throws IOException
     * @throws RestClientException
     */
    private long getSchemaId(Schema schema, String subjectName, org.everit.json.schema.Schema jsonSchema) throws IOException, RestClientException {
        long schemaId = 0 ;
        if (jsonSchemaConverterConfig.serdeSchemaRegistryId() > -1){
            schemaId = jsonSchemaConverterConfig.serdeSchemaRegistryId();
            // todo  Specify the ID for serialization， get schema info
            return schemaId;
        }
        try {
            GetSchemaResponse schemaResponse = registryClient.getSchemaBySubject(subjectName);
            if (schemaResponse != null) {
                schemaId = schemaResponse.getRecordId();
            }
        } catch (RestClientException e) {}
        if (!jsonSchemaConverterConfig.autoRegistrySchema() && schemaId == 0){
            throw new RuntimeException("No related schema found");
        }
        if (jsonSchemaConverterConfig.autoRegistrySchema() && schemaId == 0 ){
            RegisterSchemaResponse registerSchemaResponse = registryClient.registerSchema(
                    subjectName,
                    schema.getName() == null || schema.getName().equals("")
                            ? subjectName : schema.getName(),
                    RegisterSchemaRequest
                            .builder()
                            .schemaType(SchemaType.JSON)
                            .compatibility(Compatibility.BACKWARD)
                            .schemaIdl(jsonSchema.toString())
                            .desc(schema.getDoc())
                            .build()
            );
            schemaId = registerSchemaResponse.getRecordId();
        }
        return schemaId;
    }

}