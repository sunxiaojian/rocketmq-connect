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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.schema.common.LocalSchemaRegistryClient;
import org.apache.rocketmq.schema.common.Serializer;
import org.apache.rocketmq.schema.json.JsonSchema;
import org.apache.rocketmq.schema.json.JsonSchemaConverterConfig;
import org.apache.rocketmq.schema.json.util.JsonSchemaUtils;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.client.rest.JacksonMapper;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.model.Compatibility;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;


/**
 * json schema serializer
 */
public class JsonSchemaSerializer implements Serializer<JsonSchema> {
    protected static final int idSize = 8;
    private LocalSchemaRegistryClient registryClient;
    private JsonSchemaConverterConfig converterConfig;
    private final ObjectMapper OBJECT_MAPPER = JacksonMapper.INSTANCE;

    @Override
    public void configure(Map<String, ?> props) {
        this.converterConfig = new JsonSchemaConverterConfig(props);
        this.registryClient =  new LocalSchemaRegistryClient(this.converterConfig);
    }

    /**
     * serialize
     * @param topic
     * @param isKey
     * @param value
     * @param schema
     * @return
     */
    public byte[] serialize(String topic, boolean isKey, JsonSchema schema, Object value) {
        if (value == null){
            return null;
        }
        String subjectName = TopicNameStrategy.subjectName(topic, isKey);
        try {
            RegisterSchemaRequest schemaRequest = RegisterSchemaRequest
                    .builder()
                    .schemaType(schema.schemaType())
                    .compatibility(Compatibility.BACKWARD)
                    .schemaIdl(schema.toString())
                    .desc(schema.name())
                    .build();

            GetSchemaResponse getSchemaResponse = registryClient.getRegistrySchema(subjectName, schema.name(), schemaRequest);
            long schemaId = getSchemaResponse.getRecordId();
            schema = new JsonSchema(getSchemaResponse.getIdl());
            // validate json value
            if (converterConfig.validate()) {
                JsonSchemaUtils.validate(schema.rawSchema(), value);
            }
            // serialize value
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(ByteBuffer.allocate(idSize).putLong(schemaId).array());
            out.write(OBJECT_MAPPER.writeValueAsBytes(value));
            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (IOException e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }
}