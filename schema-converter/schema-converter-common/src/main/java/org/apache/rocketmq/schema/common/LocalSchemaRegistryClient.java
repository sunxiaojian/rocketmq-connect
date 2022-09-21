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

package org.apache.rocketmq.schema.common;

import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClientFactory;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;

import java.io.IOException;

/**
 * Local schema registry client
 */
public class LocalSchemaRegistryClient {

    private final SchemaRegistryClient schemaRegistryClient;
    private final Long serdeSchemaRegistryId;
    private final boolean autoRegisterSchemas;
    private final boolean useLatestVersion;
    public LocalSchemaRegistryClient(AbstractConverterConfig config){
        this.schemaRegistryClient = SchemaRegistryClientFactory.newClient(config.getSchemaRegistryUrl(),null);
        this.serdeSchemaRegistryId = config.getSerdeSchemaRegistryId();
        this.autoRegisterSchemas = config.isAutoRegisterSchemas();
        this.useLatestVersion = config.isUseLatestVersion();
    }

    /**
     * Get registry schema by specify policy
     * @param subject
     * @param schemaName
     * @param request
     * @return
     */
    public GetSchemaResponse getRegistrySchema(String subject, String schemaName, RegisterSchemaRequest request){
        if (autoRegisterSchemas) {
            return this.autoRegisterSchema(subject, schemaName, request);
        } else if (serdeSchemaRegistryId != null){
            throw new RuntimeException("Getting schema based on ID is not supported temporarily");
        } else {
            return getSchemaLatestVersion(subject);
        }
    }


    /**
     * auto register schema
     * @param subject
     * @param schemaName
     * @param request
     */
    public GetSchemaResponse autoRegisterSchema(String subject, String schemaName, RegisterSchemaRequest request){
        try {
            this.schemaRegistryClient.registerSchema(subject, schemaName, request);
        } catch (RestClientException | IOException e) {
            if (e instanceof IOException ) {
                throw new RuntimeException(e);
            }
        } finally {
            try {
                return this.schemaRegistryClient.getSchemaBySubject(subject);
            } catch (RestClientException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Get schema latest version
     * @param subject
     * @return
     */
    public GetSchemaResponse getSchemaLatestVersion(String subject){
        try {
            return schemaRegistryClient.getSchemaBySubject(subject);
        } catch (RestClientException | IOException e) {
            throw new RuntimeException(e);
        }
    }

}
