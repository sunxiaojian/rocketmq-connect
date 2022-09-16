/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.rocketmq.schema.json;

import java.util.Locale;
import java.util.Map;

public class JsonSchemaConverterConfig {
  public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
  public static final String USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG = "use.optional.for.nonrequired";
  public static final boolean USE_OPTIONAL_FOR_NON_REQUIRED_DEFAULT = false;
  public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
  public static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
  public static final String DECIMAL_FORMAT_CONFIG = "decimal.format";
  public static final DecimalFormat DECIMAL_FORMAT_DEFAULT = DecimalFormat.BASE64;
  public static final String IS_KEY = "isKey";
  /**
   * validate schema enabled
   */
  public static final String VALIDATE_ENABLED = "validate.enabled";
  public static final boolean VALIDATE_ENABLED_DEFAULT = true;

  /**
   * auto registry schema
   */

  public static final String AUTO_REGISTER_SCHEMAS = "auto.register.schemas";
  public static final boolean AUTO_REGISTER_SCHEMAS_DEFAULT = true;


  private final Map<String, ?> props;

  public JsonSchemaConverterConfig(Map<String, ?> props) {
    this.props = props;
    if (!props.containsKey(SCHEMA_REGISTRY_URL)) {
      throw new IllegalArgumentException("Config item [schema.registry.url] can not empty!");
    }
  }


  public boolean validate() {
    return props.containsKey(VALIDATE_ENABLED) ?
            Boolean.valueOf(props.get(VALIDATE_ENABLED).toString()) : VALIDATE_ENABLED_DEFAULT;
  }

  public String getSchemaRegistryUrl() {
    return props.get(SCHEMA_REGISTRY_URL).toString();
  }

  public boolean useOptionalForNonRequiredProperties() {
    return props.containsKey(USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG) ?
            Boolean.valueOf(props.get(USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG).toString()) : USE_OPTIONAL_FOR_NON_REQUIRED_DEFAULT;
  }


  /**
   * schema cache size
   *
   * @return
   */
  public int schemaCacheSize() {
    return props.containsKey(SCHEMAS_CACHE_SIZE_CONFIG) ?
            Integer.valueOf(props.get(SCHEMAS_CACHE_SIZE_CONFIG).toString()) : SCHEMAS_CACHE_SIZE_DEFAULT;

  }


  /**
   * decimal format
   * @return
   */
  public DecimalFormat decimalFormat() {
    return props.containsKey(DECIMAL_FORMAT_CONFIG) ?
            DecimalFormat.valueOf(props.get(DECIMAL_FORMAT_CONFIG).toString().toUpperCase(Locale.ROOT)) : DECIMAL_FORMAT_DEFAULT;

  }


  /**
   * auto registry schema
   * @return
   */
  public boolean autoRegistrySchema(){
    return props.containsKey(AUTO_REGISTER_SCHEMAS) ?
            Boolean.valueOf(AUTO_REGISTER_SCHEMAS) : AUTO_REGISTER_SCHEMAS_DEFAULT;
  }

}
