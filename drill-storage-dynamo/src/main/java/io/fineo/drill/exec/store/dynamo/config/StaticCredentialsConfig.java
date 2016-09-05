/*
 *    Copyright 2016 Fineo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package io.fineo.drill.exec.store.dynamo.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Map;

/**
 * Statically configured credentials. Its generally not advisable to use this - AWS will load
 * credentials onto machines that you can leverage with the 'profile' mode
 */
@JsonTypeName(StaticCredentialsConfig.NAME)
public class StaticCredentialsConfig {
  public static final String NAME = "static";

  private final String key;
  private final String secret;

  public StaticCredentialsConfig(@JsonProperty("key") String key,
    @JsonProperty("secret") String secret) {
    this.key = key;
    this.secret = secret;
  }

  public String getKey() {
    return key;
  }

  public String getSecret() {
    return secret;
  }

  @JsonIgnore
  public void setCredentials(Map<String, Object> credentials){
    credentials.put(CredentialsUtil.CREDENTIALS_TYPE_KEY, NAME);
    credentials.put(NAME, this);
  }
}
