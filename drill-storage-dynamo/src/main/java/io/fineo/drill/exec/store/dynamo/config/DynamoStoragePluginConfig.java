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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapperSpec;
import org.apache.drill.common.logical.StoragePluginConfig;

import java.util.HashMap;
import java.util.Map;

@JsonTypeName(DynamoStoragePluginConfig.NAME)
public class DynamoStoragePluginConfig extends StoragePluginConfig {
  public static final String NAME = "dynamo";

  private AWSCredentialsProvider inflatedCredentials;
  private DynamoEndpoint endpoint;
  private final ClientProperties client;
  private ParallelScanProperties scan;
  private Map<String, Object> credentials;
  private Map<String, DynamoKeyMapperSpec> keyMappers;

  @JsonCreator
  public DynamoStoragePluginConfig(
    @JsonProperty("credentials") Map<String, Object> credentials,
    @JsonProperty(DynamoEndpoint.NAME) DynamoEndpoint endpoint,
    @JsonProperty(ClientProperties.NAME) ClientProperties client,
    @JsonProperty(ParallelScanProperties.NAME) ParallelScanProperties scan,
    @JsonProperty("key-mappers") Map<String, DynamoKeyMapperSpec> keyMappers) {
    this.credentials = credentials;
    this.endpoint = endpoint;
    this.client = client == null ? new ClientProperties() : client;
    this.scan = scan == null ? new ParallelScanProperties() : scan;
    this.keyMappers = keyMappers;
  }

  @JsonIgnore
  public AWSCredentialsProvider inflateCredentials() {
    if (this.inflatedCredentials == null) {
      this.inflatedCredentials = CredentialsUtil.getProvider(credentials);
    }
    return inflatedCredentials;
  }

  @SuppressWarnings("unused")
  @JsonProperty("credentials")
  public Map<String, Object> getCredentials() {
    return credentials;
  }

  @JsonProperty(DynamoEndpoint.NAME)
  public DynamoEndpoint getEndpoint() {
    return endpoint;
  }

  @JsonProperty(ClientProperties.NAME)
  public ClientProperties getClient() {
    return client;
  }

  @JsonProperty(ParallelScanProperties.NAME)
  public ParallelScanProperties getScan() {
    return scan;
  }

  @JsonProperty("key-mappers")
  public Map<String, DynamoKeyMapperSpec> getKeyMappers() {
    return keyMappers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof DynamoStoragePluginConfig))
      return false;

    DynamoStoragePluginConfig that = (DynamoStoragePluginConfig) o;

    if (getEndpoint() != null ? !getEndpoint().equals(that.getEndpoint()) :
        that.getEndpoint() != null)
      return false;
    if (getClient() != null ? !getClient().equals(that.getClient()) : that.getClient() != null)
      return false;
    if (getScan() != null ? !getScan().equals(that.getScan()) : that.getScan() != null)
      return false;
    if (getCredentials() != null ? !getCredentials().equals(that.getCredentials()) :
        that.getCredentials() != null)
      return false;
    return getKeyMappers() != null ? getKeyMappers().equals(that.getKeyMappers()) :
           that.getKeyMappers() == null;

  }

  @Override
  public int hashCode() {
    int result = getEndpoint() != null ? getEndpoint().hashCode() : 0;
    result = 31 * result + (getClient() != null ? getClient().hashCode() : 0);
    result = 31 * result + (getScan() != null ? getScan().hashCode() : 0);
    result = 31 * result + (getCredentials() != null ? getCredentials().hashCode() : 0);
    result = 31 * result + (getKeyMappers() != null ? getKeyMappers().hashCode() : 0);
    return result;
  }

  @VisibleForTesting
  @JsonIgnore
  public void setEndpointForTesting(DynamoEndpoint endpoint) {
    this.endpoint = endpoint;
  }

  @VisibleForTesting
  @JsonIgnore
  public void setCredentialsForTesting(Map<String, Object> credentials) {
    this.credentials = credentials;
    this.inflatedCredentials = CredentialsUtil.getProvider(credentials);
  }

  @VisibleForTesting
  @JsonIgnore
  public void setScanPropertiesForTesting(ParallelScanProperties scan) {
    this.scan = scan;
  }

  @VisibleForTesting
  @JsonIgnore
  public void setDynamoKeyMapperForTesting(String tableName, DynamoKeyMapperSpec spec) {
    if (this.keyMappers == null) {
      this.keyMappers = new HashMap<>();
    }
    this.keyMappers.put(tableName, spec);
  }
}
