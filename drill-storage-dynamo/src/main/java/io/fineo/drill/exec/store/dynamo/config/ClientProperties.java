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

import com.amazonaws.ClientConfiguration;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Configuration properties for 'deep' configuration of a the AWS client. Maps to properties in
 * {@link ClientConfiguration}
 *
 * @see ClientConfiguration
 */
@JsonTypeName(ClientProperties.NAME)
@JsonAutoDetect
public class ClientProperties {

  public static final String NAME = "client";
  private final int connectionTimeout = -1;
  private final int connectionTTL = -1;
  private final int maxConnections = -1;
  private final int maxErrorRetry = -1;
  private final Boolean withGzip = null;
  private final Boolean withReaper = null;
  private final int socketTimeout = -1;
  // size set to the default in the client configuration
  private final int socketReceiveBufferHint = 0;
  private final int socketSendBufferHint = 0;
  // consistent is easier for users to reason about, so default to that. Dynamo units are also
  // calculated based on consistent reads (2x inconsistent reads)
  private final Boolean consistentRead = true;

  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  public int getConnectionTTL() {
    return connectionTTL;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public int getMaxErrorRetry() {
    return maxErrorRetry;
  }

  public Boolean getWithGzip() {
    return withGzip;
  }

  public Boolean getWithReaper() {
    return withReaper;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  public int getSocketReceiveBufferHint() {
    return socketReceiveBufferHint;
  }

  public int getSocketSendBufferHint() {
    return socketSendBufferHint;
  }

  public Boolean getConsistentRead() {
    return consistentRead;
  }

  @JsonIgnore
  public ClientConfiguration getConfiguration() {
    ClientConfiguration clientConfig = new ClientConfiguration();
    if (connectionTimeout > 0) {
      clientConfig.withClientExecutionTimeout(connectionTimeout);
    }
    if (maxConnections > 0) {
      clientConfig.withMaxConnections(maxConnections);
    }
    if (maxErrorRetry > 0) {
      clientConfig.withMaxErrorRetry(maxErrorRetry);
    }
    if (socketTimeout > 0) {
      clientConfig.withSocketTimeout(socketTimeout);
    }
    clientConfig.withSocketBufferSizeHints(socketSendBufferHint, socketReceiveBufferHint);
    if(withGzip != null){
      clientConfig.setUseGzip(withGzip.booleanValue());
    }
    if(withReaper != null){
      clientConfig.setUseReaper(withReaper.booleanValue());
    }

    return clientConfig;
  }
}
