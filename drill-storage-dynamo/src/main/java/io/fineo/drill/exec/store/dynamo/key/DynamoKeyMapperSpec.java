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

package io.fineo.drill.exec.store.dynamo.key;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;
import java.util.Map;

/**
 * Specify a key mapper class
 */
@JsonTypeName(DynamoKeyMapperSpec.NAME)
public class DynamoKeyMapperSpec {
  public static final String NAME = "dynamo-key-mapper";
  private final List<String> keyNames;
  private final List<String> keyValues;
  private final Map<String, Object> args;

  public DynamoKeyMapperSpec(@JsonProperty("key-name") List<String> keyName,
    @JsonProperty("key-type") List<String> keyValue,
    @JsonProperty("args") Map<String, Object> args) {
    this.keyNames = keyName;
    this.keyValues = keyValue;
    this.args = args;
  }

  @JsonProperty("key-name")
  public List<String> getKeyNames() {
    return keyNames;
  }

  @JsonProperty("key-value")
  public List<String> getKeyValues() {
    return keyValues;
  }

  @JsonProperty("args")
  public Map<String, Object> getArgs() {
    return args;
  }

  @Override
  public String toString() {
    return "DynamoKeyMapperSpec{" +
           "keyNames=" + keyNames +
           ", keyValues=" + keyValues +
           ", args=" + args +
           '}';
  }
}
