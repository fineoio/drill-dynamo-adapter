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

package io.fineo.drill.exec.store.dynamo.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapperSpec;

import java.util.List;

@JsonTypeName("dynamo-table")
public class DynamoTableDefinition {

  private final String name;
  private final List<PrimaryKey> keys;
  private final DynamoKeyMapperSpec keyMapper;

  @JsonCreator
  public DynamoTableDefinition(@JsonProperty("name") String name,
    @JsonProperty("keys") List<PrimaryKey> keys, @JsonProperty("key-mapper") DynamoKeyMapperSpec
    keyMapper) {
    this.name = name;
    this.keys = keys;
    this.keyMapper = keyMapper;
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public List<PrimaryKey> getKeys() {
    return keys;
  }

  @JsonProperty("key-mapper")
  public DynamoKeyMapperSpec getKeyMapper() {
    return keyMapper;
  }

  @JsonTypeName("dynamo-key-def")
  public static class PrimaryKey {
    private final String name;
    private String type;
    private final boolean isHashKey;

    @JsonCreator
    public PrimaryKey(@JsonProperty("name") String name, @JsonProperty("type") String type,
      @JsonProperty("isHashKey") boolean isHashKey) {
      this.name = name;
      this.type = type;
      this.isHashKey = isHashKey;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public boolean getIsHashKey() {
      return isHashKey;
    }

    public void setType(String type) {
      this.type = type;
    }

    @Override
    public String toString() {
      return "PrimaryKey{" +
             "name='" + name + '\'' +
             ", type='" + type + '\'' +
             ", isHashKey=" + isHashKey +
             '}';
    }
  }

  @Override
  public String toString() {
    return "DynamoTableDefinition{" +
           "name='" + name + '\'' +
           ", keys=" + keys +
           ", keyMapper=" + keyMapper +
           '}';
  }
}
