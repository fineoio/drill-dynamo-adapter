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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Map;

/**
 * Base class for a key mapper. All key-mapping implementations should inherit from this class
 */
@JsonTypeName("dynamo-key-mapper-impl")
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public abstract class DynamoKeyMapper {

  protected final DynamoKeyMapperSpec spec;

  @JsonCreator
  protected DynamoKeyMapper(@JacksonInject DynamoKeyMapperSpec spec) {
    this.spec = spec;
  }

  @JsonIgnore
  public Map<String, Object> mapHashKey(Object value) {
    return null;
  }

  @JsonIgnore
  public Map<String, Object> mapSortKey(Object value) {
    return null;
  }
}
