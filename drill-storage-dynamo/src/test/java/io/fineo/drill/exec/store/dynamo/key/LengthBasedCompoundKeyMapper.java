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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple mapper that just converts a compound key into length-based substring parts.
 */
public class LengthBasedCompoundKeyMapper extends LengthBasedTwoPartHashKeyMapper {

  private final int length;

  @JsonCreator
  public LengthBasedCompoundKeyMapper(@JsonProperty("length") int length,
    @JacksonInject DynamoKeyMapperSpec spec) {
    super(length, spec);
    this.length = length;
  }

  @Override
  @JsonIgnore
  public Map<String, Object> mapSortKey(Object value) {
    String val = (String) value;
    String p1 = val.substring(0, length);
    String p2 = val.substring(length);
    Map<String, Object> out = new HashMap<>();
    // need to make sure that you actually write a big decimal representation to match what
    // dynamo generates as values
    out.put(spec.getKeyNames().get(2), new BigDecimal(new BigInteger(p1)));
    out.put(spec.getKeyNames().get(3), new BigDecimal(new BigInteger(p2)));
    return out;
  }
}
