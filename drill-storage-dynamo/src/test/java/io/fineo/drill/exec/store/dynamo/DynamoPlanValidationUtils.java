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

package io.fineo.drill.exec.store.dynamo;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGroupScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoFilterSpec;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.fineo.drill.exec.store.dynamo.spec.filter.DynamoFilterSpec.create;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DynamoPlanValidationUtils {

  static final ObjectMapper MAPPER = new ObjectMapper();

  private DynamoPlanValidationUtils(){
  }

  public static DynamoFilterSpec equals(String key, Object value) {
    return create("equal", key, value);
  }

  public static DynamoFilterSpec gte(String key, Object val) {
    return create("greater_than_or_equal_to", key, val);
  }

  public static DynamoFilterSpec lte(String key, Object val) {
    return create("less_than_or_equal_to", key, val);
  }

  public static DynamoGroupScanSpec validatePlan(Map<String, Object> dynamo, List<String> columns,
    DynamoReadFilterSpec scan,
    List<DynamoReadFilterSpec> getOrQuery) throws IOException {
    assertEquals(DynamoGroupScan.NAME, dynamo.get("pop"));
    assertEquals(columns, dynamo.get("columns"));
    assertTrue((Boolean) dynamo.get("filterPushedDown"));
    Map<String, Object> spec = (Map<String, Object>) dynamo.get("spec");
    String specString = MAPPER.writeValueAsString(spec);
    DynamoGroupScanSpec gSpec = MAPPER.readValue(specString, DynamoGroupScanSpec.class);
    if (scan == null) {
      assertNull(gSpec.getScan());
      List<DynamoReadFilterSpec> actual = gSpec.getGetOrQuery();
      Collections.sort(actual, (spec1, spec2) -> spec1.toString().compareTo(spec2.toString()));
      assertEquals(getOrQuery, actual);
    } else {
      assertNull(gSpec.getGetOrQuery());
      assertEquals(scan, gSpec.getScan());
    }
    return gSpec;
  }
}
