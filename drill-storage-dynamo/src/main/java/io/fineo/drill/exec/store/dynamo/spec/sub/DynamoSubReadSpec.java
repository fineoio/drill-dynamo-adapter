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

package io.fineo.drill.exec.store.dynamo.spec.sub;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import org.apache.drill.common.expression.SchemaPath;

import java.util.List;


public abstract class DynamoSubReadSpec {
  private final List<SchemaPath> columns;
  private final DynamoReadFilterSpec filter;

  public DynamoSubReadSpec(DynamoReadFilterSpec filter, List<SchemaPath> columns) {
    this.filter = filter;
    this.columns = columns;
  }

  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty
  public DynamoReadFilterSpec getFilter() {
    return filter;
  }
}
