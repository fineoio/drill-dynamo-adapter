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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Fully define a scan of the table
 */
@JsonTypeName(DynamoScanSpec.NAME)
@JsonAutoDetect
public class DynamoScanSpec {

  public static final String NAME = "dynamo-scan-spec";

  private DynamoTableDefinition table;
  private DynamoReadFilterSpec filter;

  @JsonCreator
  public DynamoScanSpec(@JsonProperty("table") DynamoTableDefinition table, @JsonProperty
    ("filter") DynamoReadFilterSpec filter) {
    this.table = table;
    this.filter = filter;
  }

  public DynamoScanSpec(){
    this.filter = new DynamoReadFilterSpec();
  }

  public void setTable(DynamoTableDefinition table) {
    this.table = table;
  }

  public DynamoTableDefinition getTable() {
    return this.table;
  }

  public DynamoReadFilterSpec getFilter() {
    return filter;
  }

  public void setFilter(DynamoReadFilterSpec filter) {
    this.filter = filter;
  }

  public DynamoScanSpec(DynamoScanSpec other) {
    this.table = other.table;
    this.filter = other.filter;
  }
}
