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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import org.apache.drill.common.expression.SchemaPath;

import java.util.List;

@JsonTypeName("dynamo-sub-get-spec")
public class DynamoSubGetSpec extends DynamoSubReadSpec {

  @JsonCreator
  public DynamoSubGetSpec(@JsonProperty("filter") DynamoReadFilterSpec filter,
    @JsonProperty("columns") List<SchemaPath> columns) {
    super(filter, columns);
  }
}
