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

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.collect.ImmutableList;
import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapperSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGroupScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

public class DrillDynamoTable extends DynamicDrillTable {

  private final TableDescription desc;
  private final DynamoKeyMapperSpec key;

  public DrillDynamoTable(DynamoStoragePlugin plugin, String tableName,
    DynamoKeyMapperSpec keyMapper) {
    super(plugin, tableName, new DynamoGroupScanSpec());
    try {
      this.desc = plugin.getModel().getTable(tableName).waitForActive();
    } catch (InterruptedException e) {
      throw new DrillRuntimeException(e);
    }
    this.key = keyMapper;

    DynamoGroupScanSpec spec = ((DynamoGroupScanSpec) this.getSelection());
    // figure out the pk map
    List<KeySchemaElement> keys = desc.getKeySchema();
    List<AttributeDefinition> attributes = desc.getAttributeDefinitions();
    Map<String, DynamoTableDefinition.PrimaryKey> map = new HashMap<>();
    for (KeySchemaElement key : keys) {
      DynamoTableDefinition.PrimaryKey pk = new DynamoTableDefinition.PrimaryKey(key
        .getAttributeName(), null, KeyType.valueOf(key.getKeyType()) == KeyType.HASH);
      map.put(key.getAttributeName(), pk);
    }
    for (AttributeDefinition elem : attributes) {
      map.get(elem.getAttributeName()).setType(elem.getAttributeType());
    }
    List<DynamoTableDefinition.PrimaryKey> pks = newArrayList(map.values());
    DynamoTableDefinition def = new DynamoTableDefinition(tableName, pks, keyMapper);
    spec.setTable(def);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataType type = super.getRowType(typeFactory);
    // force add the star field, this is a dynamic row
    type.getFieldCount();
    // add the sort/partition keys that the mapper should produce
    if (key != null) {
      for (String field : key.getKeyNames()) {
        type.getField(field, true, false);
      }
    }
    // since we don't support pushing the key combination into the filter building, we need to
    // support the key schema elements in our key schema so hash/sort can be used in the filter.
    List<KeySchemaElement> keys = desc.getKeySchema();
    for (KeySchemaElement elem : keys) {
      type.getField(elem.getAttributeName(), true, false);

    }

    return type;
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.of(desc.getItemCount(), ImmutableList.of(), ImmutableList.of());
  }
}
