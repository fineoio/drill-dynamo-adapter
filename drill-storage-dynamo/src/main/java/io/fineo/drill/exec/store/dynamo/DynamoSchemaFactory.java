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

import com.google.common.collect.ImmutableList;
import io.fineo.drill.exec.store.dynamo.config.DynamoStoragePluginConfig;
import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapperSpec;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DynamoSchemaFactory implements SchemaFactory {

  private final String name;
  private final DynamoStoragePlugin plugin;
  private final DynamoStoragePluginConfig conf;

  public DynamoSchemaFactory(String name, DynamoStoragePluginConfig conf,
    DynamoStoragePlugin plugin) {
    this.name = name;
    this.conf = conf;
    this.plugin = plugin;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    DynamoSchema schema = new DynamoSchema(name);
    parent.add(name, schema);
  }

  private class DynamoSchema extends AbstractSchema {
    public DynamoSchema(String name) {
      super(ImmutableList.of(), name);
    }

    @Override
    public Set<String> getTableNames() {
      return StreamSupport.stream(plugin.getModel().listTables().pages().spliterator(), false)
                          .flatMap(page -> StreamSupport.stream(page.spliterator(), false))
                          .filter(table -> table != null)
                          .map(table -> table.getTableName())
                          .collect(Collectors.toSet());
    }

    @Override
    public Table getTable(String name) {
      DynamoKeyMapperSpec keyMapper = null;
      Map<String, DynamoKeyMapperSpec> mappers = conf.getKeyMappers();
      if (mappers != null) {
        for (Map.Entry<String, DynamoKeyMapperSpec> mapper : mappers.entrySet()) {
          if (name.matches(mapper.getKey())) {
            keyMapper = mapper.getValue();
            break;
          }
        }
      }

      return new DrillDynamoTable(plugin, name, keyMapper);
    }

    @Override
    public String getTypeName() {
      return DynamoStoragePlugin.NAME;
    }

    @Override
    public boolean isMutable() {
      return false;
    }
  }
}
