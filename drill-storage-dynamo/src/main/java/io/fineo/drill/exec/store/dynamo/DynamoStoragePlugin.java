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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fineo.drill.exec.store.dynamo.config.DynamoStoragePluginConfig;
import io.fineo.drill.exec.store.dynamo.filter.DynamoPushFilterIntoScan;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGroupScanSpec;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;

public class DynamoStoragePlugin extends AbstractStoragePlugin {

  public static final String NAME = "dynamo";

  private final DynamoStoragePluginConfig config;
  private final String name;
  private final DynamoSchemaFactory factory;
  private final DrillbitContext context;
  private AmazonDynamoDBAsyncClient client;
  private DynamoDB model;

  public DynamoStoragePlugin(DynamoStoragePluginConfig conf, DrillbitContext c, String name) {
    this.context = c;
    this.config = conf;
    this.name = name;
    this.factory = new DynamoSchemaFactory(name, conf, this);
  }

  @Override
  public StoragePluginConfig getConfig() {
    return config;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    factory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection,
    List<SchemaPath> columns) throws IOException {
    DynamoGroupScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new
      TypeReference<DynamoGroupScanSpec>() {
      });
    return new DynamoGroupScan(this, scanSpec, columns, config.getScan(), config.getClient());
  }

  public DrillbitContext getContext() {
    return context;
  }

  @Override
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext,
    PlannerPhase phase) {
    switch (phase) {
      case PHYSICAL:
        return newHashSet(DynamoPushFilterIntoScan.FILTER_ON_PROJECT,
          DynamoPushFilterIntoScan.FILTER_ON_SCAN);
    }
    return Collections.emptySet();
  }

  @Override
  public void close() throws Exception {
    if (this.model != null) {
      this.model.shutdown();
    }
    if (this.client != null) {
      this.client.shutdown();
    }
  }

  public DynamoDB getModel() {
    ensureModel();
    return model;
  }

  private void ensureModel() {
    if (this.model == null) {
      this.client = new AmazonDynamoDBAsyncClient(config.inflateCredentials());
      config.getEndpoint().configure(client);
      this.model = new DynamoDB(client);
    }
  }
}
