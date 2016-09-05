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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.fineo.drill.exec.store.dynamo.DynamoStoragePlugin;
import io.fineo.drill.exec.store.dynamo.config.ClientProperties;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import io.fineo.drill.exec.store.dynamo.config.DynamoStoragePluginConfig;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.util.Iterator;
import java.util.List;

@JsonTypeName("dynamo-segment-scanProps")
public class DynamoSubScan extends AbstractBase implements SubScan {

  private final DynamoStoragePlugin plugin;
  private final List<DynamoSubReadSpec> specs;
  private final DynamoStoragePluginConfig storage;
  private final List<SchemaPath> columns;
  private final ClientProperties client;
  private final ParallelScanProperties scanProps;
  private final DynamoTableDefinition table;

  @JsonCreator
  public DynamoSubScan(@JacksonInject StoragePluginRegistry registry,
    @JsonProperty("storage") StoragePluginConfig storage,
    @JsonProperty("specs") List<DynamoSubReadSpec> subReadList,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("client") ClientProperties client,
    @JsonProperty("scanProps") ParallelScanProperties scanProps,
    @JsonProperty("table") DynamoTableDefinition table) throws ExecutionSetupException {
    this((DynamoStoragePlugin) registry.getPlugin(storage), storage, subReadList, columns,
      client, scanProps, table);
  }

  public DynamoSubScan(DynamoStoragePlugin plugin, StoragePluginConfig config,
    List<DynamoSubReadSpec> specs, List<SchemaPath> columns, ClientProperties client,
    ParallelScanProperties scanProps, DynamoTableDefinition spec) {
    super((String) null);
    this.plugin = plugin;
    this.specs = specs;
    this.storage = (DynamoStoragePluginConfig) config;
    this.columns = columns;
    this.client = client;
    this.scanProps = scanProps;
    this.table = spec;
  }

  public DynamoSubScan(DynamoSubScan other) {
    super(other);
    this.plugin = other.plugin;
    this.specs = other.specs;
    this.storage = other.storage;
    this.columns = other.columns;
    this.client = other.client;
    this.scanProps = other.scanProps;
    this.table = other.table;
  }

  @Override
  public int getOperatorType() {
    // outside the standard operator range
    return 2000;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
    throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
    throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new DynamoSubScan(this);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  public List<DynamoSubReadSpec>  getSpecs() {
    return specs;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  public ClientProperties getClient() {
    return client;
  }

  public DynamoEndpoint getEndpoint() {
    return this.storage.getEndpoint();
  }

  public ParallelScanProperties getScanProps() {
    return scanProps;
  }

  public DynamoTableDefinition getTable() {
    return table;
  }

  @JsonIgnore
  public AWSCredentialsProvider getCredentials() {
    return this.storage.inflateCredentials();
  }

}
