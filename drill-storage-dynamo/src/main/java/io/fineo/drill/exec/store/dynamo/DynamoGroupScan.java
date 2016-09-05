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

import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import io.fineo.drill.exec.store.dynamo.config.ClientProperties;
import io.fineo.drill.exec.store.dynamo.config.DynamoStoragePluginConfig;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoGetFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGroupScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubGetSpec;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubQuerySpec;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubReadSpec;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubScan;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubScanSpec;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Logical representation of a scan of a dynamo table. Actual fragments of the scan that get
 */
@JsonTypeName(DynamoGroupScan.NAME)
public class DynamoGroupScan extends AbstractGroupScan {
  public static final String NAME = "dynamo-scan";
  // limit imposed by AWS
  private static final int MAX_DYNAMO_PARALLELIZATION = 1000000;

  private final DynamoStoragePlugin plugin;
  private DynamoGroupScanSpec spec;
  private List<SchemaPath> columns;
  private final StoragePluginConfig config;
  private final ParallelScanProperties scan;
  private final ClientProperties client;
  private ListMultimap<Integer, DynamoWork> assignments;

  // used to calculate the work distribution
  private ArrayList<DynamoWork> work;
  private TableDescription desc;
  private boolean filterPushedDown;

  @JsonCreator
  public DynamoGroupScan(@JsonProperty(DynamoGroupScanSpec.NAME) DynamoGroupScanSpec dynamoSpec,
    @JsonProperty("storage") DynamoStoragePluginConfig storagePluginConfig,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("scan") ParallelScanProperties scan,
    @JsonProperty("client") ClientProperties client,
    @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException,
    ExecutionSetupException {
    this((DynamoStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), dynamoSpec,
      columns, scan, client);
  }

  public DynamoGroupScan(DynamoStoragePlugin plugin, DynamoGroupScanSpec dynamoSpec,
    List<SchemaPath> columns, ParallelScanProperties scan, ClientProperties client) {
    super((String) null);
    this.plugin = plugin;
    this.spec = dynamoSpec;
    this.columns = columns;
    this.config = plugin.getConfig();
    this.scan = scan;
    this.client = client;
    init();
  }

  private void init() {
    try {
      this.desc = this.plugin.getModel().getTable(this.spec.getTable().getName()).waitForActive();
    } catch (InterruptedException e) {
      throw new DrillRuntimeException(e);
    }
  }

  public DynamoGroupScan(DynamoGroupScan other) {
    super((String) null);
    this.plugin = other.plugin;
    this.spec = other.spec;
    this.columns = other.columns;
    this.config = other.config;
    this.desc = other.desc;
    this.scan = other.scan;
    this.client = other.client;
    this.filterPushedDown = other.filterPushedDown;
  }

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints)
    throws PhysicalOperatorSetupException {
    this.work = new ArrayList<>();
    // if we have a list of queries/gets, then that is the 'true' limit on the set of work
    if (spec.getScan() != null) {
      setScanWork(spec.getScan(), endpoints);
    } else if (spec.getGetOrQuery() != null) {
      setQueryWork(spec.getGetOrQuery());
    } else {
      // its actually a scan, but we have no filters
      setScanWork(new DynamoReadFilterSpec(), endpoints);
    }

    this.assignments = AssignmentCreator.getMappings(endpoints, work, plugin.getContext());
  }

  private void setScanWork(DynamoReadFilterSpec filter,
    List<CoordinationProtos.DrillbitEndpoint> endpoints) {
    int max = getMaxParallelizationWidth();
    // determine how many segments we can add to each endpoint
    int totalPerEndpoint = scan.getSegmentsPerEndpoint() * endpoints.size();
    // more scans than the total allowed, so figure out how many we can have per endpoint
    if (totalPerEndpoint < max) {
      max = totalPerEndpoint;
    }

    // attempt to evenly portion the rows across the endpoints
    long rowsPerEndpoint = desc.getItemCount() / endpoints.size();
    long units = rowsPerEndpoint / scan.getApproximateRowsPerEndpoint();
    // we have less than approx 1 segment's worth of work
    if (units == 0) {
      units = 1;
    }

    if (units > max) {
      units = max;
    }

    // no affinity for any work unit
    long portion = this.desc.getTableSizeBytes() / units;
    for (int i = 0; i < units; i++) {
      work.add(new DynamoScanWork((int) units, i, portion == 0 ? 1 : portion, filter));
    }
  }

  private void setQueryWork(List<DynamoReadFilterSpec> getOrQuery) {
    int units = getOrQuery.size();
    // guess at how much data is returned... 10x the total slice seems like a nice number
    long portion = this.desc.getTableSizeBytes() / units / 10;
    for (int i = 0; i < units; i++) {
      DynamoReadFilterSpec sub = getOrQuery.get(i);
      work.add(new DynamoGetOrQueryWork(i, sub, portion));
    }
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    List<DynamoWork> segments = assignments.get(minorFragmentId);
    List<DynamoSubReadSpec> subSpecs = segments.stream()
                                               .map(work -> work.getWork(getColumns()))
                                               .collect(Collectors.toList());
    return new DynamoSubScan(plugin, plugin.getConfig(), subSpecs, this.columns, client, scan,
      spec.getTable());
  }

  @Override
  public int getMaxParallelizationWidth() {
    return Math.min(scan.getMaxSegments(), MAX_DYNAMO_PARALLELIZATION);
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public ScanStats getScanStats() {
    // for simple scans, we have to look at all the rows
    ScanStats.GroupScanProperty rowsCountable = ScanStats.GroupScanProperty.EXACT_ROW_COUNT;
    long recordCount = desc.getItemCount();
    // guess the disk costs.
    //  1. scan  - reads everything
    //  2. query - reads hash/sort + filter
    //  3. get - reads hash/sort, but no filter
    float cpuCost = 0;
    float diskCost = 100f;
    if (this.getSpec().getGetOrQuery() != null) {
      rowsCountable = ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT;
      int getCount = this.getSpec().getGetOrQuery().stream()
                         .mapToInt(spec -> spec instanceof DynamoGetFilterSpec ? 1 : 0)
                         .sum();
      // its all queries
      if (getCount == 0) {
        // queries return a fraction of the data, lets guess 10x reduction
        recordCount = recordCount / 10;
        diskCost = diskCost / 10;
      } else {
        // count the gets as the cost
        recordCount = getCount;
        diskCost = getCount / recordCount;
      }
    }
    return new ScanStats(rowsCountable, recordCount == 0 ? 1 : recordCount, cpuCost, diskCost);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    DynamoGroupScan scan = new DynamoGroupScan(this);
    scan.columns = columns;
    return scan;
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new DynamoGroupScan(this);
  }

  @JsonProperty
  public DynamoGroupScanSpec getSpec() {
    return spec;
  }

  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getConfig() {
    return config;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "DynamoGroupScan{" +
           "spec=" + spec +
           ", columns=" + columns +
           '}';
  }

  public boolean isFilterPushedDown() {
    return filterPushedDown;
  }

  public void setFilterPushedDown(boolean filterPushedDown) {
    this.filterPushedDown = filterPushedDown;
  }

  public void setScanSpec(DynamoGroupScanSpec scanSpec) {
    this.spec = scanSpec;
  }

  private abstract class DynamoWork implements CompleteWork {
    private final long bytes;
    private EndpointByteMapImpl byteMap = new EndpointByteMapImpl();
    private int segment;

    public DynamoWork(int segment, long bytes) {
      this.segment = segment;
      this.bytes = bytes;
    }

    public int getSegment() {
      return segment;
    }

    @Override
    public long getTotalBytes() {
      return bytes;
    }

    @Override
    public EndpointByteMap getByteMap() {
      return byteMap;
    }

    @Override
    public int compareTo(CompleteWork o) {
      if (o instanceof DynamoWork) {
        return Integer.compare(this.segment, ((DynamoWork) o).getSegment());
      }
      return -1;
    }

    public abstract DynamoSubReadSpec getWork(List<SchemaPath> columns);
  }

  private class DynamoScanWork extends DynamoWork {

    private final int total;
    private final DynamoReadFilterSpec filter;

    public DynamoScanWork(int totalSegments, int segment, long bytes,
      DynamoReadFilterSpec filter) {
      super(segment, bytes);
      this.total = totalSegments;
      this.filter = filter;
    }

    @Override
    public DynamoSubReadSpec getWork(List<SchemaPath> columns) {
      return new DynamoSubScanSpec(filter, total, getSegment(), columns);
    }
  }

  private class DynamoGetOrQueryWork extends DynamoWork {
    private final DynamoReadFilterSpec sub;

    public DynamoGetOrQueryWork(int segment,
      DynamoReadFilterSpec sub, long portion) {
      super(segment, portion);
      this.sub = sub;
    }

    @Override
    public DynamoSubReadSpec getWork(List<SchemaPath> columns) {
      if (sub instanceof DynamoGetFilterSpec) {
        return new DynamoSubGetSpec(sub, columns);
      }
      return new DynamoSubQuerySpec(sub, columns);
    }
  }
}
