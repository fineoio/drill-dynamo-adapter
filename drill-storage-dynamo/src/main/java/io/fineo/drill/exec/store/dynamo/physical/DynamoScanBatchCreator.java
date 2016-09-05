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

package io.fineo.drill.exec.store.dynamo.physical;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.fineo.drill.exec.store.dynamo.config.ClientProperties;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapper;
import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapperSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubGetSpec;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubQuerySpec;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubReadSpec;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubScan;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubScanSpec;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;

import java.io.IOException;
import java.util.List;
import java.util.Map;

// Created by reflection by drill to match the DynamoSubScan
@SuppressWarnings("unused")
public class DynamoScanBatchCreator implements BatchCreator<DynamoSubScan> {
  @Override
  public CloseableRecordBatch getBatch(FragmentContext context, DynamoSubScan subScan,
    List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());

    List<RecordReader> readers = Lists.newArrayList();
    List<SchemaPath> columns = subScan.getColumns();
    ClientProperties clientProps = subScan.getClient();
    ClientConfiguration client = clientProps.getConfiguration();
    AWSCredentialsProvider credentials = subScan.getCredentials();
    DynamoEndpoint endpoint = subScan.getEndpoint();
    DynamoTableDefinition table = subScan.getTable();
    DynamoKeyMapper key = null;
    if (table.getKeyMapper() != null) {
      DynamoKeyMapperSpec spec = table.getKeyMapper();
      Map<String, Object> args = spec.getArgs();
      ObjectMapper mapper = new ObjectMapper();
      try {
        String argString = mapper.writeValueAsString(args);
        InjectableValues inject = new InjectableValues.Std()
          .addValue(DynamoKeyMapperSpec.class, spec);
        key =
          mapper.setInjectableValues(inject).readValue(argString, DynamoKeyMapper.class);
      } catch (IOException e) {
        throw new ExecutionSetupException(e);
      }
    }

    for (DynamoSubReadSpec scanSpec : subScan.getSpecs()) {
      try {
        DynamoRecordReader reader;
        if (scanSpec instanceof DynamoSubGetSpec) {
          reader = new DynamoGetRecordReader(credentials, client, endpoint,
            (DynamoSubGetSpec) scanSpec, columns,
            clientProps.getConsistentRead(), table);
        } else if (scanSpec instanceof DynamoSubQuerySpec) {
          reader = new DynamoQueryRecordReader(credentials, client, endpoint,
            (DynamoSubQuerySpec) scanSpec, columns,
            clientProps.getConsistentRead(), subScan.getScanProps(), table);
        } else {
          reader = new DynamoScanRecordReader(credentials, client, endpoint,
            (DynamoSubScanSpec) scanSpec, columns,
            clientProps.getConsistentRead(), subScan.getScanProps(), table);
        }
        if (key != null) {
          reader.setKeyMapper(key);
        }
        readers.add(reader);
      } catch (Exception e1) {
        throw new ExecutionSetupException(e1);
      }
    }
    return new ScanBatch(subScan, context, readers.iterator());
  }
}
