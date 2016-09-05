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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapper;
import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapperSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubReadSpec;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubScanSpec;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.AddOrGetResult;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.Decimal38SparseVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableDecimal38SparseVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.RepeatedBitVector;
import org.apache.drill.exec.vector.RepeatedDecimal38SparseVector;
import org.apache.drill.exec.vector.RepeatedVarBinaryVector;
import org.apache.drill.exec.vector.RepeatedVarCharVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.VectorDescriptor;
import org.apache.drill.exec.vector.ZeroVector;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.System.arraycopy;
import static org.apache.drill.common.types.TypeProtos.MajorType;
import static org.apache.drill.common.types.TypeProtos.MinorType;
import static org.apache.drill.common.types.TypeProtos.MinorType.VARCHAR;

/**
 * Actually do the get/query/scan based on the {@link DynamoSubScanSpec}.
 */
public abstract class DynamoRecordReader<T extends DynamoSubReadSpec> extends AbstractRecordReader {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoRecordReader.class);
  private static final Joiner DOTS = Joiner.on('.');
  static final Joiner COMMAS = Joiner.on(", ");
  // max depth specified by AWS. See:
  // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions
  // .AccessingItemAttributes.html#DocumentPaths
  private static final int AMAZON_MAX_DEPTH = 32;

  private final Multimap<String, Integer> listIndexes = ArrayListMultimap.create();
  private final StackState topLevelState = new StackState(this::getField);

  private final AWSCredentialsProvider credentials;
  private final ClientConfiguration clientConf;
  protected final T scanSlice;
  private final boolean consistentRead;
  private final DynamoEndpoint endpoint;
  private final DynamoTableDefinition tableDef;
  private OutputMutator outputMutator;
  private OperatorContext operatorContext;
  private AmazonDynamoDBAsyncClient client;

  private Iterator<Page<Item, ?>> resultIter;
  private DynamoKeyMapper keyMapper;
  private DynamoTableDefinition.PrimaryKey hash;
  private DynamoTableDefinition.PrimaryKey range;

  public DynamoRecordReader(AWSCredentialsProvider credentials, ClientConfiguration clientConf,
    DynamoEndpoint endpoint, T scanSpec, List<SchemaPath> columns, boolean consistentRead,
    DynamoTableDefinition tableDef) {
    this.credentials = credentials;
    this.clientConf = clientConf;
    this.scanSlice = scanSpec;
    this.consistentRead = consistentRead;
    this.endpoint = endpoint;
    this.tableDef = tableDef;
    setColumns(columns);
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.operatorContext = context;
    this.outputMutator = output;

    this.client = new AmazonDynamoDBAsyncClient(credentials, this.clientConf);
    endpoint.configure(this.client);

    // setup the vectors that we know we will need - the primary key(s)
    if (this.keyMapper != null) {
      for (DynamoTableDefinition.PrimaryKey pk : tableDef.getKeys()) {
        if (pk.getIsHashKey()) {
          this.hash = pk;
        } else {
          this.range = pk;
        }
      }

      DynamoKeyMapperSpec spec = tableDef.getKeyMapper();
      for (int i = 0; i < spec.getKeyNames().size(); i++) {
        addDynamoKeyVector(spec.getKeyNames().get(i), spec.getKeyValues().get(i));
      }

    } else {
      for (DynamoTableDefinition.PrimaryKey pk : tableDef.getKeys()) {
        addDynamoKeyVector(pk.getName(), pk.getType());
      }
    }

    DynamoQueryBuilder builder = new DynamoQueryBuilder()
      .withSlice(scanSlice)
      .withConsistentRead(consistentRead)
      .withTable(tableDef);
    // TODO skip queries, which just want the primary key values
    if (!isStarQuery()) {
      List<String> columns = new ArrayList<>();
      for (SchemaPath column : getColumns()) {
        columns.add(buildPathName(column, AMAZON_MAX_DEPTH, listIndexes, true));
      }
      builder.withColumns(columns);
    }

    this.resultIter = buildQuery(builder, client);
  }

  private void addDynamoKeyVector(String name, String type) {
    // pk has to be a scalar type, so we never get null here
    MinorType minor = translateDynamoToDrillType(type);
    MajorType major = required(minor);
    MaterializedField field = MaterializedField.create(name, major);
    Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(minor, major.getMode());
    ValueVector vv = getField(field, clazz);
    topLevelState.scalars.put(name, new ScalarVectorStruct(vv));
  }

  protected abstract Iterator<Page<Item, ?>> buildQuery(DynamoQueryBuilder builder,
    AmazonDynamoDBAsyncClient client);

  private String buildPathName(SchemaPath column, int maxDepth, Multimap<String, Integer>
    listIndexes, boolean includeArrayIndex) {
    // build the full name of the column
    List<String> parts = new ArrayList<>();
    PathSegment.NameSegment name = column.getRootSegment();
    PathSegment seg = name;
    parts.add(name.getPath());
    int i = 0;
    while (((seg = seg.getChild()) != null) && (i++ < maxDepth)) {
      if (seg.isArray()) {
        int index = seg.getArraySegment().getIndex();
        String fullListName = DOTS.join(parts);
        String listLeafName = parts.remove(parts.size() - 1);
        String part = listLeafName;
        if (includeArrayIndex) {
          part += "[" + index + "]";
        }
        parts.add(part);
        if (listIndexes != null) {
          listIndexes.put(fullListName, index);
        }
      } else {
        parts.add(seg.getNameSegment().getPath());
      }
    }
    return DOTS.join(parts);
  }

  private MinorType translateDynamoToDrillType(String type) {
    switch (type) {
      // Scalar types
      case "S":
        return VARCHAR;
      case "N":
        return VARCHAR;
      // TODO replace with decimal38 when we have the correct impl working
      // return MinorType.DECIMAL38SPARSE;
      case "B":
        return MinorType.VARBINARY;
      case "BOOL":
        return MinorType.BIT;
    }
    throw new IllegalArgumentException("Don't know how to translate type: " + type);
  }

  public void setKeyMapper(DynamoKeyMapper keyMapper) {
    this.keyMapper = keyMapper;
  }

  @Override
  public int next() {
    Stopwatch watch = Stopwatch.createStarted();
    topLevelState.reset();

    int count = 0;
    Page<Item, ?> page;
    while (resultIter.hasNext()) {
      page = resultIter.next();
      for (Item item : page) {
        int rowCount = count++;
        for (Map.Entry<String, Object> attribute : item.attributes()) {
          String name = attribute.getKey();
          Object value = attribute.getValue();
          SchemaPath column = getSchemaPath(name);
          topLevelState.setColumn(column);
          handleTopField(rowCount, name, value, topLevelState);
        }
      }
    }

    topLevelState.setRowCount(count);
    LOG.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), count);
    return count;
  }

  private SchemaPath getSchemaPath(String name) {
    if (this.isStarQuery()) {
      return null;
    }
    for (SchemaPath column : getColumns()) {
      if (column.getRootSegment().getPath().equals(name)) {
        return column;
      }
    }
    return null;
  }

  private void handleTopField(int rowCount, String name, Object value, StackState state) {
    if (keyMapper != null) {
      Map<String, Object> parts = null;
      if (name.equals(hash.getName())) {
        parts = keyMapper.mapHashKey(value);
      } else if (range != null && name.equals(range.getName())) {
        parts = keyMapper.mapSortKey(value);
      }

      if (parts != null) {
        for (Map.Entry<String, Object> part : parts.entrySet()) {
          handleField(rowCount, part.getKey(), part.getValue(), state);
        }
      }
    }

    handleField(rowCount, name, value, state);
  }

  private void handleField(int rowCount, String name, Object value, StackState state) {
    // special handling for non-scalar types
    if (value instanceof Map) {
      handleMap(rowCount, name, value, state);
    } else if (value instanceof List || value instanceof Set) {
      handleList(rowCount, name, value, state);
    } else {
      writeScalar(rowCount, name, value, state);
    }
  }

  private void handleMap(int row, String name, Object value, StackState state) {
    MapVectorStruct map = state.mapVectors.get(name);
    if (map == null) {
      MaterializedField field = MaterializedField.create(name, optional(MinorType.MAP));
      MapVector vector = (MapVector) state.vectorFunc.apply(field, MapVector.class);
      vector.allocateNew();
      map = new MapVectorStruct(vector);
      state.mapVectors.put(name, map);
    }

    // new level of the stack, make a new state
    state = state.next(map.getVectorFun());

    Map<String, Object> values = (Map<String, Object>) value;
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      String fieldName = entry.getKey();
      Object fieldValue = entry.getValue();
      handleField(row, fieldName, fieldValue, state);
    }
  }

  /**
   * Sets and lists handled the same - as a repeated list of values
   */
  @SuppressWarnings("unchecked")
  private void handleList(int index, String name, Object value, StackState state) {

    // create the struct, if necessary
    ListVectorStruct struct = state.listVectors.get(name);
    if (struct == null) {
      MaterializedField field =
        MaterializedField.create(name, optional(MinorType.LIST));
      ListVector listVector = (ListVector) state.vectorFunc.apply(field, ListVector.class);
      listVector.allocateNew();
      struct = new ListVectorStruct(listVector);
      state.listVectors.put(name, struct);
    }

    // find the first item in the list
    List<Object> values;
    if (value instanceof List) {
      values = (List<Object>) value;
    } else {
      values = newArrayList((Collection<Object>) value);
    }

    // adjust the actual list contents to fill in 'null' values. Has to come after we figure out
    // the minor type so we can generate the right value vector
    values = adjustListValueForPointSelections(values, state);

    // there is only ever one vector that we use in a list, and its keyed by null
    String vectorName = "_0list_name";

    // move to the next state binding whatever gets created to this list
    ListVector list = struct.getVector();
    UInt4Vector offsets = list.getOffsetVector();
    int nextOffset = offsets.getAccessor().get(index);
    list.getMutator().setNotNull(index);
    int listIndex = 0;

    state = state.next(struct.getVectorFun());
    // ensure that we use the exact same vector each time and don't try to initialize it again
    ValueVector data = list.getDataVector();
    if (data != null && data != ZeroVector.INSTANCE) {
      if (data instanceof MapVector) {
        state.mapVectors.put(vectorName, new MapVectorStruct((MapVector) data));
      } else if (data instanceof ListVector) {
        state.listVectors.put(vectorName, new ListVectorStruct((ListVector) data));
      } else {
        state.scalars.put(vectorName, new ScalarVectorStruct(data));
      }
    }

    for (Object val : values) {
      // needed to fill the empty location. This would probably be cleaner to reason about by
      // matching up the sorted indexes and the values, but simpler to reason about this way.
      if (val != null) {
        // we should never need to create a sub-vector immediately - we created the vector above -
        // so send 'null' as the creator function.
        handleField(nextOffset + listIndex, vectorName, val, state);
      }
      listIndex++;
    }
    // mark down in the vector how far we got
    data = list.getDataVector();
    data.getMutator().setValueCount(data.getAccessor().getValueCount() + listIndex);
    // mark down how far into the vector we got this time
    offsets.getMutator().setSafe(index + 1, nextOffset + listIndex);
  }

  /**
   * Sometimes we might want to select an offset into a list. Dynamo handles this as returning
   * a smaller list. Drill expects the whole list to be there, so we need to fill in the other
   * parts of the list with nulls, up to the largest value
   */
  private List<Object> adjustListValueForPointSelections(List<Object> value,
    StackState state) {
    if (isStarQuery()) {
      return value;
    }
    String name = buildPathName(state.column, state.columnPathIndex + 1, null, false);
    Collection<Integer> indexes = listIndexes.get(name);
    if (indexes == null || indexes.size() == 0) {
      return value;
    }
    List<Integer> sorted = newArrayList(indexes);
    Collections.sort(sorted);
    int max = sorted.get(sorted.size() - 1);
    List<Object> valueOut = new ArrayList<>(max);
    int last = 0;
    for (Integer index : sorted) {
      // fill in nulls between the requested indexes
      for (int i = last; i < index; i++) {
        valueOut.add(null);
      }
      valueOut.add(value.remove(0));
      last++;
    }

    return valueOut;
  }

  private void writeScalar(int index, String column, Object columnValue, StackState state) {
    ScalarVectorStruct struct = state.scalars.get(column);
    if (struct == null) {
      MinorType minor = getMinorType(columnValue);
      MajorType type = optional(minor);
      MaterializedField field = MaterializedField.create(column, type);
      Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(minor, type.getMode());
      ValueVector vv = state.vectorFunc.apply(field, clazz);
      vv.allocateNew();
      struct = new ScalarVectorStruct(vv);
      state.scalars.put(column, struct);
    }
    // TODO remove when "properly" filling Decimal38
    if (columnValue instanceof BigDecimal) {
      columnValue = ((BigDecimal) columnValue).toPlainString();
    }
    writeScalar(index, columnValue, struct);
  }

  public MajorType optional(MinorType minor) {
    return setDecimal38ScaleIfNecessary(minor, Types.optional(minor));
  }

  public MajorType required(MinorType minor) {
    return setDecimal38ScaleIfNecessary(minor, Types.required(minor));
  }

  public MajorType repeated(MinorType minor) {
    return setDecimal38ScaleIfNecessary(minor, Types.repeated(minor));
  }

  private MajorType setDecimal38ScaleIfNecessary(MinorType minor, MajorType major) {
    if (minor == MinorType.DECIMAL38SPARSE) {
      major = MajorType.newBuilder(major).setScale(38).build();
    }
    return major;
  }

  private <T extends ValueVector> T getField(MaterializedField field, Class<T> clazz) {
    try {
      return outputMutator.addField(field, clazz);
    } catch (SchemaChangeException e) {
      throw new DrillRuntimeException("Failed to create vector for field: " + field.getName(), e);
    }
  }

  private void writeScalar(int index, Object value, ScalarVectorStruct struct) {
    if (value == null) {
      return;
    }
    MajorType type = struct.getType();
    MinorType minor = type.getMinorType();
    ValueVector vector = struct.getVector();
    type:
    switch (minor) {
      case VARCHAR:
        byte[] bytes = ((String) value).getBytes();
        switch (type.getMode()) {
          case OPTIONAL:
            ((NullableVarCharVector.Mutator) vector.getMutator()).setSafe(index, bytes,
              0, bytes.length);
            break type;
          case REQUIRED:
            ((VarCharVector.Mutator) vector.getMutator()).setSafe(index, bytes);
            break type;
          case REPEATED:
            ((RepeatedVarCharVector.Mutator) vector.getMutator()).addSafe(index, bytes);
            break type;
          default:
            failForMode(type);
        }
      case BIT:
        int bool = (Boolean) value ? 1 : 0;
        switch (type.getMode()) {
          case OPTIONAL:
            NullableBitVector bv = (NullableBitVector) vector;
            bv.getMutator().setSafe(index, bool);
            break type;
          case REQUIRED:
            ((BitVector.Mutator) vector.getMutator()).setSafe(index, bool);
            break type;
          case REPEATED:
            ((RepeatedBitVector.Mutator) vector.getMutator()).addSafe(index, bool);
            break type;
          default:
            failForMode(type);
        }
      case DECIMAL38SPARSE:
        //TODO fix this logic. This is just... wrong, but we get around it by just returning a
        // string representation of the value
        BigDecimal decimal = (BigDecimal) value;
        BigInteger intVal = decimal.unscaledValue();
        String zeros = Joiner.on("").join(Iterators.limit(Iterators.cycle("0"), decimal.scale()));
        BigInteger base = new BigInteger(1 + zeros);
        intVal = intVal.multiply(base);

        byte[] sparseInt = new byte[24];
        byte[] intBytes = intVal.toByteArray();
        arraycopy(intBytes, 0, sparseInt, 0, intBytes.length);
        // kind of an ugly way to manage the actual transfer of bytes. However, this is much
        // easier than trying to manage a larger page of bytes.
        Decimal38SparseHolder holder = new Decimal38SparseHolder();
        holder.start = 0;
        holder.buffer = operatorContext.getManagedBuffer(sparseInt.length);
        holder.buffer.setBytes(0, sparseInt);
        holder.precision = decimal.precision();
        holder.scale = decimal.scale();
        switch (type.getMode()) {
          case OPTIONAL:
            ((NullableDecimal38SparseVector.Mutator) vector.getMutator()).setSafe(index, holder);
            break type;
          case REQUIRED:
            ((Decimal38SparseVector.Mutator) vector.getMutator()).setSafe(index, holder);
            break type;
          case REPEATED:
            ((RepeatedDecimal38SparseVector.Mutator) vector.getMutator()).addSafe(index, holder);
            break type;
          default:
            failForMode(type);
        }
      case VARBINARY:
        byte[] bytesBinary = (byte[]) value;
        switch (type.getMode()) {
          case OPTIONAL:
            ((NullableVarBinaryVector.Mutator) vector.getMutator()).setSafe(index,
              bytesBinary, 0, bytesBinary.length);
            break type;
          case REQUIRED:
            ((VarBinaryVector.Mutator) vector.getMutator()).setSafe(index, bytesBinary);
            break type;
          case REPEATED:
            ((RepeatedVarBinaryVector.Mutator) vector.getMutator()).addSafe(index, bytesBinary);
            break type;
          default:
            failForMode(type);
        }
      case NULL:
        System.out.println();
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  private void failForMode(MajorType type) {
    throw new IllegalArgumentException(
      "Mode: " + type.getMode() + " not " + "supported for scalar type: " + type);
  }

  private MinorType getMinorType(Object value) {
    if (value instanceof String) {
      return VARCHAR;
    } else if (value instanceof Boolean) {
      return MinorType.BIT;
    } else if (value instanceof BigDecimal) {
      return MinorType.VARCHAR;
//      return MinorType.DECIMAL38SPARSE;
    } else if (value instanceof byte[]) {
      return MinorType.VARBINARY;
    } else if (value instanceof Map) {
      return MinorType.MAP;
    } else if (value instanceof List) {
      return MinorType.LIST;
    } else if (value == null) {
      return MinorType.VARCHAR;
    }
    throw new UnsupportedOperationException("Unexpected type for: " + value);
  }

  @Override
  public void close() throws Exception {
    this.client.shutdown();
  }

  // Struct for a logical ValueVector
  private abstract class ValueVectorStruct<T extends ValueVector> {
    private final T vector;

    ValueVectorStruct(T vector) {
      this.vector = vector;
    }

    T getVector() {
      return vector;
    }

    public void reset() {
      this.getVector().clear();
      this.getVector().allocateNew();
    }

    void setValueCount(int valueCount) {
      this.getVector().getMutator().setValueCount(valueCount);
    }
  }

  private class ScalarVectorStruct extends ValueVectorStruct<ValueVector> {

    ScalarVectorStruct(ValueVector vector) {
      super(vector);
    }

    MajorType getType() {
      return getVector().getField().getType();
    }
  }

  private abstract class NestedVectorStruct<T extends ValueVector> extends ValueVectorStruct<T> {

    private final BiFunction<MaterializedField, Class<? extends ValueVector>, ValueVector>
      vectorFun;
    private Map<String, ScalarVectorStruct> scalarStructs = new HashMap<>();
    private Map<String, ListVectorStruct> listStructs = new HashMap<>();
    private Map<String, MapVectorStruct> mapStructs = new HashMap<>();

    NestedVectorStruct(T vector, BiFunction<MaterializedField,
      Class<? extends ValueVector>, ValueVector> vectorFunc) {
      super(vector);
      this.vectorFun = vectorFunc;
    }

    Map<String, ListVectorStruct> getListStructs() {
      return listStructs;
    }

    Map<String, MapVectorStruct> getMapStructs() {
      return mapStructs;
    }

    Map<String, ScalarVectorStruct> getScalarStructs() {
      return scalarStructs;
    }

    BiFunction<MaterializedField, Class<? extends ValueVector>, ValueVector> getVectorFun() {
      return vectorFun;
    }

    void clear() {
      this.getVector().clear();
    }

    @Override
    public void reset() {
      super.reset();
      this.listStructs.values().forEach(NestedVectorStruct::clear);
      this.listStructs.clear();
      this.mapStructs.values().forEach(NestedVectorStruct::clear);
      this.mapStructs.clear();
      this.scalarStructs.values().forEach(s -> s.getVector().clear());
    }
  }

  private class MapVectorStruct extends NestedVectorStruct<MapVector> {

    MapVectorStruct(MapVector vector) {
      super(vector, (field, clazz) -> vector.addOrGet(field.getName(), field.getType(), clazz));
    }
  }

  private class ListVectorStruct extends NestedVectorStruct<ListVector> {

    ListVectorStruct(final ListVector vector) {
      super(vector, (field, clazz) -> {
        AddOrGetResult result = vector.addOrGetVector(new VectorDescriptor(field));
        return result.getVector();
      });
    }
  }

  private class StackState {
    private final BiFunction<MaterializedField, Class<? extends ValueVector>, ValueVector>
      vectorFunc;
    // assume we are not deeply nested
    private Map<String, ScalarVectorStruct> scalars = new HashMap<>(0);
    private Map<String, MapVectorStruct> mapVectors = new HashMap<>(0);
    private Map<String, ListVectorStruct> listVectors = new HashMap<>(0);
    private SchemaPath column;
    private int columnPathIndex;

    public StackState(BiFunction<MaterializedField,
      Class<? extends ValueVector>, ValueVector> vectorFunc) {
      this.vectorFunc = vectorFunc;
    }

    public void reset() {
      scalars.values().stream().forEach(ValueVectorStruct::reset);
      mapVectors.values().stream().forEach(NestedVectorStruct::reset);
      listVectors.values().stream().forEach(NestedVectorStruct::reset);
    }

    public void setRowCount(int rows) {
      scalars.values().stream().forEach(scalar -> scalar.setValueCount(rows));

      // varchar vectors get messed up if you try to set 0 values, so we just skip it if there was
      // no data
      if (rows > 0) {
        mapVectors.values().stream().forEach(map -> map.setValueCount(rows));
        listVectors.values().stream().forEach(list -> list.setValueCount(rows));
      }
    }

    public void setColumn(SchemaPath column) {
      this.column = column;
      this.columnPathIndex = 0;
    }

    public StackState next(
      BiFunction<MaterializedField, Class<? extends ValueVector>, ValueVector> vectorFun) {
      StackState state = new StackState(vectorFun);
      state.setColumn(this.column);
      state.columnPathIndex = this.columnPathIndex++;
      return state;
    }
  }
}
