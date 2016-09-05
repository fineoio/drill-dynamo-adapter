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

package io.fineo.drill.exec.store.dynamo.filter;

import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoGetFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGroupScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoQueryFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import io.fineo.drill.exec.store.dynamo.spec.filter.FilterTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.drill.exec.store.dynamo.spec.filter.FilterTree.FilterLeaf;
import static io.fineo.drill.exec.store.dynamo.spec.filter.FilterTree.FilterNode;
import static io.fineo.drill.exec.store.dynamo.spec.filter.FilterTree.FilterNodeInner;

/**
 * Build the various forms of 'reads' from Dynamo. Currently makes the following preferences:
 * <ol>
 * <li>Get + Attribute -> Query</li>
 * </ol>
 * Generally, this is how Dynamo defines build each of the different read types and we try to
 * stay as faithful as possible to this:
 * <ol>
 * <li>Get: requires hash key and sort key (if the table has a sort key)
 * <ul>
 * <li>Only supports equals on the hash and semi-optional sort key</li>
 * <li>Cannot handle attribute filters</li>
 * </ul>
 * </li>
 * <li>Query: requires hash key, optional sort key (regardless of table setup)
 * <ul>
 * <li>Hash key must be '='</li>
 * <li>Sort key can be any form of equality check or a BETWEEN statement</li>
 * <li>Up to (and no more than) 1 sort key allowed per Query</li>
 * </ul></li>
 * <li>Scan: reads the entire table and should be avoided at all costs
 * <ul>
 * <li>No key filtering, all filters are handled together</li>
 * <li>All data is read then filtered, so its really rough on read throughput limits</li>
 * </ul>
 * </li>
 * </ol>
 */
class DynamoReadBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoReadBuilder.class);

  private enum CASE {
    UNSET,
    AND,
    OR;
  }

  private final DynamoTableDefinition.PrimaryKey hashKey;
  private final DynamoTableDefinition.PrimaryKey rangeKey;
  private final boolean rangeKeyExists;
  private final List<GetOrQuery> queries = new ArrayList<>();

  private Scan scan;

  private FilterFragment nextHash;
  private FilterFragment nextRange;
  private DynamoFilterSpec nextAttribute;
  private CASE AND_OR = CASE.UNSET;

  // assumed to handle the filter entirely. This doesn't occur when we have multiple range
  // conditions, but a single hash condition to match, generating a series of point queries
  // joined on AND conditions. For instance, (s > 5 && s < 7) && h = 1 will generate:
  //  * q(h=1 && s > 5)
  //  * q(h=1 && s < 7)
  // We can't combine this into a single query with dynamo, so we have to rely on the drill
  // filter to resolve this. However, for the above case, you should just use BETWEEN
  private boolean handledFilter = true;

  DynamoReadBuilder(DynamoTableDefinition.PrimaryKey hash, DynamoTableDefinition.PrimaryKey range) {
    this.hashKey = hash;
    this.rangeKey = range;
    this.rangeKeyExists = range != null;
  }

  /**
   * Non-typed setting of the fragment. This should only be used the first time
   *
   * @param fragment to set
   */
  public void set(FilterFragment fragment) {
    assert AND_OR == CASE.UNSET;
    assert queries.size() == 0;
    assert nextHash == null;
    assert nextRange == null;
    assert nextAttribute == null;
    if (fragment.isHash()) {
      nextHash = fragment;
      if (!this.rangeKeyExists) {
        createGetOrQuery();
      }
    } else if (fragment.isRange()) {
      nextRange = fragment;
    } else {
      assert fragment.isAttribute();
      nextAttribute = fragment.getFilter();
    }
  }

  private void and() {
    AND_OR = CASE.AND;
  }

  private void or() {
    AND_OR = CASE.OR;
  }

  /**
   * Scan & (anything) -> Scan
   * <p>
   * (Get/Query ...) && (Get/Query...) -> Scan, degenerate case of hash && hash -> scan
   * <p>
   * (Get/Query ...) && hash -> scan<br/>
   * (Get/Query ...) && sort -> update latest<br/>
   * (Get/Query ...) && attr -> update latest<br/>
   */
  public void and(DynamoReadBuilder that) {
    if (that == null) {
      return;
    }
    if (!that.handledFilter()) {
      this.handledFilter = false;
    }
    and();

    // if either are scans already, then just combine them
    if (this.scan != null || that.scan != null) {
      andScan(that);
      return;
    }

    boolean thisQueries = this.queries.size() > 0;
    boolean thatQueries = that.queries.size() > 0;

    // both have queries, so we need to make a scan
    if (thisQueries && thatQueries) {
      andScan(that);
      return;
    } else if (thatQueries) {
      // queries from them, but no queries from us
      this.queries.addAll(that.queries);
    }

    // try to add the attributes
    tryAndNextAttributes(that);
    // oops, it became a scan. Add the attributes that are hang around
    if (this.scan != null) {
      this.scan = buildScan();
    }
  }

  private void tryAndNextAttributes(DynamoReadBuilder build) {
    this.and(build.nextHash);
    this.and(build.nextRange);
    this.andAttribute(build.nextAttribute);
  }

  private void andScan(DynamoReadBuilder build) {
    Scan bs = build.buildScan();
    this.scan = buildScan();
    scan.and(bs);
  }

  /**
   * Scan || anything -> scan
   * <p>
   * (get/query...) || (get/query...) -> (get/query...)
   * <p>
   * (get/query...) || hash -> get/query<br/>
   * (get/query...) || sort -> scan<br/>
   * (get/query...) || attr -> scan
   */
  public void or(DynamoReadBuilder that) {
    if (that == null) {
      return;
    }
    CASE prev = AND_OR;
    or();
    // if either are scans already, then just combine them
    if (this.scan != null || that.scan != null) {
      orScan(that);
      return;
    }

    boolean hadQueries = !this.queries.isEmpty();
    this.queries.addAll(that.queries);

    /*
     * Handle the leftovers. They have to be anded together, to make this not already a scan.
     *   hash && hash -> scan
     *   hash || hash -> first => get/query; second => could be get/query
     *
     *   hash && sort -> turns in get/query
     *   hash || sort -> scan
     *
     *   hash && attr -> could be query
     *   hash || attr -> scan
     *
     *   sort && sort => sort -> could be query
     *   sort || sort => sort -> could be query
     *
     *   sort && attr -> could be query
     *   sort || attr -> scan
     *
     *   queries && hash -> scan
     *   queries || hash -> could be query/get
     *
     *   queries && sort -> added to last query
     *   queries || sort -> scan
     *
     *   queries && attr -> added to last query
     *   queries || attr -> scan
     * We essentially need to handle the cases that don't automatically turn into scan/get/query.
     */
    if (!(that.nextHash == null && that.nextRange == null && that.nextAttribute == null)) {
      // has some queries, so only thing that could be left is "|| hash"
      if (that.queries.size() > 0) {
        assert that.AND_OR == CASE.OR : "Have previous queries but not a sort and CASE = AND";
        assert that.nextRange == null : "NextRange should have been merged into queries";
        assert that.nextAttribute == null : "NextAttr should have been merged into queries";
        assert that.nextHash != null : "Should only have nextHash as the not null value!";

        or(that.nextHash);
      } else {
        switch (that.AND_OR) {
          // there are no queries, so there could be more leftovers. What we do depends on the how
          // they were added
          case UNSET:
            // could be anything, so just OR the combination
            or(that.nextHash);
            or(that.nextRange);
            orAttribute(that.nextAttribute);
            break;
          // we have more than one thing set, so combine them appropriately
          case AND:
            // could be:
            //  1. hash && attr
            //  2. sort && attr
            if (that.nextHash != null) {
              assert that.nextRange == null;
              assert that.nextAttribute != null;
              add(new Query(that.nextHash.getFilter(), that.nextAttribute));
            } else {
              // AND has priority in evaluation, so we have to create a scan to support the
              // filter on the range and attribute
              assert this.AND_OR != CASE.UNSET : "No fragment operator in attempted merge OR";
              assert that.nextRange != null;
              assert that.nextAttribute != null;
              this.scan = buildScan();
              this.scan.or(that.nextRange.getFilter().and(that.nextAttribute));
            }
            break;
          case OR:
            // could be:
            //  1. sort
            //  2. hash
            assert that.nextAttribute == null;
            or(that.nextHash);
            or(that.nextRange);
        }
      }
    }

    if (this.scan != null) {
      this.scan = buildScan();
    }
  }

  private void orScan(DynamoReadBuilder that) {
    Scan thatScan = that.buildScan();
    this.scan = buildScan();
    scan.or(thatScan);
  }

  /**
   * hash && hash -> scan
   * <p>
   * hash && sort -> get/query, dependning on fragment.equals<br/>
   * sort && hash -> "    "
   * <p>
   * attr && hash -> anything<br/>
   * hash && attr -> anything
   * <p>
   * sort && attr -> anything<br/>
   * attr && sort -> anything
   * <p>
   * sort && sort -> query or scan
   */
  public void and(FilterFragment fragment) {
    if (fragment == null) {
      return;
    }
    and();
    // we have a scan or the fragment is an attribute
    if (shouldScan(fragment)) {
      andScan(fragment.getFilter());
      return;
    }

    if (fragment.isAttribute()) {
      andAttribute(fragment.getFilter());
      return;
    }

    // fragment is a hash key
    if (andHash(fragment)) {
      return;
    }

    andRange(fragment);
  }

  private boolean andHash(FilterFragment fragment) {
    if (!fragment.isHash()) {
      return false;
    }
    // if we have any gets/queries or another hash keey, AND on a hash key requires scanning
    // everything
    if (nextHash != null || queries.size() > 0) {
      if (nextHash != null) {
        LOG.warn("Two AND conditions on hash keys, must create a scan to cover them! Its "
                 + "unlikely this will ever return any valid data though, unless its "
                 + "something like 'h = 1 & h = 1', which is better served by a get, but we "
                 + "can't determine that without introspecting the query");
      }
      andScan(fragment.getFilter());
      return true;
    }

    this.nextHash = fragment;

    // there is no rangePrimaryKey key, we can immediately decide what to do about this part
    if (!rangeKeyExists) {
      createGetOrQuery();
      return true;
    }

    // we have a range key
    if (this.nextRange != null) {
      createGetOrQuery();
    }

    // there is no range key, so nothing more to do.
    return true;
  }

  private void andRange(FilterFragment fragment) {
    assert fragment.isRange() : "Supposed to have handled non-range filter fragments at this "
                                + "point! Fragment: " + fragment;
    // there is a hash, so it must be a scan (hash && sort)
    if (nextHash != null) {
      setRange(fragment, this::and);
      createGetOrQuery();
      return;
    }

    updateRange(fragment, this::and);
  }

  public void andScanSpec(DynamoReadFilterSpec scan) {
    if (this.scan == null) {
      this.scan = buildScan();
    }
    andScan(scan);
  }

  public void andGetOrQuery(List<DynamoReadFilterSpec> getOrQuery) {
    // AND with queries/gets automatically creates a scan
    // GET && GET (hash = 1 && hash = 2)
    if (this.scan == null) {
      this.scan = buildScan();
    }

    for (DynamoReadFilterSpec spec : getOrQuery) {
      andScan(spec);
    }
  }

  private void andScan(DynamoReadFilterSpec spec) {
    and(scan.spec, spec.getKey());
  }

  public DynamoGroupScanSpec buildSpec(DynamoTableDefinition def) {
    if (this.scan != null) {
      return outputScan(def);
    }
    // check to see if we have any hanging fragments that would change anything.
    if (this.nextHash != null) {
      createGetOrQuery();
    } else if (this.nextAttribute != null) {
      switch (AND_OR) {
        case UNSET:
          return outputScan(def);
        case AND:
          QueryList createOrUpdated = updateQueryWithAttribute(this.nextAttribute);
          if (createOrUpdated != null) {
            add(createOrUpdated);
            break;
          }
          // fall through to build a scan
        case OR:
          return outputScan(def);
      }
    } else if (this.nextRange != null) {
      return outputScan(def);
    }

    // no more hanging attributes
    Set<DynamoReadFilterSpec> queries = new HashSet<>();
    for (GetOrQuery gq : this.queries) {
      if (gq.get != null) {
        queries.add(new DynamoGetFilterSpec(gq.get.getFilter()));
      } else {
        DynamoFilterSpec attribute = gq.attribute();
        for (Query query : gq.query) {
          queries.add(new DynamoQueryFilterSpec(query.getFilter(), attribute));
        }
      }
    }

    return new DynamoGroupScanSpec(def, null, newArrayList(queries));
  }

  private DynamoGroupScanSpec outputScan(DynamoTableDefinition def) {
    this.scan = buildScan();
    DynamoReadFilterSpec scan = new DynamoReadFilterSpec(this.scan.spec);
    return new DynamoGroupScanSpec(def, scan, null);
  }

  @FunctionalInterface
  private interface VoidBiFunction<A, B> {
    void apply(A var1, B var2);
  }

  private void setRange(FilterFragment
    fragment, VoidBiFunction<DynamoFilterSpec, DynamoFilterSpec> func) {
    if (nextRange == null) {
      nextRange = fragment;
    } else {
      func.apply(nextRange.getFilter(), fragment.getFilter());
      // its now a multi-range request, so it cannot be an equals fragment
      nextRange.setEquals(false);
    }
  }

  private void andScan(DynamoFilterSpec spec) {
    if (scan == null) {
      scan = buildScan();
    }
    scan.and(spec);
  }

  private void andAttribute(DynamoFilterSpec spec) {
    if (spec == null) {
      return;
    }

    // see if we can combine this with the last query/get
    if (queries.size() > 0) {
      QueryList query = updateQueryWithAttribute(spec);
      add(query);
    } else {
      // nothing created yet, just combine attributes
      nextAttribute = and(nextAttribute, spec);
    }
  }

  private QueryList updateQueryWithAttribute(DynamoFilterSpec attribute) {
    if (queries.size() == 0) {
      return null;
    }
    GetOrQuery gq = queries.remove(queries.size() - 1);
    QueryList query = gq.query;
    // oops, actually created a get, to make a new query
    if (query == null) {
      query = new QueryList(new Query(gq.get.getFilter(), null));
    }

    query.setAttribute(and(query.attribute(), attribute));
    return query;
  }

  private void add(QueryList query) {
    add(new GetOrQuery(query));
  }

  private void add(Get get) {
    add(new GetOrQuery(get));
  }

  private void add(Query query) {
    add(new GetOrQuery(query));
  }

  private void add(GetOrQuery gq) {
    queries.add(gq);
    nextHash = null;
    nextRange = null;
    nextAttribute = null;
  }

  private DynamoFilterSpec and(DynamoFilterSpec nextAttribute, DynamoFilterSpec spec) {
    if (nextAttribute == null) {
      return spec;
    } else if (spec == null) {
      return nextAttribute;
    }
    return nextAttribute.and(spec);
  }

  /*
   * hash || sort -> scan<br/>
   * sort || hash -> scan
   * <p>
   * attr || hash -> scan<br/>
   * hash || attr -> scan
   * <p>
   * hash || hash -> get/query, depending on hash key<br/>
   * sort || sort -> scan or query, but equals = false<br/>
   * attr || attr -> attr
   * <p>
   * attr || sort -> scan<br/>
   * sort || attr -> scan
   */
  private void or(FilterFragment fragment) {
    if (fragment == null) {
      return;
    }
    or();
    // we have to read everything anyways, so just add this spec
    // OR checking a non-equality requires scan - either is an attribute or a non-equality key
    boolean isKey = fragment.isHash() || fragment.isRange();
    if (shouldScan(fragment)) {
      orScan(fragment.getFilter(), isKey);
      return;
    }

    if (fragment.isAttribute()) {
      orAttribute(fragment.getFilter());
      return;
    }

    if (fragment.isHash()) {
      if (nextRange != null || nextAttribute != null) {
        orScan(fragment.getFilter(), true);
        return;
      }

      if (nextHash != null) {
        // hash = '1' || hash = '2'
        // we only get here if there was no matching sort condition (which would enable a get),
        // so it has to be a query for the key
        if (rangeKeyExists) {
          createGetOrQuery();
        }
      }

      nextHash = fragment;
      if (!rangeKeyExists) {
        createGetOrQuery();
      }
      return;
    }

    assert fragment.isRange() : "Should have handled the fragment before here!";

    // hash = 1 || range = 2 OR attr = 'a' || range = 2
    if (nextHash != null || nextAttribute != null) {
      orScan(fragment.getFilter(), true);
      return;
    }

    updateRange(fragment, this::or);
  }

  /**
   * Gets cannot support a condition key, so we switch over to using a Query. This will evaluate
   * the attribute filter on the server side, but still reads the attribute; its better than
   * materializing the attribute and then sending its across the wire, transferring it to a buffer
   * and then filter on it above... I think.
   */
  private void createGetOrQuery() {
    assert nextHash != null : "Must at least have a nextHash specified when building Get/Query";
    DynamoFilterSpec range = nextRange == null ? null : nextRange.getFilter();
    boolean validRange = !rangeKeyExists || (nextRange != null && nextRange.isEquals());
    if (nextHash.isEquals() && validRange && nextAttribute == null) {
      DynamoFilterSpec key = and(nextHash.getFilter(), range);
      add(new Get(key));
    } else {
      if (range == null) {
        add(new Query(nextHash.getFilter(), nextAttribute));
      } else {
        QueryList list = new QueryList();
        list.setAttribute(nextAttribute);
        range.getTree().visit(new FilterTree.FilterNodeVisitor<Void>() {
          @Override
          public Void visitInnerNode(FilterNodeInner inner) {
            inner.getLeft().visit(this);
            inner.getRight().visit(this);
            // any AND condition means both sides need to be true, which we can't handle with a
            // single query, so we need to retain the parent filter
            if (inner.and()) {
              handledFilter = false;
            }
            return null;
          }

          @Override
          public Void visitLeafNode(FilterLeaf leaf) {
            DynamoFilterSpec copy = DynamoFilterSpec.copy(leaf);
            FilterNode root = nextHash.getFilter().getTree().getRoot();
            assert root instanceof FilterLeaf;
            DynamoFilterSpec hashCopy = nextHash.getFilter().copy((FilterLeaf) root);
            list.add(new Query(hashCopy.and(copy)));
            return null;
          }
        });

        add(list);
      }
    }
  }

  private void orAttribute(DynamoFilterSpec attr) {
    if (attr == null) {
      return;
    }
    if (queries.size() > 0 || nextHash != null || nextRange != null) {
      orScan(attr, false);
    } else {
      // just set the attribute
      if (this.nextAttribute == null) {
        this.nextAttribute = attr;
      } else {
        this.nextAttribute = this.nextAttribute.or(attr);
      }
    }
  }

  /**
   * Update the current range expression or add it to the previous query.
   */
  private void updateRange(FilterFragment
    fragment, VoidBiFunction<DynamoFilterSpec, DynamoFilterSpec> func) {
    if (this.queries.size() > 0) {
      GetOrQuery gq = queries.remove(queries.size() - 1);
      switch (AND_OR) {
        case UNSET:
          throw new UnsupportedOperationException("Should not be update range with an UNSET state");
        case AND:
          // turn the get into a set of queries
          QueryList list;
          if (gq.get != null) {
            list = new QueryList(new Query(gq.get.getFilter()));
            // create a new query with the pk condition
            list.add(new Query(and(cloneHashKeyFilter(gq.get), fragment.getFilter())));
            add(list);
          } else {
            // h = 1 && s = 2 && s = 3
            list = gq.query;
            Query query = list.list.get(0);
            DynamoFilterSpec hash = cloneHashKeyFilter(query);
            list.add(new Query(hash.and(fragment.getFilter())));
          }
          // AND means that we need to use the Drill filter handling to mange the case like
          //  h =1 && s < 5 AND h 1= && s < 2
          // where we cannot completely handle the filter
          add(list);
          handledFilter = false;
          return;
        case OR:
        default:
          // fall through to build a scan
          this.scan = buildScan();
      }
    }
    setRange(fragment, func);
  }

  private DynamoFilterSpec cloneHashKeyFilter(LeafQuerySpec leaf) {
    // find the hash key reference
    return leaf.getFilter().getTree().visit(new FilterTree.FilterNodeVisitor<DynamoFilterSpec>() {
      @Override
      public DynamoFilterSpec visitInnerNode(FilterNodeInner inner) {
        // any AND condition means both sides need to be true, which we can't handle with a
        // single query, so we need to retain the parent filter
        assert inner.and() : "Query should never have a not AND condition between hash and"
                             + " sort key";
        DynamoFilterSpec spec = inner.getLeft().visit(this);
        if (spec == null) {
          spec = inner.getRight().visit(this);
        }

        return spec;
      }

      @Override
      public DynamoFilterSpec visitLeafNode(FilterLeaf leaf) {
        return leaf.getKey().equals(hashKey.getName()) ?
               DynamoFilterSpec.copy(leaf) :
               null;
      }
    });
  }

  /**
   * Need to have a scan when:
   * <ul>
   * <li>Already have a scan</li>
   * <li>fragment is not checking equality AND not an attribute (is a hash or sort)</li>
   * <li>fragment is hash and not checking equals </li>
   * </ul>
   *
   * @param fragment to check
   * @return if a scan should be run
   */
  private boolean shouldScan(FilterFragment fragment) {
    return scan != null ||
           (!fragment.isAttribute() && !fragment.isEquality()) ||
           (fragment.isHash() && !fragment.isEquals());
  }

  private void orScan(DynamoFilterSpec spec, boolean isKey) {
    if (scan == null) {
      scan = buildScan();
    }
    scan.or(spec);
  }

  private DynamoFilterSpec or(DynamoFilterSpec nextAttribute, DynamoFilterSpec spec) {
    if (nextAttribute == null) {
      return spec;
    } else if (spec == null) {
      return nextAttribute;
    }
    return nextAttribute.or(spec);
  }

  // Take all the previous gets/queries and combine them into a scan
  private Scan buildScan() {
    Scan scan;
    if (this.scan != null) {
      scan = this.scan;
    } else {
      scan = new Scan();
    }
    for (GetOrQuery gq : queries) {
      if (gq.get != null) {
        scan.or(gq.get.getFilter());
      } else {
        QueryList queries = gq.query;
        Query query = queries.list.get(0);
        DynamoFilterSpec spec = query.getFilter();
        DynamoFilterSpec attr = query.attribute() != null ?
                                query.attribute :
                                queries.attribute();
        if (attr != null) {
          spec = and(spec, attr.deepClone());
        }
        for (int i = 1; i < queries.list.size(); i++) {
          query = queries.list.get(i);
          DynamoFilterSpec filter = attr == null ?
                                    query.getFilter() :
                                    and(attr.deepClone(), query.getFilter());
          spec = or(spec, filter);
        }
        scan.or(spec);
      }
    }
    queries.clear();

    // add the edge attributes.
    switch (AND_OR) {
      case UNSET:
      case AND:
        scan.and(nextHash);
        scan.and(nextRange);
        scan.and(nextAttribute);
        break;
      case OR:
        scan.or(nextHash);
        scan.or(nextRange);
        scan.or(nextAttribute);
    }
    nextHash = null;
    nextRange = null;
    nextAttribute = null;
    return scan;
  }

  public boolean handledFilter() {
    return this.handledFilter;
  }

  private class Scan {
    private DynamoFilterSpec spec;

    public void and(DynamoFilterSpec spec) {
      set(spec, DynamoReadBuilder.this::and);
    }

    public void or(DynamoFilterSpec spec) {
      set(spec, DynamoReadBuilder.this::or);
    }

    public void and(FilterFragment fragment) {
      if (fragment != null) {
        and(fragment.getFilter());
      }
    }

    public void or(FilterFragment fragment) {
      if (fragment != null) {
        or(fragment.getFilter());
      }
    }

    private void set(DynamoFilterSpec spec, BiFunction<DynamoFilterSpec,
      DynamoFilterSpec, DynamoFilterSpec> func) {
      if (spec == null) {
        return;
      }
      this.spec = func.apply(this.spec, spec);
    }

    public void and(Scan scan) {
      this.spec = DynamoReadBuilder.this.and(spec, scan.spec);
    }

    public void or(Scan thatScan) {
      this.spec = DynamoReadBuilder.this.or(spec, thatScan.spec);
    }
  }

  private static class LeafQuerySpec {
    private final DynamoFilterSpec filter;

    public LeafQuerySpec(DynamoFilterSpec filter) {
      this.filter = filter;
    }

    public DynamoFilterSpec getFilter() {
      return filter;
    }
  }

  private static class Get extends LeafQuerySpec {

    protected DynamoFilterSpec attribute;

    public Get(DynamoFilterSpec key) {
      super(key);
    }

    public DynamoFilterSpec attribute() {
      return attribute;
    }
  }

  private static class Query extends LeafQuerySpec {

    protected DynamoFilterSpec attribute;

    public Query(DynamoFilterSpec filter, DynamoFilterSpec attr) {
      super(filter);
      this.attribute = attr;
    }

    public Query(DynamoFilterSpec filterSpec) {
      super(filterSpec);
    }

    public void setAttribute(DynamoFilterSpec attribute) {
      this.attribute = attribute;
    }

    public DynamoFilterSpec attribute() {
      return attribute;
    }
  }

  private static class GetOrQuery {
    private Get get;
    private QueryList query;

    public GetOrQuery(Get get) {
      this.get = get;
    }

    public GetOrQuery(Query query) {
      this(new QueryList(query));
    }

    public GetOrQuery(QueryList query) {
      this.query = query;
    }

    public DynamoFilterSpec attribute() {
      return get != null ? get.attribute() : query.attribute();
    }
  }

  /**
   * List of queries that are logically bound togther. For instance:
   * (s1 || s2) && h -> q1(h & s1) || q1(h & s2)
   * Thus when we get an attribute, we need to update the entire list
   */
  private static class QueryList implements Iterable<Query> {
    private List<Query> list = new ArrayList<>();
    private DynamoFilterSpec attribute;

    public QueryList() {
    }

    public QueryList(Query query) {
      this.list.add(query);
    }

    public void setAttribute(DynamoFilterSpec attribute) {
      this.attribute = attribute;
    }

    public DynamoFilterSpec attribute() {
      return attribute;
    }

    @Override
    public Iterator<Query> iterator() {
      return list.iterator();
    }

    public void add(Query query) {
      this.list.add(query);
    }
  }
}
