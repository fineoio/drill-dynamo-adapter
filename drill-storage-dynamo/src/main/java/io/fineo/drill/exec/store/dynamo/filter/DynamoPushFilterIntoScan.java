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

import com.google.common.collect.ImmutableList;
import io.fineo.drill.exec.store.dynamo.DynamoGroupScan;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGroupScanSpec;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

import static org.apache.drill.exec.planner.logical.DrillOptiq.toDrill;
import static org.apache.drill.exec.planner.physical.PrelUtil.getPlannerSettings;

public final class DynamoPushFilterIntoScan {

  private DynamoPushFilterIntoScan() {
  }

  public static final StoragePluginOptimizerRule FILTER_ON_SCAN = new FilterIntoScanBase(
    RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)),
    "DynamoPushFilterIntoScan:Filter_On_Scan") {

    @Override
    protected ScanPrel getScan(RelOptRuleCall call) {
      return call.rel(1);
    }

    @Override
    protected void pushDown(RelOptRuleCall call) {
      final FilterPrel filter = call.rel(0);
      final RexNode condition = filter.getCondition();

      final ScanPrel scan = getScan(call);
      DynamoGroupScan groupScan = (DynamoGroupScan) scan.getGroupScan();
      doPushFilterToScan(call, filter, null, scan, groupScan, condition);
    }
  };


  public static final StoragePluginOptimizerRule FILTER_ON_PROJECT = new FilterIntoScanBase(
    RelOptHelper
      .some(FilterPrel.class,
        RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class))),
    "DynamoPushFilterIntoScan:Filter_On_Project") {

    @Override
    protected ScanPrel getScan(RelOptRuleCall call) {
      return call.rel(2);
    }

    @Override
    protected void pushDown(RelOptRuleCall call) {
      final FilterPrel filter = call.rel(0);
      final ProjectPrel project = call.rel(1);
      final ScanPrel scan = getScan(call);

      // convert the filter to one that references the child of the project
      final RexNode condition = RelOptUtil.pushPastProject(filter.getCondition(), project);
      doPushFilterToScan(call, filter, project, scan, (DynamoGroupScan) scan.getGroupScan(),
        condition);
    }
  };

  private abstract static class FilterIntoScanBase extends StoragePluginOptimizerRule {

    private FilterIntoScanBase(RelOptRuleOperand operand, String description) {
      super(operand, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      if ((call.getRelList().size() >= 2 && checkDynamoScan(call.rel(1))) || (
        call.getRelList().size() >= 3 && checkDynamoScan(call.rel(2)))) {
        return super.matches(call);
      }
      return false;
    }

    private boolean checkDynamoScan(RelNode scan) {
      return scan instanceof ScanPrel ?
             ((ScanPrel) scan).getGroupScan() instanceof DynamoGroupScan :
             false;
    }

    @Override
    public final void onMatch(RelOptRuleCall call) {
      final ScanPrel scan = getScan(call);
      // according to Drill docs we ned to check again even after the matches check
      GroupScan gs = scan.getGroupScan();
      if (!(gs instanceof DynamoGroupScan)) {
        return;
      }
      DynamoGroupScan groupScan = (DynamoGroupScan) gs;
      if (groupScan.isFilterPushedDown()) {
        /*
         * The rule can get triggered again due to the transformed "scan => filter" sequence
         * created by the earlier execution of this rule when we could not do a complete
         * conversion of Optiq Filter's condition to Dynamo Filter. In such cases, we rely upon
         * this flag to not do a re-processing of the rule on the already transformed call.
         */
        return;
      }
      pushDown(call);
    }

    protected abstract ScanPrel getScan(RelOptRuleCall call);

    protected abstract void pushDown(RelOptRuleCall call);
  }

  private static void doPushFilterToScan(final RelOptRuleCall call, final FilterPrel filter,
    final ProjectPrel project, final ScanPrel scan, final DynamoGroupScan groupScan,
    final RexNode condition) {
    // convert the expression
    final LogicalExpression conditionExp =
      toDrill(new DrillParseContext(getPlannerSettings(call.getPlanner())), scan, condition);
    // try to build a new filter
    final DynamoFilterBuilder filterBuilder = new DynamoFilterBuilder(groupScan, conditionExp);
    final DynamoGroupScanSpec newScanSpec = filterBuilder.parseTree();
    if (newScanSpec == null) {
      return; //no filter pushDown ==> No transformation.
    }

    final DynamoGroupScan newGroupScan = new DynamoGroupScan(groupScan);
    newGroupScan.setScanSpec(newScanSpec);
    newGroupScan.setFilterPushedDown(true);

    final ScanPrel newScanPrel =
      ScanPrel.create(scan, filter.getTraitSet(), newGroupScan, scan.getRowType());

    // Depending on whether is a project in the middle, assign either scan or copy of project to
    // childRel.
    final RelNode childRel = project == null ? newScanPrel : project
      .copy(project.getTraitSet(), ImmutableList.of((RelNode) newScanPrel)); ;

    if (filterBuilder.isAllExpressionsConverted()) {
        /*
         * Since we could convert the entire filter condition expression into a Dynamo filter,
         * we can eliminate the filter operator altogether.
         */
      call.transformTo(childRel);
    } else {
      call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(childRel)));
    }
  }
}
