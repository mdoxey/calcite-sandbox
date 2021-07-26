package IntroExample;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;

public class QueryOptimizerExample {

  public static void main(String[] args) throws SQLException {
    // provides a wrapper around a schema
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(true).plus();
    // builds a config for the query planner
    FrameworkConfig config = Frameworks.newConfigBuilder()
        // adds a dummy schema from a Calcite testbase - this is where we would normally have connection to db
        .defaultSchema(CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.HR))
        .build();
    // builder for relational operators, uses the Logical rel factories by default, can override these
    RelBuilder builder = RelBuilder.create(config);


    // builds a query roughly equal to "SELECT * from emps, depts NATURAL JOIN deptno WHERE empid = 100"
    // inefficiency: filter comes *after* join
    RelNode opTree = builder.scan("emps")
        .scan("depts")
        .join(JoinRelType.INNER, "deptno")
        .filter(builder.equals(builder.field("empid"), builder.literal(100)))
        .build();

    RelWriter rw = new RelWriterImpl(new PrintWriter(System.out, true));
    // prints out the default EXPLAIN for above query
    opTree.explain(rw);

    // Hep = heuristic planner
    // hepProgram defines the set of rules that the planner will attempt to use/apply
    HepProgram hepProgram = HepProgram.builder()
        // rule that pushes filter below (before) the join
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .build();

    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(opTree);
    // uses the HepPlanner's optimizer to find the best plan using the defined rules
    System.out.println();
    hepPlanner.findBestExp().explain(rw);

    // a cluster of related operations in a relational expression tree during query optimization
    RelOptCluster cluster = opTree.getCluster();
    // Volcano = applies a set of rules to minimize the expected cost of executing query
    // this casting works because VolcanoPlanner is the default planner used by Calcite
    VolcanoPlanner volcanoPlanner = (VolcanoPlanner) cluster.getPlanner();

    // operators in opTree above are "logical" - they don't have a specific, physical implementation
    // To plan a query for Volcano, they must be converted to physical operator so the VolcanoPlanner can
    // calculate an estimated cost
    // Each operator has calling convention --> specifies how query will be executed
    // Enumerable -> implements queries over collections
    RelTraitSet desiredTraits = cluster.traitSet().replace(EnumerableConvention.INSTANCE);
    // converts opTree to new calling convention using "trait" mechanism
    RelNode newRoot = volcanoPlanner.changeTraits(opTree, desiredTraits);
    volcanoPlanner.setRoot(newRoot);

    RelNode optimized = volcanoPlanner.findBestExp();
    System.out.println();
    optimized.explain(rw);

    // can now execute this optimized query since we are using EnumerableConvention now
    ResultSet result = RelRunners.run(optimized).executeQuery();
    int columns = result.getMetaData().getColumnCount();
    System.out.println();
    while (result.next()) {
      System.out.println(result.getString(1) + " " + result.getString(7));
    }
  }
}
