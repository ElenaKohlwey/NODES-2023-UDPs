package org.ek.n23;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.util.Random;
import java.util.stream.Stream;
import org.ek.n23.entity.Action;
import org.ek.n23.entity.Precedes;
import org.ek.n23.utility.Summary;
import org.ek.n23.utility.TestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GenerateGraphFromConfigNodeTest extends TestBase {

  @Override
  protected InputStream inputStreamOfCypherFile() {
    return null;
  }

  @Override
  protected Stream<Class<?>> procedureClasses() {
    return Stream.of(Procedures.class);
  }

  @Override
  protected String[] initialCypher() {
    return null;
  }

  private ConfigObject conf = ConfigObject.SampleConfig;

  @Test
  void createdGraphMeetsRequirementsMulti() {
    Random random = new Random();

    for (int i = 0; i < 100; i++) {
      int seed = random.nextInt();
      System.out.println("Start execution of tests with seed " + seed);

      // clear out the database
      deleteGraph();

      // create graph with given seed
      createGraph(seed);

      // fetch nodesCount
      int nodesCount = getNodesCount();

      // test that Action node count
      testActionNodesCount(nodesCount);

      // test Action duration
      testActionDuration(nodesCount);

      // test starting nodes count
      testStartingNodesCount();

      // test outgoing rels count
      testOutgoingRelsCount(nodesCount);
    }
  }

  @Test
  void createdGraphMeetsRequirementsSingle() {
    // clear out the database
    deleteGraph();

    // create graph
    createGraph(-16719473);

    // fetch nodesCount
    int nodesCount = getNodesCount();

    // test that Action node count
    testActionNodesCount(nodesCount);

    // test Action duration
    testActionDuration(nodesCount);

    // test starting nodes count
    testStartingNodesCount();

    // test outgoing rels count
    testOutgoingRelsCount(nodesCount);
  }

  /** deletes existing graph in db */
  private void deleteGraph() {
    try (Session session = driver().session()) {
      session.run(DELETE_ALL_CYPHER);
    }
  }

  /** creates a graph with given seed */
  private void createGraph(int seed) {
    try (Session session = driver().session()) {
      session.run(
        String.format(
          "CALL %s(%s,'%s') YIELD %s, %s, %s RETURN %s, %s, %s",
          Procedures.ProcedureName.GENERATE_GRAPH_BY_SEED_AND_CONFIG,
          seed,
          conf.name(),
          Summary.NODES_MAP,
          Summary.RELATIONSHIPS_MAP,
          Summary.OTHER_MAP,
          Summary.NODES_MAP,
          Summary.RELATIONSHIPS_MAP,
          Summary.OTHER_MAP
        )
      );
    }
  }

  /** fetches amount of created Action nodes */
  private int getNodesCount() {
    int nodesCount;
    String variableName = "nodesCount";
    try (Session session = driver().session()) {
      nodesCount =
        session
          .run(
            String.format(
              "MATCH (a:%s) RETURN count(a) AS %s",
              Action.LABEL_NAME,
              variableName
            )
          )
          .single()
          .get(variableName)
          .asInt();
    }
    return nodesCount;
  }

  /** tests that Action node count is conform with config */
  private void testActionNodesCount(int nodesCount) {
    assertTrue(conf.actionCount().min() <= nodesCount);
    assertTrue(nodesCount - 2 <= conf.actionCount().max());
  }

  /** tests that Action duration is conform with config */
  private void testActionDuration(int nodesCount) {
    Record resultRecord;
    int actionDurationMin;
    int actionDurationMax;
    String variableNameMin = "minDuration";
    String variableNameMax = "maxDuration";

    try (Session session = driver().session()) {
      resultRecord =
        session
          .run(
            String.format(
              "MATCH (a:%s) WHERE NOT a.%s IN ['%s','%s'] RETURN min(a.%s) AS %s, max(a.%s) AS %s",
              Action.LABEL_NAME,
              Action.NAME_KEY,
              Action.transformToNodeName(0),
              Action.transformToNodeName(nodesCount - 1),
              Action.DURATION_KEY,
              variableNameMin,
              Action.DURATION_KEY,
              variableNameMax
            )
          )
          .single();

      actionDurationMin = resultRecord.get(variableNameMin).asInt();
      actionDurationMax = resultRecord.get(variableNameMax).asInt();
    }

    assertTrue(conf.actionDuration().min() <= actionDurationMin);
    assertTrue(actionDurationMax <= conf.actionDuration().max());
  }

  /** tests that starting nodes count is conform with config */
  private void testStartingNodesCount() {
    int startingNodesCount;
    String variableName = "nodesCount";

    try (Session session = driver().session()) {
      startingNodesCount =
        session
          .run(
            String.format(
              "MATCH (a:%s {%s:'%s'})-[s:%s]->(:%s) RETURN count(s) AS %s",
              Action.LABEL_NAME,
              Action.NAME_KEY,
              Action.transformToNodeName(0),
              Precedes.TYPE_NAME,
              Action.LABEL_NAME,
              variableName
            )
          )
          .single()
          .get(variableName)
          .asInt();
    }

    assertTrue(conf.startingNodesCount().min() <= startingNodesCount);
    assertTrue(startingNodesCount <= conf.startingNodesCount().max());
  }

  /** tests that outgoing rels count is conform with config */
  private void testOutgoingRelsCount(int nodesCount) {
    Record resultRecord;
    int numberRelsMin;
    int numberRelsMax;
    String variableNameMin = "minOutgoingRels";
    String variableNameMax = "maxOutgoingRels";

    try (Session session = driver().session()) {
      resultRecord =
        session
          .run(
            String.format(
              "MATCH (a:%s)-[s:%s]->(b:%s) WHERE NOT a.name = '%s' AND NOT b.name = '%s' WITH a, count(s) AS relCount RETURN min(relCount) AS %s, max(relCount) AS %s",
              Action.LABEL_NAME,
              Precedes.TYPE_NAME,
              Action.LABEL_NAME,
              Action.transformToNodeName(0),
              Action.transformToNodeName(nodesCount - 1),
              variableNameMin,
              variableNameMax
            )
          )
          .single();

      numberRelsMin = resultRecord.get(variableNameMin).asInt();
      numberRelsMax = resultRecord.get(variableNameMax).asInt();
    }

    assertTrue(conf.outgoingRelationsCount().min() <= numberRelsMin);
    assertTrue(numberRelsMax <= conf.outgoingRelationsCount().max());
  }
}
