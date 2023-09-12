package org.ek.n23;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.ek.n23.entity.Action;
import org.ek.n23.utility.TestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;

/**
 * These tests verify the functionality of the forwardPath
 * method. They are parameterized to make them lean but still
 * test everything.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ForwardPathTest extends TestBase {

  HashMap<String, int[]> results;

  @Override
  protected InputStream inputStreamOfCypherFile() {
    // read in the cypher file in the folder test/resources
    return getClass().getResourceAsStream("/projectScheduleForward.cypher");
  }

  @Override
  protected Stream<Class<?>> procedureClasses() {
    // state the class (or classes) that these tests apply to
    return Stream.of(ProcedureGraphTraversal.class);
  }

  @Override
  protected String[] initialCypher() {
    /*  state a cypher query (e.g. call of a procedure)
    that shall be called before any test is executed */
    return new String[] {
      String.format(
        "MATCH (a:%s {%s:'Start'}) CALL %s(a)",
        Action.LABEL_NAME,
        Action.NAME_KEY,
        ProcedureGraphTraversal.ProcedureName.FORWARD_PATH
      ),
    };
  }

  @BeforeAll
  public void initializeResults() {
    results = new HashMap<>();
    results.put("A", new int[] { 0, 3 });
    results.put("B", new int[] { 3, 7 });
    results.put("C", new int[] { 3, 9 });
    results.put("D", new int[] { 7, 13 });
    results.put("E", new int[] { 7, 11 });
    results.put("F", new int[] { 9, 13 });
    results.put("G", new int[] { 13, 19 });
    results.put("H", new int[] { 13, 21 });
    results.put("End", new int[] { 21, 21 });
  }

  @Test
  void nodes_ES_EF() {
    Record nodeRecord;
    long earliestStart;
    long earliestFinish;

    for (Map.Entry<String, int[]> result : results.entrySet()) {
      try (Session session = driver().session()) {
        nodeRecord =
          session
            .run(
              String.format(
                "MATCH (a:%s {%s:'%s'}) RETURN a.%s AS %s, a.%s AS %s",
                Action.LABEL_NAME,
                Action.NAME_KEY,
                result.getKey(),
                Action.EARLIEST_START_KEY,
                Action.EARLIEST_START_KEY,
                Action.EARLIEST_FINISH_KEY,
                Action.EARLIEST_FINISH_KEY
              )
            )
            .single();
      }
      earliestStart = nodeRecord.get(Action.EARLIEST_START_KEY, -1l);
      earliestFinish = nodeRecord.get(Action.EARLIEST_FINISH_KEY, -1l);
      assertEquals(result.getValue()[0], earliestStart);
      assertEquals(result.getValue()[1], earliestFinish);
    }
  }
}
