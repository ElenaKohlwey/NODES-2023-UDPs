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
 * These tests verify the functionality of the backwardPath
 * method. They are parameterized to make them lean but still
 * test everything.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BackwardPathTest extends TestBase {

  HashMap<String, int[]> results;

  @Override
  protected InputStream inputStreamOfCypherFile() {
    // read in the cypher file in the folder test/resources
    return getClass().getResourceAsStream("/projectScheduleBackward.cypher");
  }

  @Override
  protected Stream<Class<?>> procedureClasses() {
    // state the class (or classes) that these tests apply to
    return Stream.of(ProcedureBackwardPath.class);
  }

  @Override
  protected String[] initialCypher() {
    /*  state a cypher query (e.g. call of a procedure)
    that shall be called before any test is executed */
    return new String[] {
      String.format(
        "MATCH (a:%s {%s:'End'}) CALL %s(a)",
        Action.LABEL_NAME,
        Action.NAME_KEY,
        ProcedureName.BACKWARD_PATH
      ),
    };
  }

  @BeforeAll
  public void initializeResults() {
    results = new HashMap<>();
    results.put("A", new int[] { 0, 3 });
    results.put("B", new int[] { 5, 9 });
    results.put("C", new int[] { 3, 9 });
    results.put("D", new int[] { 9, 15 });
    results.put("E", new int[] { 9, 13 });
    results.put("F", new int[] { 9, 13 });
    results.put("G", new int[] { 15, 21 });
    results.put("H", new int[] { 13, 21 });
    results.put("End", new int[] { 21, 21 });
  }

  @Test
  void nodes_LS_LF() {
    Record nodeRecord;
    long latestStart;
    long latestFinish;

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
                Action.LATEST_START_KEY,
                Action.LATEST_START_KEY,
                Action.LATEST_FINISH_KEY,
                Action.LATEST_FINISH_KEY
              )
            )
            .single();
      }
      latestStart = nodeRecord.get(Action.LATEST_START_KEY, -1l);
      latestFinish = nodeRecord.get(Action.LATEST_FINISH_KEY, -1l);
      assertEquals(result.getValue()[0], latestStart);
      assertEquals(result.getValue()[1], latestFinish);
    }
  }
}
