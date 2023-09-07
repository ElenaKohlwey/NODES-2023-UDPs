package org.ek.n23.utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilder;
import org.neo4j.harness.Neo4jBuilders;

/**
 * Base class for all tests that need a neo4j test database.<p>
 * It does four things:<p>
 * - build the test database (with the cypher file if one is specified)<p>
 * - registers the specified procedure classes <p>
 * - executes the initial cypher statement (if one is specified)<p>
 * - closes the database after the tests are done
 *
 * @author Jens Deininger
 */
public abstract class TestBase {

  public static final String DELETE_ALL_CYPHER = "MATCH (n) DETACH DELETE n";

  private static final Config driverConfig = Config
    .builder()
    .withoutEncryption()
    .build();

  private Driver driver;

  private Neo4j embeddedDatabaseServer;

  /**
   * The {@link InputStream} of the file containing the cypher statements
   * that create the test database.<p>
   * Provide null if there is none
   */
  protected abstract InputStream inputStreamOfCypherFile();

  /**
   * The classes containing the procedures to be registered for the test
   */
  protected abstract Stream<Class<?>> procedureClasses();

  /**
   * The initial cypher statement to execute. Empty or null if none
   */
  protected abstract String[] initialCypher();

  /**
   * Anything else one wants to happen in the BeforeAll part after the
   * initial cypher statement has run. <p>
   * To be overwritten in the derivations.
   */
  protected void otherBeforeAllStuff() {}

  protected Driver driver() {
    return driver;
  }

  @BeforeAll
  void initializeNeo4j() throws IOException {
    StringWriter sw = new StringWriter();

    InputStream cypherInputStream = inputStreamOfCypherFile();
    if (cypherInputStream != null) {
      new BufferedReader(new InputStreamReader(inputStreamOfCypherFile()))
        .transferTo(sw);
      sw.flush();
      cypherInputStream.close();
    }

    Neo4jBuilder builder = Neo4jBuilders.newInProcessBuilder();

    Stream<Class<?>> classes = procedureClasses();
    if (classes != null) {
      Iterator<Class<?>> iter = procedureClasses().iterator();
      while (iter.hasNext()) {
        builder = builder.withProcedure(iter.next());
      }
    }
    this.embeddedDatabaseServer = builder.withFixture(sw.toString()).build();

    driver =
      GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);

    String[] initialCypherString = initialCypher();
    if (initialCypherString != null && initialCypherString.length > 0) {
      for (String initialCypher : initialCypherString) {
        try (Session session = driver.session()) {
          session.run(initialCypher);
        }
      }
    }
    otherBeforeAllStuff();
  }

  @AfterAll
  void closeDriver() {
    driver.close();
    embeddedDatabaseServer.close();
  }
}
