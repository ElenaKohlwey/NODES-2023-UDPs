package org.ek.n23;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.stream.Stream;
import org.ek.n23.entity.Config;
import org.ek.n23.utility.MinMax;
import org.neo4j.graphdb.Node;

/**
 * This class holds all min and max values.
 * It holds example objects, provides getters and setters
 * and a constructur that converts a Neo4j configNode
 * into a ConfigObject.
 *
 * @author Jens Deininger
 */
public class ConfigObject {

  // This Config is used for unit tests. Do not modify!
  public static final ConfigObject SampleConfig = new ConfigObject("UnitTests")
    .setActionCount(40, 60)
    .setActionDuration(3, 20)
    .setStartingNodesCount(1, 10)
    .setOutgoingRelationsCount(1, 5);

  public static final ConfigObject MyFirstConfig = new ConfigObject("NODES2023")
    .setActionCount(42, 345)
    .setActionDuration(3, 20)
    .setStartingNodesCount(1, 5)
    .setOutgoingRelationsCount(1, 2);

  // region static stuff

  protected static final String INCOMPLETE_INIT =
    "RandomGraphConfig is not fully initialised";

  protected static final String DUPLICATE_CONFIG_NAME =
    "RandomGraphConfig is not fully initialised";

  private static final HashMap<String, ConfigObject> smConfigName2ConfigMap = new HashMap<>();

  public static ConfigObject getConfig(String name) {
    return smConfigName2ConfigMap.get(name);
  }

  public static Stream<String> configNames() {
    return smConfigName2ConfigMap.keySet().stream();
  }

  public static Stream<ConfigObject> configs() {
    return smConfigName2ConfigMap.values().stream();
  }

  static {
    Field[] allFields = ConfigObject.class.getDeclaredFields();
    for (Field f : allFields) {
      int mod = f.getModifiers();
      if (
        !Modifier.isStatic(mod) ||
        !Modifier.isFinal(mod) ||
        !f.getType().equals(ConfigObject.class)
      ) {
        continue;
      }
      ConfigObject config = null;
      try {
        config = (ConfigObject) f.get(null);
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw new UnsupportedOperationException("Well, dis be bad voodoo");
      }
      if (smConfigName2ConfigMap.containsKey(config.name())) {
        throw new IllegalStateException(DUPLICATE_CONFIG_NAME);
      }
      if (!config.isFullyInitialised()) {
        throw new IllegalStateException(INCOMPLETE_INIT);
      }
      smConfigName2ConfigMap.put(config.name(), config);
    }
  }

  // endregion

  // region private fields

  private String mName;

  private MinMax<Long> mActionCount;

  private MinMax<Long> mActionDuration;

  private MinMax<Long> mStartingNodesCount;

  private MinMax<Long> mOutgoingRelationsCount;

  // endregion

  // region ctor

  public ConfigObject(String name) {
    if (name == null || name.trim().isEmpty()) {
      throw new IllegalArgumentException();
    }
    mName = name;
  }

  public ConfigObject(Node configNode) {
    String validationMessage = Config.validate(configNode);

    if (!validationMessage.equals(Config.VALIDATION_PASSED)) {
      throw new IllegalArgumentException(validationMessage);
    }
    this.setActionCount(
        Config.actionCountMin(configNode),
        Config.actionCountMax(configNode)
      )
      .setActionDuration(
        Config.actionDurationMin(configNode),
        Config.actionDurationMax(configNode)
      )
      .setStartingNodesCount(
        Config.startingNodesCountMin(configNode),
        Config.startingNodesCountMax(configNode)
      )
      .setOutgoingRelationsCount(
        Config.outgoingRelationsCountMin(configNode),
        Config.outgoingRelationsCountMax(configNode)
      );
  }

  // endregion

  // region getters

  public String name() {
    return mName;
  }

  public MinMax<Long> actionCount() {
    if (mActionCount == null) {
      throw new IllegalStateException(INCOMPLETE_INIT);
    }
    return mActionCount;
  }

  public MinMax<Long> actionDuration() {
    if (mActionDuration == null) {
      throw new IllegalStateException(INCOMPLETE_INIT);
    }
    return mActionDuration;
  }

  public MinMax<Long> startingNodesCount() {
    if (mStartingNodesCount == null) {
      throw new IllegalStateException(INCOMPLETE_INIT);
    }
    return mStartingNodesCount;
  }

  public MinMax<Long> outgoingRelationsCount() {
    if (mOutgoingRelationsCount == null) {
      throw new IllegalStateException(INCOMPLETE_INIT);
    }
    return mOutgoingRelationsCount;
  }

  protected boolean isFullyInitialised() {
    Field[] fields = this.getClass().getDeclaredFields();
    for (Field f : fields) {
      try {
        if (f.get(this) == null) {
          return false;
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw new UnsupportedOperationException("Well, dis be bad voodoo");
      }
    }
    return true;
  }

  // endregion

  // region setters

  public ConfigObject setActionCount(Number min, Number max) {
    mActionCount = new MinMax<>(min.longValue(), max.longValue());
    return this;
  }

  public ConfigObject setActionDuration(Number min, Number max) {
    mActionDuration = new MinMax<>(min.longValue(), max.longValue());
    return this;
  }

  public ConfigObject setStartingNodesCount(Number min, Number max) {
    mStartingNodesCount = new MinMax<>(min.longValue(), max.longValue());
    return this;
  }

  public ConfigObject setOutgoingRelationsCount(Number min, Number max) {
    mOutgoingRelationsCount = new MinMax<>(min.longValue(), max.longValue());
    return this;
  }

  // endregion

  public void write2Node(Node configNode) {
    Config.setActionCountMin(configNode, actionCount().min());
    Config.setActionCountMax(configNode, actionCount().max());

    Config.setActionDurationMin(configNode, actionDuration().min());
    Config.setActionDurationMax(configNode, actionDuration().max());

    Config.setStartingNodesCountMin(configNode, startingNodesCount().min());
    Config.setStartingNodesCountMax(configNode, startingNodesCount().max());

    Config.setOutgoingRelationsCountMin(
      configNode,
      outgoingRelationsCount().min()
    );
    Config.setOutgoingRelationsCountMax(
      configNode,
      outgoingRelationsCount().max()
    );
  }
}
