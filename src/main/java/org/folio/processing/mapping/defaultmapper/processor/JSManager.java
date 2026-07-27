package org.folio.processing.mapping.defaultmapper.processor;

import java.util.HashMap;
import java.util.Map;
import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages precompiled JavaScript snippets for mapping functions.
 */
// Preserves the published utility type name for backward compatibility.
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JSManager {

  private static final Logger LOGGER = LogManager.getLogger(JSManager.class);

  private static final String GRAAL_JS_ENGINE = "graal.js";
  private static final ScriptEngine ENGINE = new ScriptEngineManager().getEngineByName(GRAAL_JS_ENGINE);
  private static final Map<Integer, CompiledScript> PRE_COMPILED_JS = new HashMap<>();

  // Preserves the published method name for backward compatibility.
  @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
  public static Object runJScript(String jscript, String data) throws ScriptException {
    CompiledScript script = PRE_COMPILED_JS.get(jscript.hashCode());
    if (script == null) {
      LOGGER.debug("runJScript:: compiling JS function: {}", jscript);
      script = ((Compilable) ENGINE).compile(jscript);
      PRE_COMPILED_JS.put(jscript.hashCode(), script);
    }
    Bindings bindings = new SimpleBindings();
    bindings.put("DATA", data);
    return script.eval(bindings);
  }
}
