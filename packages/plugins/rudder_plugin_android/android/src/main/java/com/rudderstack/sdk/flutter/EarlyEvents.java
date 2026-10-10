package com.rudderstack.sdk.flutter;

import static com.rudderstack.sdk.flutter.parsers.RudderConfigParser.getRudderConfigObject;

import android.content.Context;
import android.content.SharedPreferences;

import com.rudderstack.android.sdk.core.RudderClient;
import com.rudderstack.android.sdk.core.RudderConfig;
import com.rudderstack.android.sdk.core.RudderLogger;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Sends an immediate event before the Flutter engine starts, such as an app open from the launcher activity.
 *
 * <p>It uses the configuration the plugin saved when Dart last initialized it, so it works from the second launch of an
 * install onwards; until then, and when the app was initialized with database encryption or integrations (which the
 * saved configuration cannot carry), it sends nothing and returns false. The client it creates is the one the plugin
 * keeps when Dart initializes it later in the same process.
 */
public final class EarlyEvents {
  static final Object LOCK = new Object();
  private static final String PREFS = "rl_flutter_init";
  private static final String KEY_WRITE_KEY = "write_key";
  private static final String KEY_CONFIG = "config";
  // Off the main thread, so creating the client never delays the first frame.
  private static final ExecutorService executor = Executors.newSingleThreadExecutor();

  private EarlyEvents() {}

  static void save(Context context, String writeKey, Map<String, Object> configMap, boolean reusable) {
    SharedPreferences.Editor editor = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit();
    if (reusable && writeKey != null && configMap != null) {
      editor.putString(KEY_WRITE_KEY, writeKey).putString(KEY_CONFIG, new JSONObject(configMap).toString());
    } else {
      editor.clear();
    }
    editor.apply();
  }

  /**
   * Creates the client and the dispatcher, warming its connection, in the background; an event sent next (from the
   * launcher activity) then waits for neither. Call it from {@code Application.onCreate}: it sends nothing, so a process
   * started in the background costs one connection to the data plane.
   */
  public static void prepare(Context context) {
    final Context app = context.getApplicationContext();
    SharedPreferences prefs = app.getSharedPreferences(PREFS, Context.MODE_PRIVATE);
    final String writeKey = prefs.getString(KEY_WRITE_KEY, null);
    final String configJson = prefs.getString(KEY_CONFIG, null);
    if (writeKey == null || configJson == null) return;
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          ready(app, writeKey, new JSONObject(configJson));
        } catch (Exception e) {
          RudderLogger.logError(e);
        }
      }
    });
  }

  private static ImmediateEventDispatcher ready(Context app, String writeKey, JSONObject configJson) throws Exception {
    RudderConfig config = getRudderConfigObject(toMap(configJson), null, null);
    // First, so the connection is ready by the time the event is sent; creating the client takes a few hundred ms.
    ImmediateEventDispatcher.warmUp(config.getDataPlaneUrl());
    synchronized (LOCK) {
      if (RudderClient.getInstance() == null) RudderClient.getInstance(app, writeKey, config);
    }
    return ImmediateEventDispatcher.shared(app, writeKey, config.getDataPlaneUrl());
  }

  /** Sends {@code eventName} as an immediate event; false when there is no saved configuration to send it with. */
  public static boolean trackImmediate(Context context, final String eventName, Map<String, Object> properties) {
    final Context app = context.getApplicationContext();
    SharedPreferences prefs = app.getSharedPreferences(PREFS, Context.MODE_PRIVATE);
    final String writeKey = prefs.getString(KEY_WRITE_KEY, null);
    final String configJson = prefs.getString(KEY_CONFIG, null);
    if (writeKey == null || configJson == null) return false;
    final Map<String, Object> eventProperties = new HashMap<>(properties);
    eventProperties.put(ImmediateEventDispatcher.SEND_IMMEDIATELY, true);
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          ImmediateEventDispatcher dispatcher = ready(app, writeKey, new JSONObject(configJson));
          dispatcher.track(eventName, eventProperties, null);
        } catch (Exception e) {
          RudderLogger.logError(e);
        }
      }
    });
    return true;
  }

  // org.json keeps whole numbers as Integer, which the configuration parser casts them to.
  private static Map<String, Object> toMap(JSONObject json) throws Exception {
    Map<String, Object> map = new HashMap<>();
    Iterator<String> keys = json.keys();
    while (keys.hasNext()) {
      String key = keys.next();
      map.put(key, toValue(json.get(key)));
    }
    return map;
  }

  private static Object toValue(Object value) throws Exception {
    if (value == JSONObject.NULL) return null;
    if (value instanceof JSONObject) return toMap((JSONObject) value);
    if (value instanceof JSONArray) {
      JSONArray array = (JSONArray) value;
      List<Object> list = new ArrayList<>();
      for (int i = 0; i < array.length(); i++) list.add(toValue(array.get(i)));
      return list;
    }
    return value;
  }
}
