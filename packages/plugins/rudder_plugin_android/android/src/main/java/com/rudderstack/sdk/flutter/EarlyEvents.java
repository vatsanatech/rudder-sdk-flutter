package com.rudderstack.sdk.flutter;

import static com.rudderstack.sdk.flutter.parsers.RudderConfigParser.getRudderConfigObject;

import android.content.Context;
import android.content.SharedPreferences;
import android.content.pm.PackageInfo;
import android.os.Build;

import com.rudderstack.android.sdk.core.RudderClient;
import com.rudderstack.android.sdk.core.RudderConfig;
import com.rudderstack.android.sdk.core.RudderLogger;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
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
  // The SDK's own store (RudderPreferenceManager): the identity it last saved and the opt-out flag.
  private static final String RUDDER_PREFS = "rl_prefs";
  private static final String KEY_ANONYMOUS_ID = "rl_anonymous_id_key";
  private static final String KEY_TRAITS = "rl_traits";
  private static final String KEY_OPT_OUT = "rl_opt_status";
  private static final String KEY_SESSION_ID = "rl_session_id_key";
  private static final String LIBRARY = "com.rudderstack.android.sdk.core";
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

  /**
   * Sends {@code eventName} as the first thing the process does, for an event such as the app open, which is due before
   * anything else starts. Unlike {@link #trackImmediate} it does not wait for the client: the event is built here from
   * the identity the SDK last saved, with the same shape the SDK uploads, and posted at once. False when there is no
   * saved configuration or identity yet, or the user opted out.
   */
  public static boolean sendFirst(Context context, String eventName, Map<String, Object> properties) {
    try {
      Context app = context.getApplicationContext() != null ? context.getApplicationContext() : context;
      SharedPreferences prefs = app.getSharedPreferences(PREFS, Context.MODE_PRIVATE);
      String writeKey = prefs.getString(KEY_WRITE_KEY, null);
      String configJson = prefs.getString(KEY_CONFIG, null);
      SharedPreferences rudder = app.getSharedPreferences(RUDDER_PREFS, Context.MODE_PRIVATE);
      String anonymousId = rudder.getString(KEY_ANONYMOUS_ID, null);
      if (writeKey == null || configJson == null || anonymousId == null || rudder.getBoolean(KEY_OPT_OUT, false)) {
        return false;
      }
      String dataPlaneUrl = new JSONObject(configJson).optString("dataPlaneUrl", null);
      if (dataPlaneUrl == null || dataPlaneUrl.isEmpty()) return false;
      String messageId = UUID.randomUUID().toString();
      JsonObject event = buildEvent(app, rudder, anonymousId, messageId, eventName, properties);
      ImmediateEventDispatcher.sendFirst(app, writeKey, dataPlaneUrl, event, messageId);
      return true;
    } catch (Exception e) {
      RudderLogger.logError(e);
      return false;
    }
  }

  private static JsonObject buildEvent(Context app, SharedPreferences rudder, String anonymousId, String messageId,
      String eventName, Map<String, Object> properties) throws Exception {
    JsonObject traits = new JsonObject();
    String savedTraits = rudder.getString(KEY_TRAITS, null);
    if (savedTraits != null) traits = JsonParser.parseString(savedTraits).getAsJsonObject();
    traits.addProperty("anonymousId", anonymousId);
    String userId = traits.has("userId") ? traits.get("userId").getAsString()
        : traits.has("id") ? traits.get("id").getAsString() : null;

    PackageInfo info = app.getPackageManager().getPackageInfo(app.getPackageName(), 0);
    JsonObject appContext = new JsonObject();
    appContext.addProperty("name", String.valueOf(app.getApplicationInfo().loadLabel(app.getPackageManager())));
    appContext.addProperty("namespace", app.getPackageName());
    appContext.addProperty("version", info.versionName);
    appContext.addProperty("build", String.valueOf(info.versionCode));
    JsonObject os = new JsonObject();
    os.addProperty("name", "Android");
    os.addProperty("version", Build.VERSION.RELEASE);
    JsonObject device = new JsonObject();
    device.addProperty("manufacturer", Build.MANUFACTURER);
    device.addProperty("model", Build.MODEL);
    device.addProperty("name", Build.DEVICE);
    device.addProperty("type", "Android");
    JsonObject library = new JsonObject();
    library.addProperty("name", LIBRARY);
    JsonObject eventContext = new JsonObject();
    eventContext.add("app", appContext);
    eventContext.add("os", os);
    eventContext.add("device", device);
    eventContext.add("library", library);
    eventContext.add("traits", traits);
    eventContext.addProperty("locale", Locale.getDefault().toLanguageTag());
    eventContext.addProperty("timezone", TimeZone.getDefault().getID());
    long sessionId = rudder.getLong(KEY_SESSION_ID, 0);
    if (sessionId > 0) eventContext.addProperty("sessionId", sessionId);

    JsonObject eventProperties = new JsonObject();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof Number) eventProperties.addProperty(entry.getKey(), (Number) value);
      else if (value instanceof Boolean) eventProperties.addProperty(entry.getKey(), (Boolean) value);
      else if (value != null) eventProperties.addProperty(entry.getKey(), String.valueOf(value));
    }
    eventProperties.addProperty(ImmediateEventDispatcher.SEND_IMMEDIATELY, true);
    JsonObject integrations = new JsonObject();
    integrations.addProperty("All", true);

    JsonObject event = new JsonObject();
    event.addProperty("type", "track");
    event.addProperty("event", eventName);
    event.addProperty("messageId", messageId);
    event.addProperty("anonymousId", anonymousId);
    if (userId != null && !userId.isEmpty()) event.addProperty("userId", userId);
    event.addProperty("channel", "mobile");
    event.addProperty("originalTimestamp", ImmediateEventDispatcher.timestamp());
    event.add("context", eventContext);
    event.add("properties", eventProperties);
    event.add("integrations", integrations);
    return event;
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
