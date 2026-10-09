package com.rudderstack.sdk.flutter;

import android.content.Context;
import android.content.SharedPreferences;
import android.os.SystemClock;
import android.util.Base64;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rudderstack.android.sdk.core.RudderClient;
import com.rudderstack.android.sdk.core.RudderLogger;
import com.rudderstack.android.sdk.core.RudderMessage;
import com.rudderstack.android.sdk.core.RudderMessageBuilder;
import com.rudderstack.android.sdk.core.RudderOption;
import com.rudderstack.android.sdk.core.gson.RudderGson;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Sends a track event whose properties carry {@code sendImmediately: true} straight to the data plane as a one-event
 * batch, instead of writing it to the SDK's database and waiting for the next flush.
 *
 * <p>The message is built by the SDK's own builder and serializer, so it has the same shape, message ID, context and
 * identity as a queued one. It is written to a small pending store before anything else, so it survives the process
 * being killed while it waits or is in flight; pending messages are sent on the next initialization. Any failure
 * (no network, timeout, non-2xx) hands the message to the SDK queue, which persists and retries it.
 */
final class ImmediateEventDispatcher {
  static final String SEND_IMMEDIATELY = "sendImmediately";

  private static final String PENDING_PREFS = "rl_immediate_pending";
  // The SDK's own opt-out flag (RudderPreferenceManager).
  private static final String RUDDER_PREFS = "rl_prefs";
  private static final String RUDDER_OPT_STATUS_KEY = "rl_opt_status";
  private static final int CONNECT_TIMEOUT_MS = 3000;
  private static final int READ_TIMEOUT_MS = 5000;

  // Sends run one at a time so the keep-alive connection is reused; the warm-up never delays them.
  private final ExecutorService sender = Executors.newSingleThreadExecutor();
  private final ExecutorService warmer = Executors.newSingleThreadExecutor();
  private final SharedPreferences pending;
  private final SharedPreferences rudderPrefs;
  private final String authorization;
  private final String batchUrl;
  private final String healthUrl;

  ImmediateEventDispatcher(Context context, String writeKey, String dataPlaneUrl) {
    this.pending = context.getSharedPreferences(PENDING_PREFS, Context.MODE_PRIVATE);
    this.rudderPrefs = context.getSharedPreferences(RUDDER_PREFS, Context.MODE_PRIVATE);
    this.authorization = "Basic " + Base64.encodeToString((writeKey + ":").getBytes(StandardCharsets.UTF_8),
        Base64.NO_WRAP);
    String base = dataPlaneUrl.endsWith("/") ? dataPlaneUrl : dataPlaneUrl + "/";
    this.batchUrl = base + "v1/batch";
    this.healthUrl = base + "health";
  }

  static boolean isImmediate(Map<String, Object> properties) {
    return properties != null && Boolean.TRUE.equals(properties.get(SEND_IMMEDIATELY));
  }

  /**
   * Opens the connection to the data plane in the background, so the first immediate event at a cold start does not
   * pay for DNS, TCP and TLS. The kept-alive connection is reused by the next request.
   */
  void warmUp() {
    warmer.execute(new Runnable() {
      @Override
      public void run() {
        try {
          HttpURLConnection connection = (HttpURLConnection) new URL(healthUrl).openConnection();
          connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
          connection.setReadTimeout(READ_TIMEOUT_MS);
          drain(connection);
        } catch (Exception e) {
          RudderLogger.logDebug("ImmediateEventDispatcher: warm-up failed: " + e);
        }
      }
    });
  }

  void track(String eventName, Map<String, Object> properties, RudderOption options) {
    RudderMessageBuilder builder = new RudderMessageBuilder().setEventName(eventName).setProperty(properties);
    if (options != null) builder.setRudderOption(options);
    final RudderMessage message = builder.build();
    final JsonObject event = toEvent(message);
    final String messageId = event != null && event.has("messageId") ? event.get("messageId").getAsString() : null;
    if (messageId == null || rudderPrefs.getBoolean(RUDDER_OPT_STATUS_KEY, false)) {
      // Opted out (the SDK drops it) or unserializable: the SDK path decides.
      RudderClient.getInstance().track(message);
      return;
    }
    pending.edit().putString(messageId, event.toString()).apply();
    final long trackedAt = SystemClock.elapsedRealtime();
    sender.execute(new Runnable() {
      @Override
      public void run() {
        if (deliver(event)) {
          RudderLogger.logDebug("ImmediateEventDispatcher: " + message.getEventName() + " delivered in "
              + (SystemClock.elapsedRealtime() - trackedAt) + " ms");
        } else {
          RudderClient.getInstance().track(message);
        }
        pending.edit().remove(messageId).apply();
      }
    });
  }

  /**
   * Sends messages a killed process left pending, exactly as they were stored (same session and context); one that
   * still fails is handed to the SDK queue.
   */
  void sendPending() {
    final Map<String, ?> stored = pending.getAll();
    if (stored.isEmpty()) return;
    sender.execute(new Runnable() {
      @Override
      public void run() {
        for (Map.Entry<String, ?> entry : stored.entrySet()) {
          try {
            JsonObject event = JsonParser.parseString(String.valueOf(entry.getValue())).getAsJsonObject();
            if (!deliver(event)) {
              RudderMessage message = RudderGson.deserialize(event.toString(), RudderMessage.class);
              if (message != null) RudderClient.getInstance().track(message);
            }
          } catch (Exception e) {
            RudderLogger.logError(e);
          }
          pending.edit().remove(entry.getKey()).apply();
        }
      }
    });
  }

  // The message as the SDK would upload it, with the fields the SDK adds while processing a queued message.
  private static JsonObject toEvent(RudderMessage message) {
    String messageJson = RudderGson.serialize(message);
    if (messageJson == null) return null;
    JsonObject event = JsonParser.parseString(messageJson).getAsJsonObject();
    event.addProperty("type", "track");
    if (!event.has("integrations") || event.getAsJsonObject("integrations").size() == 0) {
      JsonObject integrations = new JsonObject();
      integrations.addProperty("All", true);
      event.add("integrations", integrations);
    }
    Long sessionId = RudderClient.getInstance().getSessionId();
    if (sessionId != null && event.has("context")) {
      event.getAsJsonObject("context").addProperty("sessionId", sessionId);
    }
    return event;
  }

  private boolean deliver(JsonObject event) {
    try {
      String sentAt = timestamp();
      event.addProperty("sentAt", sentAt);
      JsonArray events = new JsonArray();
      events.add(event);
      JsonObject batch = new JsonObject();
      batch.addProperty("sentAt", sentAt);
      batch.add("batch", events);
      return post(batch.toString());
    } catch (Exception e) {
      RudderLogger.logWarn("ImmediateEventDispatcher: queued after " + e);
      return false;
    }
  }

  private boolean post(String body) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) new URL(batchUrl).openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
      connection.setReadTimeout(READ_TIMEOUT_MS);
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json; charset=utf-8");
      connection.setRequestProperty("Authorization", authorization);
      byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      connection.setFixedLengthStreamingMode(bytes.length);
      try (OutputStream out = connection.getOutputStream()) {
        out.write(bytes);
      }
      int code = connection.getResponseCode();
      if (code < 200 || code >= 300) {
        RudderLogger.logWarn("ImmediateEventDispatcher: data plane returned " + code + ", queued instead");
        return false;
      }
      return true;
    } finally {
      drain(connection);
    }
  }

  // Reads the body to the end instead of disconnecting, which returns the socket to the keep-alive pool.
  private static void drain(HttpURLConnection connection) {
    try {
      InputStream in = connection.getResponseCode() < 400 ? connection.getInputStream() : connection.getErrorStream();
      if (in == null) return;
      byte[] buffer = new byte[512];
      while (in.read(buffer) != -1) {
        // discard
      }
      in.close();
    } catch (Exception e) {
      connection.disconnect();
    }
  }

  private static String timestamp() {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    return format.format(new Date());
  }
}
