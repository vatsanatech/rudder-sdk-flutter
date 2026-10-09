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
 * identity as a queued one. Any failure (no network, timeout, non-2xx) hands the same message to the SDK queue, which
 * persists and retries it. A message is kept in a small pending store while its request is in flight, and pending
 * messages are queued on the next initialization, so a process killed mid-request loses nothing.
 */
final class ImmediateEventDispatcher {
  static final String SEND_IMMEDIATELY = "sendImmediately";

  private static final String PENDING_PREFS = "rl_immediate_pending";
  // The SDK's own opt-out flag (RudderPreferenceManager).
  private static final String RUDDER_PREFS = "rl_prefs";
  private static final String RUDDER_OPT_STATUS_KEY = "rl_opt_status";
  private static final int CONNECT_TIMEOUT_MS = 3000;
  private static final int READ_TIMEOUT_MS = 5000;

  private final ExecutorService executor = Executors.newSingleThreadExecutor();
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

  /**
   * Opens the connection to the data plane in the background, so the first immediate event at a cold start does not
   * pay for DNS, TCP and TLS. The kept-alive connection is reused by the next request.
   */
  void warmUp() {
    executor.execute(new Runnable() {
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

  static boolean isImmediate(Map<String, Object> properties) {
    return properties != null && Boolean.TRUE.equals(properties.get(SEND_IMMEDIATELY));
  }

  void track(String eventName, Map<String, Object> properties, RudderOption options) {
    RudderMessageBuilder builder = new RudderMessageBuilder().setEventName(eventName).setProperty(properties);
    if (options != null) builder.setRudderOption(options);
    final RudderMessage message = builder.build();
    final long trackedAt = SystemClock.elapsedRealtime();
    executor.execute(new Runnable() {
      @Override
      public void run() {
        send(message, trackedAt);
      }
    });
  }

  /** Queues messages whose request never completed, e.g. because the process was killed. */
  void requeuePending() {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        for (Map.Entry<String, ?> entry : pending.getAll().entrySet()) {
          try {
            RudderMessage message = RudderGson.deserialize(String.valueOf(entry.getValue()), RudderMessage.class);
            if (message != null) RudderClient.getInstance().track(message);
          } catch (Exception e) {
            RudderLogger.logError(e);
          }
          pending.edit().remove(entry.getKey()).commit();
        }
      }
    });
  }

  private void send(RudderMessage message, long trackedAt) {
    String messageJson = RudderGson.serialize(message);
    JsonObject event = messageJson == null ? null : JsonParser.parseString(messageJson).getAsJsonObject();
    String messageId = event != null && event.has("messageId") ? event.get("messageId").getAsString() : null;
    if (messageId == null || rudderPrefs.getBoolean(RUDDER_OPT_STATUS_KEY, false)) {
      // Opted out (the SDK drops it) or unserializable: the SDK path decides.
      RudderClient.getInstance().track(message);
      return;
    }
    pending.edit().putString(messageId, messageJson).commit();
    boolean delivered = false;
    try {
      delivered = post(batchBody(event));
    } catch (Exception e) {
      RudderLogger.logWarn("ImmediateEventDispatcher: " + message.getEventName() + " queued after " + e);
    }
    if (delivered) {
      String line = "ImmediateEventDispatcher: " + message.getEventName() + " delivered in "
          + (SystemClock.elapsedRealtime() - trackedAt) + " ms";
      RudderLogger.logDebug(line);
    } else {
      RudderClient.getInstance().track(message);
    }
    pending.edit().remove(messageId).commit();
  }

  // The fields the SDK adds while processing a queued message, which a directly sent one would otherwise lack.
  private String batchBody(JsonObject event) {
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
    String sentAt = timestamp();
    event.addProperty("sentAt", sentAt);
    JsonObject batch = new JsonObject();
    batch.addProperty("sentAt", sentAt);
    JsonArray events = new JsonArray();
    events.add(event);
    batch.add("batch", events);
    return batch.toString();
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
