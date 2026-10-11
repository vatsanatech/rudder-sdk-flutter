package com.rudderstack.sdk.flutter;

import android.content.Context;
import android.content.SharedPreferences;
import android.net.SSLCertificateSocketFactory;
import android.net.SSLSessionCache;
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

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

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

  // Sends run one at a time so the keep-alive connection is reused; the warm-up (its own thread) never delays them.
  private final ExecutorService sender = Executors.newSingleThreadExecutor();
  private final SharedPreferences pending;
  private final SSLSocketFactory sessionCachingFactory;
  private final SharedPreferences rudderPrefs;
  private final String authorization;
  private final String batchUrl;
  private final String healthUrl;

  private static ImmediateEventDispatcher shared;
  private static final java.util.concurrent.atomic.AtomicBoolean warmed = new java.util.concurrent.atomic.AtomicBoolean();

  /**
   * The process's dispatcher, created once by whichever starts first: the plugin's initialization from Dart, or an
   * event sent before the Flutter engine starts ({@link EarlyEvents}). Creating it warms the connection and sends
   * what a killed process left pending.
   */
  static synchronized ImmediateEventDispatcher shared(Context context, String writeKey, String dataPlaneUrl) {
    if (shared == null) {
      shared = new ImmediateEventDispatcher(context.getApplicationContext(), writeKey, dataPlaneUrl);
      shared.warmUp();
      shared.sendPending();
    }
    return shared;
  }

  /**
   * Sends an event built without the client ({@link EarlyEvents#sendFirst}) as the process's first request to the data
   * plane: no warm-up request runs ahead of it, and messages a killed process left pending are sent after it.
   */
  static synchronized void sendFirst(Context context, String writeKey, String dataPlaneUrl, JsonObject event,
      String messageId) {
    warmed.set(true);
    boolean created = shared == null;
    // From Application.attachBaseContext there is no application context yet; the base context serves the same files.
    Context appContext = context.getApplicationContext() != null ? context.getApplicationContext() : context;
    if (created) shared = new ImmediateEventDispatcher(appContext, writeKey, dataPlaneUrl);
    shared.sendStored(event, messageId);
    if (created) shared.sendPending(messageId);
  }

  // Stored pending first, so a process killed while it is in flight loses nothing; one not delivered stays pending and
  // the next process sends it (there is no client yet to queue it with).
  private void sendStored(final JsonObject event, final String messageId) {
    pending.edit().putString(messageId, event.toString()).apply();
    sender.execute(new Runnable() {
      @Override
      public void run() {
        if (deliver(event)) pending.edit().remove(messageId).apply();
      }
    });
  }

  ImmediateEventDispatcher(Context context, String writeKey, String dataPlaneUrl) {
    this.pending = context.getSharedPreferences(PENDING_PREFS, Context.MODE_PRIVATE);
    this.sessionCachingFactory = sessionCachingFactory(context);
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
    warmUp(healthUrl);
  }

  /**
   * Opens the connection to the data plane once per process, before anything else needs it: an event sent before the
   * Flutter engine starts then reuses it instead of paying for DNS, TCP and TLS itself.
   */
  static void warmUp(String dataPlaneUrl) {
    if (dataPlaneUrl == null || !warmed.compareAndSet(false, true)) return;
    final String healthUrl = (dataPlaneUrl.endsWith("/") ? dataPlaneUrl : dataPlaneUrl + "/") + "health";
    new Thread(new Runnable() {
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
    }, "rl-warm-up").start();
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
    sendPending(null);
  }

  // Except the message being sent first; one that still fails is queued, or kept pending while there is no client.
  private void sendPending(String exceptMessageId) {
    final Map<String, Object> stored = new java.util.HashMap<>(pending.getAll());
    if (exceptMessageId != null) stored.remove(exceptMessageId);
    if (stored.isEmpty()) return;
    sender.execute(new Runnable() {
      @Override
      public void run() {
        for (Map.Entry<String, Object> entry : stored.entrySet()) {
          try {
            JsonObject event = JsonParser.parseString(String.valueOf(entry.getValue())).getAsJsonObject();
            if (!deliver(event)) {
              if (RudderClient.getInstance() == null) continue;
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
    if (sessionCachingFactory != null && connection instanceof HttpsURLConnection) {
      ((HttpsURLConnection) connection).setSSLSocketFactory(sessionCachingFactory);
    }
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

  // TLS sessions kept on disk: a new process resumes the last session with the data plane, a round trip and a
  // certificate verification shorter than a full handshake. Null where it cannot be set up.
  @SuppressWarnings("deprecation")
  private static SSLSocketFactory sessionCachingFactory(Context context) {
    try {
      return SSLCertificateSocketFactory.getDefault(CONNECT_TIMEOUT_MS, new SSLSessionCache(context));
    } catch (Exception e) {
      return null;
    }
  }

  static String timestamp() {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    return format.format(new Date());
  }
}
