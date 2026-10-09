#import <Foundation/Foundation.h>
#if __has_include(<Rudder/Rudder.h>)
#import <Rudder/Rudder.h>
#else
#import <Rudder.h>
#endif

NS_ASSUME_NONNULL_BEGIN

/// Sends a track event whose properties carry `sendImmediately: true` straight to the data plane as a one-event batch,
/// instead of writing it to the SDK's database and waiting for the next flush.
///
/// The message is built by the SDK's own builder, so it has the same shape, message ID, context and identity as a
/// queued one. Any failure (no network, timeout, non-2xx) hands the same message to the SDK queue, which persists and
/// retries it. A message is kept in a small pending store while its request is in flight, and pending messages are
/// queued on the next initialization, so a process killed mid-request loses nothing.
@interface RSImmediateEventDispatcher : NSObject

- (instancetype)initWithWriteKey:(NSString *)writeKey dataPlaneUrl:(NSString *)dataPlaneUrl;
+ (BOOL)isImmediate:(nullable NSDictionary *)properties;
- (void)track:(NSString *)eventName properties:(NSDictionary *)properties options:(nullable RSOption *)options;
/// Opens the connection to the data plane in the background, so the first immediate event does not pay for it.
- (void)warmUp;
/// Queues messages whose request never completed, e.g. because the process was killed.
- (void)requeuePending;

@end

NS_ASSUME_NONNULL_END
