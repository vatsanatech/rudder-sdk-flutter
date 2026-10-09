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
/// queued one. It is written to a small pending store before anything else, so it survives the process being killed
/// while it waits or is in flight; pending messages are sent on the next initialization. Any failure (no network,
/// timeout, non-2xx) hands the message to the SDK queue, which persists and retries it.
@interface RSImmediateEventDispatcher : NSObject

- (instancetype)initWithWriteKey:(NSString *)writeKey dataPlaneUrl:(NSString *)dataPlaneUrl;
+ (BOOL)isImmediate:(nullable NSDictionary *)properties;
- (void)track:(NSString *)eventName properties:(NSDictionary *)properties options:(nullable RSOption *)options;
/// Opens the connection to the data plane in the background, so the first immediate event does not pay for it.
- (void)warmUp;
/// Sends messages a killed process left pending, exactly as they were stored; one that still fails is queued.
- (void)sendPending;

@end

NS_ASSUME_NONNULL_END
