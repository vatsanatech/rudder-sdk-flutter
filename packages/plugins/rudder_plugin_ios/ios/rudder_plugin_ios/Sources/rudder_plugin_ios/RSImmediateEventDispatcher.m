#import "RSImmediateEventDispatcher.h"

static NSString *const kSendImmediately = @"sendImmediately";
static NSString *const kPendingSuite = @"rl_immediate_pending";
static NSString *const kPendingKey = @"messages";
static const NSTimeInterval kRequestTimeout = 5;

@implementation RSImmediateEventDispatcher {
    dispatch_queue_t _queue;
    NSURLSession *_session;
    NSUserDefaults *_pending;
    NSString *_authorization;
    NSURL *_batchUrl;
    NSURL *_healthUrl;
}

- (instancetype)initWithWriteKey:(NSString *)writeKey dataPlaneUrl:(NSString *)dataPlaneUrl {
    self = [super init];
    if (self) {
        _queue = dispatch_queue_create("com.rudderstack.flutter.immediate", DISPATCH_QUEUE_SERIAL);
        NSURLSessionConfiguration *configuration = [NSURLSessionConfiguration defaultSessionConfiguration];
        configuration.timeoutIntervalForRequest = kRequestTimeout;
        configuration.timeoutIntervalForResource = kRequestTimeout * 2;
        _session = [NSURLSession sessionWithConfiguration:configuration];
        _pending = [[NSUserDefaults alloc] initWithSuiteName:kPendingSuite];
        NSData *credentials = [[NSString stringWithFormat:@"%@:", writeKey] dataUsingEncoding:NSUTF8StringEncoding];
        _authorization = [NSString stringWithFormat:@"Basic %@", [credentials base64EncodedStringWithOptions:0]];
        NSString *base = [dataPlaneUrl hasSuffix:@"/"] ? dataPlaneUrl : [dataPlaneUrl stringByAppendingString:@"/"];
        _batchUrl = [NSURL URLWithString:[base stringByAppendingString:@"v1/batch"]];
        _healthUrl = [NSURL URLWithString:[base stringByAppendingString:@"health"]];
    }
    return self;
}

+ (BOOL)isImmediate:(NSDictionary *)properties {
    id flag = properties[kSendImmediately];
    return [flag isKindOfClass:[NSNumber class]] && [flag boolValue];
}

- (void)track:(NSString *)eventName properties:(NSDictionary *)properties options:(RSOption *)options {
    RSMessageBuilder *builder = [[[RSMessageBuilder alloc] init] setEventName:eventName];
    [builder setPropertyDict:properties];
    if (options != nil) [builder setRSOption:options];
    RSMessage *message = [builder build];
    message.type = @"track";
    NSDate *trackedAt = [NSDate date];
    dispatch_async(_queue, ^{
        [self send:message trackedAt:trackedAt];
    });
}

- (void)warmUp {
    if (_healthUrl == nil) return;
    [[_session dataTaskWithURL:_healthUrl completionHandler:^(NSData *data, NSURLResponse *response, NSError *error) {
        if (error != nil) [RSLogger logDebug:[NSString stringWithFormat:@"RSImmediateEventDispatcher: warm-up failed: %@", error]];
    }] resume];
}

- (void)requeuePending {
    dispatch_async(_queue, ^{
        NSDictionary *stored = [self->_pending dictionaryForKey:kPendingKey];
        for (NSString *messageId in stored) {
            id dict = stored[messageId];
            if ([dict isKindOfClass:[NSDictionary class]]) [self queue:[[RSMessage alloc] initWithDict:dict]];
        }
        [self->_pending removeObjectForKey:kPendingKey];
    });
}

#pragma mark - Private

// Runs on _queue.
- (void)send:(RSMessage *)message trackedAt:(NSDate *)trackedAt {
    NSDictionary *messageDict = [message dict];
    NSString *messageId = message.messageId;
    NSData *body = [self batchBody:messageDict];
    if (messageId == nil || body == nil || _batchUrl == nil || [[RSPreferenceManager getInstance] getOptStatus]) {
        // Opted out (the SDK drops it) or not serializable: the SDK path decides.
        [self queue:message];
        return;
    }
    [self storePending:messageDict messageId:messageId];

    NSMutableURLRequest *request = [NSMutableURLRequest requestWithURL:_batchUrl];
    request.HTTPMethod = @"POST";
    request.HTTPBody = body;
    [request setValue:@"application/json; charset=utf-8" forHTTPHeaderField:@"Content-Type"];
    [request setValue:_authorization forHTTPHeaderField:@"Authorization"];
    [[_session dataTaskWithRequest:request completionHandler:^(NSData *data, NSURLResponse *response, NSError *error) {
        NSInteger status = [response isKindOfClass:[NSHTTPURLResponse class]] ? ((NSHTTPURLResponse *)response).statusCode : 0;
        BOOL delivered = error == nil && status >= 200 && status < 300;
        dispatch_async(self->_queue, ^{
            if (delivered) {
                NSString *line = [NSString stringWithFormat:@"RSImmediateEventDispatcher: %@ delivered in %.0f ms",
                                  message.event, -[trackedAt timeIntervalSinceNow] * 1000];
                [RSLogger logDebug:line];
            } else {
                [RSLogger logWarn:[NSString stringWithFormat:@"RSImmediateEventDispatcher: %@ queued (status %ld, %@)",
                                   message.event, (long)status, error.localizedDescription]];
                [self queue:message];
            }
            [self removePending:messageId];
        });
    }] resume];
}

// The fields the SDK adds while processing a queued message, which a directly sent one would otherwise lack.
- (NSData *)batchBody:(NSDictionary *)messageDict {
    NSMutableDictionary *event = [messageDict mutableCopy];
    event[@"type"] = @"track";
    NSDictionary *integrations = event[@"integrations"];
    if (![integrations isKindOfClass:[NSDictionary class]] || integrations.count == 0) event[@"integrations"] = @{@"All": @YES};
    NSNumber *sessionId = [RSClient sharedInstance].sessionId;
    if (sessionId != nil && [event[@"context"] isKindOfClass:[NSDictionary class]]) {
        NSMutableDictionary *context = [event[@"context"] mutableCopy];
        context[@"sessionId"] = sessionId;
        event[@"context"] = context;
    }
    NSString *sentAt = [RSUtils getTimestamp];
    event[@"sentAt"] = sentAt;
    NSDictionary *batch = @{@"sentAt": sentAt, @"batch": @[event]};
    if (![NSJSONSerialization isValidJSONObject:batch]) return nil;
    return [NSJSONSerialization dataWithJSONObject:batch options:0 error:nil];
}

- (void)queue:(RSMessage *)message {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
    // The only SDK entry point that keeps the message ID, so a retry after a lost response is deduplicated.
    [[RSClient sharedInstance] trackMessage:message];
#pragma clang diagnostic pop
}

- (void)storePending:(NSDictionary *)messageDict messageId:(NSString *)messageId {
    if (![NSJSONSerialization isValidJSONObject:messageDict]) return;
    NSMutableDictionary *stored = [[_pending dictionaryForKey:kPendingKey] mutableCopy] ?: [NSMutableDictionary dictionary];
    stored[messageId] = messageDict;
    [_pending setObject:stored forKey:kPendingKey];
}

- (void)removePending:(NSString *)messageId {
    NSMutableDictionary *stored = [[_pending dictionaryForKey:kPendingKey] mutableCopy];
    if (stored[messageId] == nil) return;
    [stored removeObjectForKey:messageId];
    [_pending setObject:stored forKey:kPendingKey];
}

@end
