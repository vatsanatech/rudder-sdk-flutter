#import <Flutter/Flutter.h>
#if __has_include(<Rudder/Rudder.h>)
#import <Rudder/Rudder.h>
#else
#import <Rudder.h>
#endif

@interface RudderSdkFlutterPlugin : NSObject <FlutterPlugin>
+ (void)listenAppLaunchNotification:(NSNotification *)notification;
+ (void)addIntegration:(id<RSIntegrationFactory>)integration;
+ (void) setDBEncryption:(RSDBEncryption*)dbEncryption;
/// Sends an immediate event before the Flutter engine starts (e.g. an app open from didFinishLaunching), with the
/// configuration the plugin saved when Dart last initialized it. NO when there is none yet (the first launch of an
/// install) or the app was initialized with database encryption or integrations, which it cannot carry.
+ (BOOL)trackEarlyEvent:(NSString*)eventName properties:(NSDictionary*)properties;
@end
