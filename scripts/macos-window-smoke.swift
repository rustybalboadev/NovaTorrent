// Run from a logged-in macOS GUI session:
// swift scripts/macos-window-smoke.swift /path/to/NovaTorrent.app
import AppKit
import CoreGraphics

guard CommandLine.arguments.count == 2 else {
    fputs("Usage: swift macos-window-smoke.swift /path/to/NovaTorrent.app\n", stderr)
    exit(2)
}
let bundleURL = URL(fileURLWithPath: CommandLine.arguments[1]).standardizedFileURL
guard let bundle = Bundle(url: bundleURL), let identifier = bundle.bundleIdentifier else {
    fatalError("Not an application bundle: \(bundleURL.path)")
}
guard NSRunningApplication.runningApplications(withBundleIdentifier: identifier).isEmpty else {
    fatalError("Quit NovaTorrent before testing; an existing process can mask a launch failure")
}

var application: NSRunningApplication?
var launchError: Error?
NSWorkspace.shared.openApplication(at: bundleURL, configuration: .init()) { app, error in
    application = app
    launchError = error
}
let deadline = Date().addingTimeInterval(30)
var visibleSince: Date?
while Date() < deadline {
    RunLoop.current.run(until: Date().addingTimeInterval(0.2))
    if let error = launchError { fatalError("Launch failed: \(error)") }
    guard let app = application else { continue }
    if app.isTerminated { fatalError("Application exited before showing its main window") }
    let windows = CGWindowListCopyWindowInfo([.optionOnScreenOnly, .excludeDesktopElements], kCGNullWindowID)
        as? [[String: Any]] ?? []
    let mainWindow = windows.first { window in
        guard window[kCGWindowOwnerPID as String] as? Int32 == app.processIdentifier,
              window[kCGWindowLayer as String] as? Int == 0,
              let bounds = window[kCGWindowBounds as String] as? [String: Any],
              let width = bounds["Width"] as? Double,
              let height = bounds["Height"] as? Double else { return false }
        return width >= 960 && height >= 620
    }
    if mainWindow != nil {
        if visibleSince == nil { visibleSince = Date() }
        if Date().timeIntervalSince(visibleSince!) >= 2 {
            print("PASS: \(bundleURL.path) has a visible main window (PID \(app.processIdentifier))")
            app.terminate()
            exit(0)
        }
    } else {
        visibleSince = nil
    }
}
application?.terminate()
fputs("FAIL: no persistent on-screen main window within 30 seconds\n", stderr)
exit(1)
