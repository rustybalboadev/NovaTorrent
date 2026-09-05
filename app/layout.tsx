import type { Metadata } from "next";
import "./globals.css";
import { ThemeProvider } from "@/components/theme-provider";

const themeBootstrapScript = `
(function () {
  var root = document.documentElement;
  var resolved = "light";
  try {
    var stored = window.localStorage.getItem("theme");
    resolved = stored === "light" || stored === "dark"
      ? stored
      : (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
  } catch (_) {
    resolved = window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  root.classList.remove("light", "dark");
  root.classList.add(resolved);
  root.style.colorScheme = resolved;
})();`;

export const metadata: Metadata = {
  title: "NovaTorrent",
  description: "A modern Rust and Tauri BitTorrent client.",
  icons: {
    icon: "/novatorrent-logo.png",
    shortcut: "/novatorrent-logo.png",
    apple: "/novatorrent-logo.png"
  }
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <script dangerouslySetInnerHTML={{ __html: themeBootstrapScript }} />
      </head>
      <body>
        <ThemeProvider attribute="class" defaultTheme="system" enableSystem disableTransitionOnChange>
          {children}
        </ThemeProvider>
      </body>
    </html>
  );
}
