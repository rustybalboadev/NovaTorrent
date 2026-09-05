(function () {
  var root = document.documentElement;
  var resolved = "light";
  try {
    var stored = window.localStorage.getItem("theme");
    resolved =
      stored === "light" || stored === "dark"
        ? stored
        : window.matchMedia("(prefers-color-scheme: dark)").matches
          ? "dark"
          : "light";
  } catch (_) {
    resolved = window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  root.classList.remove("light", "dark");
  root.classList.add(resolved);
  root.style.colorScheme = resolved;
})();
