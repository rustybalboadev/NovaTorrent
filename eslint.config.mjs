import nextVitals from "eslint-config-next/core-web-vitals";

const config = [
  {
    ignores: [".next/**", "out/**", "node_modules/**", "src-tauri/**"]
  },
  ...nextVitals
];

export default config;
