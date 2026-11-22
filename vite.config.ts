import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// BASE_PATH is set by the GitHub Pages workflow to "/<repo-name>/".
export default defineConfig({
  base: process.env.BASE_PATH ?? "/",
  plugins: [react(), tailwindcss()],
  // CodeMirror + language packs are ~1 MB; acceptable for a static tool page.
  build: { chunkSizeWarningLimit: 1500 },
  test: {
    include: ["tests/unit/**/*.test.ts"],
    environment: "node",
  },
});
