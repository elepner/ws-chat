import { defineConfig } from "vite-plus";

export default defineConfig({
  staged: {
    "*": "vp check --fix",
  },
  fmt: {
    semi: true,
    singleQuote: false,
    printWidth: 80,
    sortPackageJson: false,
    sortImports: true,
    ignorePatterns: ["node_modules"],
  },
});
