import { defineConfig } from "vite-plus"

export default defineConfig({
  staged: {
    "*": "vp check --fix",
  },
  fmt: {
    semi: false,
    singleQuote: false,
    printWidth: 80,
    sortPackageJson: false,
    ignorePatterns: ["node_modules"],
  },
})
