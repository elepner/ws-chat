import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";

import { hello } from "./program.js";

describe("hello", () => {
  it.effect("returns the greeting", () =>
    Effect.gen(function* () {
      const result = yield* hello;
      expect(result).toBe("Hello, World!");
    }),
  );
});
