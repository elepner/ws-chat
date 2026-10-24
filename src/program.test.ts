import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";

const hello = Effect.gen(function* () {
  yield* Effect.log("Hello, World!");
  return "Hello, World!";
});

describe("hello", () => {
  it.effect("returns the greeting", () =>
    Effect.gen(function* () {
      const result = yield* hello;
      expect(result).toBe("Hello, World!");
    }),
  );
});
