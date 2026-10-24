import { Effect } from "effect";

export const hello = Effect.gen(function* () {
  yield* Effect.log("Hello, World!");
  return "Hello, World!";
});

if (import.meta.url === `file://${process.argv[1]}`) {
  Effect.runPromise(hello);
}
