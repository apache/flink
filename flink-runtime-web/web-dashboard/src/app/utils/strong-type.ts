export function typeDefinition<T>(): (item: unknown) => T {
  return item => item as T;
}
