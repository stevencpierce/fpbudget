export function money(n: number | null | undefined): string {
  if (n === null || n === undefined) return "—";
  return (
    "$" +
    n.toLocaleString("en-US", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })
  );
}

/** Compact money for headers: $1.2M / $340k / $980.00 */
export function moneyCompact(n: number | null | undefined): string {
  if (n === null || n === undefined) return "—";
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`;
  if (abs >= 10_000) return `$${Math.round(n / 1000).toLocaleString("en-US")}k`;
  return money(n);
}
