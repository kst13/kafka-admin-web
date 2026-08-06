export function sumLag(lags: { lag: number }[]): number {
  return lags.reduce((acc, p) => acc + p.lag, 0)
}
