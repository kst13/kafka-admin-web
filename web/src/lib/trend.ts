// SVG polyline points 문자열 생성. y축은 0 ~ max(values) 범위(패딩 안쪽).
export function toPolyline(values: number[], w: number, h: number, pad: number): string {
  if (values.length === 0) return ''
  const innerW = w - pad * 2
  const innerH = h - pad * 2
  const max = Math.max(...values)
  const min = Math.min(...values)
  if (values.length === 1 || max === min) {
    const y = pad + innerH / 2
    return `${pad},${y} ${w - pad},${y}`
  }
  return values
    .map((v, i) => {
      const x = pad + (innerW * i) / (values.length - 1)
      const y = pad + innerH - (innerH * v) / max
      return `${x},${y}`
    })
    .join(' ')
}
