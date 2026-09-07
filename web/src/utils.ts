export const formatStatus = (status: string) => status.replaceAll('_', ' ')

export const severityBand = (severity: number) => {
  if (severity < 1 || severity > 5) throw new RangeError('severity must be between 1 and 5')
  return severity >= 4 ? 'critical' : severity === 3 ? 'urgent' : 'standard'
}
