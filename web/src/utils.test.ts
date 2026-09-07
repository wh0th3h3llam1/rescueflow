import {describe,expect,it} from 'vitest'
import {formatStatus,severityBand} from './utils'

describe('dashboard formatting',()=>{
  it('formats workflow statuses',()=>expect(formatStatus('ASSIGNMENT_PENDING')).toBe('ASSIGNMENT PENDING'))
  it('validates and groups severity',()=>{expect(severityBand(5)).toBe('critical');expect(()=>severityBand(0)).toThrow(RangeError)})
})
