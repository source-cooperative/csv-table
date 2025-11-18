import { describe, expect, it } from 'vitest'

import { CSVRange } from '../src/cache.js'

describe('CSVRange', () => {
  it('should initialize correctly', () => {
    const range = new CSVRange({ firstByte: 0, firstRow: 0 })
    expect(range.rowCount).toBe(0)
    expect(range.byteCount).toBe(0)
    expect(range.cachedByteCount).toBe(0)
    expect(range.previous).toBeUndefined()
    expect(range.next).toStrictEqual({ firstByte: 0, row: 0 })
    expect(range.rows).toEqual([])
    expect(range.getCells({ row: 0 })).toBeUndefined()
  })

  it('should initialize correctly at a random position', () => {
    const range = new CSVRange({ firstByte: 100, firstRow: 10 })
    expect(range.rowCount).toBe(0)
    expect(range.byteCount).toBe(0)
    expect(range.cachedByteCount).toBe(0)
    expect(range.previous).toStrictEqual({ lastByte: 99, row: 9 })
    expect(range.next).toStrictEqual({ firstByte: 100, row: 10 })
    expect(range.rows).toStrictEqual([])
    expect(range.getCells({ row: 10 })).toBeUndefined()
  })

  it('should add rows correctly', () => {
    const range = new CSVRange({ firstByte: 100, firstRow: 10 })
    range.append({
      row: ['a', 'b', 'c'],
      errors: [],
      meta: {
        // Number are made up for testing purposes
        byteOffset: 100,
        byteCount: 10,
        charCount: 9,
        delimiter: ',',
        newline: '\n',
      },
    }, { ignore: true })
    range.append({
      row: ['d', 'e', 'f'],
      errors: [],
      meta: {
        // Number are made up for testing purposes
        byteOffset: 110,
        byteCount: 10,
        charCount: 9,
        delimiter: ',',
        newline: '\n',
      },
    })
    range.prepend({
      row: ['1', '2', '3'],
      errors: [],
      meta: {
        // Number are made up for testing purposes
        byteOffset: 90,
        byteCount: 10,
        charCount: 9,
        delimiter: ',',
        newline: '\n',
      },
    })
    range.prepend({
      row: ['4', '5', '6'],
      errors: [],
      meta: {
        // Number are made up for testing purposes
        byteOffset: 80,
        byteCount: 10,
        charCount: 9,
        delimiter: ',',
        newline: '\n',
      },
    }, { ignore: true })
    expect(range.rowCount).toBe(2)
    expect(range.byteCount).toBe(40)
    expect(range.cachedByteCount).toBe(20)
    expect(range.previous).toStrictEqual({ lastByte: 79, row: 8 })
    expect(range.next).toStrictEqual({ firstByte: 120, row: 11 })
    expect(range.rows).toEqual([
      {
        row: ['1', '2', '3'],
        errors: [],
        meta: {
          byteOffset: 90,
          byteCount: 10,
          charCount: 9,
          delimiter: ',',
          newline: '\n',
        },
      },
      {
        row: ['d', 'e', 'f'],
        errors: [],
        meta: {
          byteOffset: 110,
          byteCount: 10,
          charCount: 9,
          delimiter: ',',
          newline: '\n',
        },
      },
    ])
    expect(range.getCells({ row: 7 })).toBeUndefined()
    expect(range.getCells({ row: 8 })).toBeUndefined()
    expect(range.getCells({ row: 9 })).toEqual(['1', '2', '3'])
    expect(range.getCells({ row: 10 })).toEqual(['d', 'e', 'f'])
    expect(range.getCells({ row: 11 })).toBeUndefined()
    expect(range.getCells({ row: 12 })).toBeUndefined()
  })

  it('should throw when adding non-contiguous rows', () => {
    const range = new CSVRange({ firstByte: 100, firstRow: 10 })
    expect(() => {
      range.prepend({
        row: ['x'],
        errors: [],
        meta: {
          byteOffset: 10,
          byteCount: 10,
          charCount: 1,
          delimiter: ',',
          newline: '\n',
        },
      })
    }).toThrow('Cannot prepend the row: it is not contiguous with the first row')
    expect(() => {
      range.append({
        row: ['y'],
        errors: [],
        meta: {
          byteOffset: 120,
          byteCount: 10,
          charCount: 1,
          delimiter: ',',
          newline: '\n',
        },
      })
    }).toThrow('Cannot append the row: it is not contiguous with the last row')
  })
})
