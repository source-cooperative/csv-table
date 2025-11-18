import { describe, expect, it } from 'vitest'

import { CSVCache, CSVRange } from '../src/cache.js'

describe('CSVRange', () => {
  it('should initialize correctly', () => {
    const range = new CSVRange({ firstByte: 0, firstRow: 0 })
    expect(range.rowCount).toBe(0)
    expect(range.byteCount).toBe(0)
    expect(range.previous).toBeUndefined()
    expect(range.next).toStrictEqual({ firstByte: 0, row: 0 })
    expect(range.rows).toEqual([])
    expect(range.getCells({ row: 0 })).toBeUndefined()
  })

  it('should initialize correctly at a random position', () => {
    const range = new CSVRange({ firstByte: 100, firstRow: 10 })
    expect(range.rowCount).toBe(0)
    expect(range.byteCount).toBe(0)
    expect(range.previous).toStrictEqual({ lastByte: 99, row: 9 })
    expect(range.next).toStrictEqual({ firstByte: 100, row: 10 })
    expect(range.rows).toStrictEqual([])
    expect(range.getCells({ row: 10 })).toBeUndefined()
  })

  it('should add rows correctly', () => {
    const range = new CSVRange({ firstByte: 100, firstRow: 10 })
    range.append({
      // Number are made up for testing purposes
      byteOffset: 100,
      byteCount: 10,
    })
    range.append({
      cells: ['d', 'e', 'f'],
      // Number are made up for testing purposes
      byteOffset: 110,
      byteCount: 10,
    })
    range.prepend({
      cells: ['1', '2', '3'],
      // Number are made up for testing purposes
      byteOffset: 90,
      byteCount: 10,
    })
    range.prepend({
      // Number are made up for testing purposes
      byteOffset: 80,
      byteCount: 10,
    })
    expect(range.rowCount).toBe(2)
    expect(range.byteCount).toBe(40)
    expect(range.previous).toStrictEqual({ lastByte: 79, row: 8 })
    expect(range.next).toStrictEqual({ firstByte: 120, row: 11 })
    expect(range.rows).toEqual([['1', '2', '3'], ['d', 'e', 'f']])
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
        cells: ['x'],
        byteOffset: 10,
        byteCount: 10,
      })
    }).toThrow('Cannot prepend the row: it is not contiguous with the first row')
    expect(() => {
      range.append({
        byteOffset: 120,
        byteCount: 10,
      })
    }).toThrow('Cannot append the row: it is not contiguous with the last row')
  })

  it('should merge with another range correctly', () => {
    const range1 = new CSVRange({ firstByte: 0, firstRow: 0 })
    range1.append({
      byteOffset: 0,
      byteCount: 10,
    })
    range1.append({
      cells: ['b', 'c', 'd'],
      byteOffset: 10,
      byteCount: 10,
    })

    const range2 = new CSVRange({ firstByte: 20, firstRow: 2 })
    range2.append({
      cells: ['e', 'f', 'g'],
      byteOffset: 20,
      byteCount: 10,
    })
    range2.append({
      byteOffset: 30,
      byteCount: 10,
    })

    range1.merge(range2)

    expect(range1.rowCount).toBe(2)
    expect(range1.byteCount).toBe(40)
    expect(range1.previous).toBeUndefined()
    expect(range1.next).toStrictEqual({ firstByte: 40, row: 2 })
    expect(range1.rows).toEqual([['b', 'c', 'd'], ['e', 'f', 'g']])
    expect(range1.getCells({ row: 0 })).toEqual(['b', 'c', 'd'])
    expect(range1.getCells({ row: 1 })).toEqual(['e', 'f', 'g'])
  })

  it('should throw when merging non-contiguous ranges', () => {
    const range1 = new CSVRange({ firstByte: 0, firstRow: 0 })
    const range2 = new CSVRange({ firstByte: 30, firstRow: 3 })
    expect(() => {
      range1.merge(range2)
    }).toThrow('Cannot merge ranges: not contiguous')
  })
})

describe('CSVCache', () => {
  it('should initialize correctly', () => {
    const header = {
      row: ['col1', 'col2', 'col3'],
      errors: [],
      meta: {
        byteOffset: 0,
        byteCount: 15,
        charCount: 14,
        delimiter: ',',
        newline: '\n' as const,
      },
    }
    const cache = new CSVCache({
      header,
      byteLength: 100,
    })
    expect(cache.columnNames).toEqual(['col1', 'col2', 'col3'])
    expect(cache.rowCount).toBe(0)
    expect(cache.delimiter).toBe(',')
    expect(cache.newline).toBe('\n')
    expect(() => cache.estimateNumRows()).toThrow('Cannot estimate number of rows: average row byte count is undefined')
    expect(cache.getCell({ row: 0, column: 0 })).toBeUndefined()
    expect(cache.getRowNumber({ row: 0 })).toBeUndefined()
    expect(cache.getMissingRowRanges({ rowStart: 0, rowEnd: 10 })).toEqual([
      { firstByte: 15, ignoreFirstRow: false, lastByte: 99, ignoreLastRow: false, maxNumRows: 10 },
    ])
  })

  it('should store and retrieve rows correctly', () => {
    const header = {
      row: ['col1', 'col2', 'col3'],
      errors: [],
      meta: {
        byteOffset: 0,
        byteCount: 15,
        charCount: 14,
        delimiter: ',',
        newline: '\n' as const,
      },
    }
    const cache = new CSVCache({
      header,
      byteLength: 100,
    })
    // should be row 0
    cache.store({
      row: ['a', 'b', 'c'],
      errors: [],
      meta: {
        byteOffset: 15,
        byteCount: 10,
        charCount: 9,
        delimiter: ',',
        newline: '\n',
      },
    })
    // Should be a new range
    cache.store({
      row: ['d', 'e', 'f'],
      errors: [],
      meta: {
        byteOffset: 125,
        byteCount: 10,
        charCount: 9,
        delimiter: ',',
        newline: '\n',
      },
    })
  })
})
