import { describe, expect, it } from 'vitest'

import { CSVCache, CSVRange } from '../src/cache.js'

describe('CSVRange', () => {
  it('should initialize correctly', () => {
    const range = new CSVRange({ firstByte: 0, firstRow: 0 })
    expect(range.rowCount).toBe(0)
    expect(range.byteCount).toBe(0)
    expect(range.firstRow).toBe(0)
    expect(range.next).toStrictEqual({ firstByte: 0, row: 0 })
    expect(range.rows).toEqual([])
    expect(range.getCells({ row: 0 })).toBeUndefined()
  })

  it('should initialize correctly at a random position', () => {
    const range = new CSVRange({ firstByte: 100, firstRow: 10 })
    expect(range.rowCount).toBe(0)
    expect(range.byteCount).toBe(0)
    expect(range.firstRow).toBe(10)
    expect(range.next).toStrictEqual({ firstByte: 100, row: 10 })
    expect(range.rows).toStrictEqual([])
    expect(range.getCells({ row: 10 })).toBeUndefined()
  })

  it('should add rows correctly', () => {
    const range = new CSVRange({ firstByte: 100, firstRow: 10 })
    range.append({
      byteOffset: 100,
      byteCount: 10,
    })
    range.append({
      cells: ['d', 'e', 'f'],
      byteOffset: 110,
      byteCount: 10,
    })
    range.prepend({
      cells: ['1', '2', '3'],
      byteOffset: 90,
      byteCount: 10,
    })
    range.prepend({
      byteOffset: 80,
      byteCount: 10,
    })
    expect(range.rowCount).toBe(2)
    expect(range.byteCount).toBe(40)
    expect(range.firstRow).toBe(9)
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
    expect(range1.firstRow).toBe(0)
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
  describe('constructor', () => {
    it('should initialize correctly', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        initialByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      expect(cache.columnNames).toEqual(['col1', 'col2', 'col3'])
      expect(cache.rowCount).toBe(0)
      expect(cache.delimiter).toBe(',')
      expect(cache.newline).toBe('\n')
      expect(cache.averageRowByteCount).toBe(undefined)
      expect(cache.getCell({ row: 0, column: 0 })).toBeUndefined()
      expect(cache.getRowNumber({ row: 0 })).toBeUndefined()
      expect(cache.getNextMissingRow({ rowStart: 0, rowEnd: 10 })).toEqual({ firstByte: 15, isEstimate: false })
      // As no rows are cached, any range should return the same firstByte
      expect(cache.getNextMissingRow({ rowStart: 100, rowEnd: 200 })).toEqual({ firstByte: 15, isEstimate: false })
    })

    it('should initialize from header correctly', () => {
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
      const cache = CSVCache.fromHeader({ header, byteLength: 100 })
      expect(cache.columnNames).toEqual(['col1', 'col2', 'col3'])
      expect(cache.rowCount).toBe(0)
      expect(cache.delimiter).toBe(',')
      expect(cache.newline).toBe('\n')
      expect(cache.averageRowByteCount).toBe(undefined)
      expect(cache.getCell({ row: 0, column: 0 })).toBeUndefined()
      expect(cache.getRowNumber({ row: 0 })).toBeUndefined()
      expect(cache.getNextMissingRow({ rowStart: 0, rowEnd: 10 })).toEqual({ firstByte: 15, isEstimate: false })
      // As no rows are cached, any range should return the same firstByte
      expect(cache.getNextMissingRow({ rowStart: 100, rowEnd: 200 })).toEqual({ firstByte: 15, isEstimate: false })
    })

    it.each([
      { columnNames: [] },
      { initialByteCount: 200 },
    ])('throws when initializing from invalid options: %o', (options) => {
      expect(() => {
        new CSVCache({
          columnNames: options.columnNames ?? ['a', 'b', 'c'],
          byteLength: 100,
          initialByteCount: options.initialByteCount ?? 15,
          delimiter: ',',
          newline: '\n' as const,
        })
      }).toThrow()
    })
  })

  describe('store and retrieve rows', () => {
    it('should store and retrieve rows correctly', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        initialByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      // should be row 0
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 15,
        byteCount: 0, // not forbidden
      })
      // The average row byte count should be 0
      expect(cache.averageRowByteCount).toBe(0)
      // should be row 1
      cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 15,
        byteCount: 20,
      })
      // The average row byte count should be 10 now
      expect(cache.averageRowByteCount).toBe(10)
      // the first row must be retrieved correctly
      expect(cache.getCell({ row: 0, column: 0 })).toStrictEqual({ value: 'a' })
      expect(cache.getCell({ row: 0, column: 1 })).toStrictEqual({ value: 'b' })
      expect(cache.getCell({ row: 0, column: 2 })).toStrictEqual({ value: 'c' })
      expect(cache.getRowNumber({ row: 0 })).toStrictEqual({ value: 0 })
      // the second row must be retrieved correctly
      expect(cache.getCell({ row: 1, column: 0 })).toStrictEqual({ value: 'd' })
      expect(cache.getCell({ row: 1, column: 1 })).toStrictEqual({ value: 'e' })
      expect(cache.getCell({ row: 1, column: 2 })).toStrictEqual({ value: 'f' })
      expect(cache.getRowNumber({ row: 1 })).toStrictEqual({ value: 1 })

      // This row should be in a new random range, and the estimated row number should be 3
      cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 44,
        byteCount: 7,
      })
      // the average row byte count should be 9 now
      expect(cache.averageRowByteCount).toBe(9)
      // it should be retrieved correctly with row: 2
      expect(cache.getCell({ row: 3, column: 0 })).toStrictEqual({ value: 'd' })
      expect(cache.getCell({ row: 3, column: 1 })).toStrictEqual({ value: 'e' })
      expect(cache.getCell({ row: 3, column: 2 })).toStrictEqual({ value: 'f' })
      expect(cache.getRowNumber({ row: 3 })).toStrictEqual({ value: 3 })
    })

    it('should report if all the rows are cached', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        initialByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      expect(cache.allRowsCached).toBe(false)
      // Cache some rows
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 10,
        byteCount: 10,
      })
      cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 20,
        byteCount: 10,
      })
      expect(cache.allRowsCached).toBe(false)
      // Simulate caching all rows by adjusting byteLength and storing a row at the end
      cache.store({
        cells: ['x', 'y', 'z'],
        byteOffset: 30,
        byteCount: 70,
      })
      expect(cache.allRowsCached).toBe(true)
    })

    it.each([
      { byteOffset: -10, byteCount: 10 },
      { byteOffset: 10, byteCount: -10 },
      { byteOffset: 100, byteCount: 60 },
      { byteOffset: 95, byteCount: 10 },
    ])('throws if trying to store a row outside of the cache bounds: %o', ({ byteOffset, byteCount }) => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        initialByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      expect(() => {
        cache.store({
          cells: ['a', 'b', 'c'],
          byteOffset,
          byteCount,
        })
      }).toThrow()
    })

    it.each([
      { initialRow: { byteOffset: 10, byteCount: 10 }, row: { byteOffset: 5, byteCount: 10 }, expected: 'Cannot store the row: overlap with previous range' },
      { initialRow: { byteOffset: 20, byteCount: 10 }, row: { byteOffset: 5, byteCount: 10 }, expected: 'Cannot store the row: overlap with previous range' },
      { initialRow: { byteOffset: 10, byteCount: 10 }, row: { byteOffset: 15, byteCount: 10 }, expected: 'Cannot store the row: overlap with previous range' },
      { initialRow: { byteOffset: 20, byteCount: 10 }, row: { byteOffset: 15, byteCount: 10 }, expected: 'Cannot store the row: overlap with next range' },
      { initialRow: { byteOffset: 20, byteCount: 10 }, row: { byteOffset: 25, byteCount: 10 }, expected: 'Cannot store the row: overlap with next range' },
    ])('throws when storing rows that overlap existing cached rows: %o', ({ initialRow, row, expected }) => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 200,
        initialByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store an initial row
      cache.store(initialRow)
      expect(() => {
        cache.store(row)
      }).toThrow(expected)
    })

    it('shoud merge two adjacent ranges when storing rows', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        initialByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store first row
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 10,
        byteCount: 10,
      })
      // Store third row, creating a new random range
      cache.store({
        cells: ['e', 'f', 'g'],
        byteOffset: 40,
        byteCount: 60,
      })
      // At this point, we should have two ranges, and not all the rows have been cached
      expect(cache.allRowsCached).toBe(false)
      expect(cache.rowCount).toBe(2)
      // Now store the second row, which should merge the two ranges
      cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 20,
        byteCount: 20,
      })
      // now, the cache should include all the rows
      expect(cache.allRowsCached).toBe(true)
      expect(cache.rowCount).toBe(3)
    })

    it('should prepend rows to a random range correctly', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        initialByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store a row to create a random range
      cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 40,
        byteCount: 10,
      })
      // Prepend a row to the random range
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 30,
        byteCount: 10,
      })
      // Check that both rows are stored correctly
      expect(cache.getCell({ row: 2, column: 0 })).toStrictEqual({ value: 'a' })
      expect(cache.getCell({ row: 2, column: 1 })).toStrictEqual({ value: 'b' })
      expect(cache.getCell({ row: 2, column: 2 })).toStrictEqual({ value: 'c' })
      expect(cache.getCell({ row: 3, column: 0 })).toStrictEqual({ value: 'd' })
      expect(cache.getCell({ row: 3, column: 1 })).toStrictEqual({ value: 'e' })
      expect(cache.getCell({ row: 3, column: 2 })).toStrictEqual({ value: 'f' })
    })

    it('should store rows after checking multiple random ranges', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 300,
        initialByteCount: 0,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store first row to create first random range
      cache.store({
        byteOffset: 10,
        byteCount: 10,
      })
      // Store second row to create second random range
      cache.store({
        byteOffset: 30,
        byteCount: 10,
      })
      // Store third row to create third random range
      cache.store({
        byteOffset: 50,
        byteCount: 10,
      })
    })
  })
})
