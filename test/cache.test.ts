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

  it('firstRow can be set', () => {
    const range = new CSVRange({ firstByte: 0, firstRow: 0 })
    expect(range.firstRow).toBe(0)
    range.firstRow = 5
    expect(range.firstRow).toBe(5)
  })

  it.each([-1, +Infinity, NaN, 1.5])('firstRow setter throws for incorrect value: %d', (value) => {
    const range = new CSVRange({ firstByte: 0, firstRow: 0 })
    expect(range.firstRow).toBe(0)
    expect(() => {
      range.firstRow = value
    }).toThrow()
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
    expect(range1.rowByteCount).toBe(10)

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
    expect(range2.rowByteCount).toBe(10)

    range1.merge(range2)

    expect(range1.rowCount).toBe(2)
    expect(range1.byteCount).toBe(40)
    expect(range1.firstRow).toBe(0)
    expect(range1.rowByteCount).toBe(20)
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
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      expect(cache.columnNames).toEqual(['col1', 'col2', 'col3'])
      expect(cache.rowCount).toBe(0)
      expect(cache.delimiter).toBe(',')
      expect(cache.newline).toBe('\n')
      expect(cache.averageRowByteCount).toBe(undefined)
      expect(cache.headerByteCount).toBe(15)
      expect(cache.allRowsCached).toBe(false)
      expect(cache.numRowsEstimate).toEqual({ numRows: 0, isEstimate: true })
      expect(cache.getCell({ row: 0, column: 0 })).toBeUndefined()
      expect(cache.getRowNumber({ row: 0 })).toBeUndefined()
      expect(cache.getNextMissingRow({ rowStart: 0, rowEnd: 10 })).toEqual({ firstByteToStore: 15, firstByteToFetch: 15, firstRow: 0 })
      // As no rows are cached, any range should return the same firstByte
      expect(cache.getNextMissingRow({ rowStart: 100, rowEnd: 200 })).toEqual({ firstByteToStore: 15, firstByteToFetch: 15, firstRow: 0 })
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
      expect(cache.getNextMissingRow({ rowStart: 0, rowEnd: 10 })).toEqual({ firstByteToStore: 15, firstByteToFetch: 15, firstRow: 0 })
      // As no rows are cached, any range should return the same firstByte
      expect(cache.getNextMissingRow({ rowStart: 100, rowEnd: 200 })).toEqual({ firstByteToStore: 15, firstByteToFetch: 15, firstRow: 0 })
    })

    it.each([
      { columnNames: [] },
      { headerByteCount: 200 },
    ])('throws when initializing from invalid options: %o', (options) => {
      expect(() => {
        new CSVCache({
          columnNames: options.columnNames ?? ['a', 'b', 'c'],
          byteLength: 100,
          headerByteCount: options.headerByteCount ?? 15,
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
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      // should be row 0
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 15,
        byteCount: 0, // not forbidden
        firstRow: 0,
      })
      // The average row byte count should be 0
      expect(cache.averageRowByteCount).toBe(0)
      // should be row 1
      cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 15,
        byteCount: 20,
        firstRow: 1,
      })
      // The average row byte count should be 10 now
      expect(cache.averageRowByteCount).toBe(10)
      expect(cache.numRowsEstimate).toEqual({ numRows: 9, isEstimate: true })
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
        firstRow: 3,
      })
      // the average row byte count should be 9 now
      expect(cache.averageRowByteCount).toBe(9)
      // it should be retrieved correctly with row: 2
      expect(cache.getCell({ row: 3, column: 0 })).toStrictEqual({ value: 'd' })
      expect(cache.getCell({ row: 3, column: 1 })).toStrictEqual({ value: 'e' })
      expect(cache.getCell({ row: 3, column: 2 })).toStrictEqual({ value: 'f' })
      expect(cache.getRowNumber({ row: 3 })).toStrictEqual({ value: 3 })

      // adding an ignored row should not change the average row byte count
      cache.store({
        byteOffset: 60,
        byteCount: 5,
        firstRow: 4,
      })
      expect(cache.averageRowByteCount).toBe(9)
    })

    it('should report if all the rows are cached', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      expect(cache.allRowsCached).toBe(false)
      // Cache some rows
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 10,
        byteCount: 10,
        firstRow: 0,
      })
      cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 20,
        byteCount: 10,
        firstRow: 1,
      })
      expect(cache.allRowsCached).toBe(false)
      // Simulate caching all rows by adjusting byteLength and storing a row at the end
      cache.store({
        cells: ['x', 'y', 'z'],
        byteOffset: 30,
        byteCount: 70,
        firstRow: 2,
      })
      expect(cache.allRowsCached).toBe(true)
      expect(cache.numRowsEstimate).toEqual({ numRows: 3, isEstimate: false })
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
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      expect(() => {
        cache.store({
          cells: ['a', 'b', 'c'],
          byteOffset,
          byteCount,
          firstRow: 0,
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
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store an initial row
      cache.store({ ...initialRow, firstRow: 0 })
      expect(() => {
        cache.store({ ...row, firstRow: 1 })
      }).toThrow(expected)
    })

    it('should merge two adjacent ranges when storing rows', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store first row
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 10,
        byteCount: 10,
        firstRow: 0,
      })
      // Store third row, creating a new random range
      cache.store({
        cells: ['e', 'f', 'g'],
        byteOffset: 40,
        byteCount: 60,
        firstRow: 2,
      })
      // At this point, we should have two ranges, and not all the rows have been cached
      expect(cache.allRowsCached).toBe(false)
      expect(cache.rowCount).toBe(2)
      // Now store the second row, which should merge the two ranges
      cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 20,
        byteCount: 20,
        firstRow: 1,
      })
      // now, the cache should include all the rows
      expect(cache.allRowsCached).toBe(true)
      expect(cache.rowCount).toBe(3)
    })

    it('should prepend rows to a random range correctly', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store a row to create a random range
      cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 40,
        byteCount: 10,
        firstRow: 3,
      })
      // Prepend a row to the random range
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 30,
        byteCount: 10,
        firstRow: 2,
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
        headerByteCount: 0,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store first row to create first random range
      cache.store({
        byteOffset: 10,
        byteCount: 10,
        firstRow: 0,
      })
      // Store second row to create second random range
      cache.store({
        byteOffset: 30,
        byteCount: 10,
        firstRow: 1,
      })
      // Store third row to create third random range
      cache.store({
        byteOffset: 50,
        byteCount: 10,
        firstRow: 2,
      })
    })

    it('should create a random range between existing ones if there is space', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 300,
        headerByteCount: 0,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store first row to create first random range
      cache.store({
        byteOffset: 10,
        byteCount: 10,
        firstRow: 0,
      })
      // Store third row to create second random range
      cache.store({
        byteOffset: 50,
        byteCount: 10,
        firstRow: 2,
      })
      // Now store the second row in between
      cache.store({
        byteOffset: 30,
        byteCount: 10,
        firstRow: 1,
      })
    })
  })

  describe('getCell', () => {
    it('should throw for out-of-bounds rows or columns', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      expect(() => cache.getCell({ row: -1, column: 0 })).toThrow()
      expect(() => cache.getCell({ row: 0, column: -1 })).toThrow()
      expect(() => cache.getCell({ row: 0, column: 3 })).toThrow()
    })

    it('should return undefined for missing row', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store a row
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 10,
        byteCount: 10,
        firstRow: 0,
      })
      // Missing row
      expect(cache.getCell({ row: 1, column: 0 })).toBeUndefined()
    })

    it('should return empty string for missing column', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store a row
      cache.store({
        cells: ['a', 'b'], // missing last column
        byteOffset: 10,
        byteCount: 10,
        firstRow: 0,
      })
      // Missing column
      expect(cache.getCell({ row: 0, column: 2 })).toStrictEqual({ value: '' })
    })

    it('should return the correct cell value for existing rows and columns', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store a row
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 10,
        byteCount: 10,
        firstRow: 0,
      })
      // Existing row and columns
      expect(cache.getCell({ row: 0, column: 0 })).toStrictEqual({ value: 'a' })
      expect(cache.getCell({ row: 0, column: 1 })).toStrictEqual({ value: 'b' })
      expect(cache.getCell({ row: 0, column: 2 })).toStrictEqual({ value: 'c' })
    })
  })

  describe('getRowNumber', () => {
    it('should throw for out-of-bounds rows', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      expect(() => cache.getRowNumber({ row: -1 })).toThrow()
    })

    it('should return undefined for missing row', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store a row
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 10,
        byteCount: 10,
        firstRow: 0,
      })
      // Missing row
      expect(cache.getRowNumber({ row: 1 })).toBeUndefined()
    })

    it('should return the correct row number for existing rows', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store a row
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 10,
        byteCount: 10,
        firstRow: 0,
      })
      // Existing row
      expect(cache.getRowNumber({ row: 0 })).toStrictEqual({ value: 0 })
    })
  })

  describe('getNextMissingRow', () => {
    it.each([
      { range: { rowStart: 0, rowEnd: 10 }, expectedFirstRow: 0 },
      { range: { rowStart: 5, rowEnd: 15 }, expectedFirstRow: 0 },
      { range: { rowStart: 10, rowEnd: 20 }, expectedFirstRow: 0 },
    ])('should propose the first byte if the cache is empty since it cannot estimate positions: %o', ({ range, expectedFirstRow }) => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 200,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      expect(cache.getNextMissingRow(range)).toEqual({ firstByteToFetch: 10, firstByteToStore: 10, firstRow: expectedFirstRow })
    })

    it.each([
      { options: { rowStart: 0, rowEnd: 0 }, expected: undefined },
      { options: { rowStart: 0, rowEnd: 2 }, expected: { firstByteToFetch: 20, firstByteToStore: 20, firstRow: 1 } },
      { options: { rowStart: 1, rowEnd: 2 }, expected: { firstByteToFetch: 20, firstByteToStore: 20, firstRow: 1 } },
      { options: { rowStart: 2, rowEnd: 2 }, expected: undefined },
      { options: { rowStart: 3, rowEnd: 2 }, expected: undefined },
    ])('should propose the next missing row correctly after #serial range: %o', ({ options, expected }) => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 200,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Stored row is in the #serial range, next.firstByte is 20
      cache.store({ byteOffset: 10, byteCount: 10, cells: ['x', 'y', 'z'], firstRow: 0 })
      expect(cache.getNextMissingRow(options)).toEqual(expected)
    })

    it.each([
      { options: { rowStart: 0, rowEnd: 4 }, expected: { firstByteToStore: 10, firstByteToFetch: 10, firstRow: 0 } },
      { options: { rowStart: 1, rowEnd: 4 }, expected: { firstByteToStore: 20, firstByteToFetch: 0, firstRow: 1 } },
      { options: { rowStart: 2, rowEnd: 4 }, expected: { firstByteToStore: 40, firstByteToFetch: 40, firstRow: 3 } },
      { options: { rowStart: 3, rowEnd: 4 }, expected: { firstByteToStore: 40, firstByteToFetch: 40, firstRow: 3 } },
      { options: { rowStart: 4, rowEnd: 5 }, expected: { firstByteToStore: 50, firstByteToFetch: 20, firstRow: 4 } },
    ])('should propose the next missing row correctly around a random range: %o', ({ options, expected }) => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 200,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Stored row is in a random range, next.firstByte is 40
      cache.store({ byteOffset: 30, byteCount: 10, cells: ['x', 'y', 'z'], firstRow: 2 })
      // the average row byte count is now 10
      expect(cache.averageRowByteCount).toBe(10)
      // the estimated row number is 2
      expect(cache.getRowNumber({ row: 2 })).toEqual({ value: 2 })

      expect(cache.getNextMissingRow(options)).toEqual(expected)
    })

    it('should return undefined when all rows are cached', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Simulate caching all rows by storing a row that covers the entire byteLength
      cache.store({
        cells: ['x', 'y', 'z'],
        byteOffset: 10,
        byteCount: 90,
        firstRow: 0,
      })
      expect(cache.allRowsCached).toBe(true)
      expect(cache.getNextMissingRow({ rowStart: 0, rowEnd: 10 })).toBeUndefined()
    })

    it('should return undefined when all rows until rowEnd - 1 is already cached (exclusive)', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 200,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store rows 0, 1, and 2
      cache.store({ byteOffset: 10, byteCount: 10, cells: ['a', 'b', 'c'], firstRow: 0 }) // row 0
      cache.store({ byteOffset: 20, byteCount: 10, cells: ['d', 'e', 'f'], firstRow: 1 }) // row 1
      cache.store({ byteOffset: 30, byteCount: 10, cells: ['g', 'h', 'i'], firstRow: 2 }) // row 2

      expect(cache.getNextMissingRow({ rowStart: 2, rowEnd: 2 })).toBeUndefined()
      expect(cache.getNextMissingRow({ rowStart: 2, rowEnd: 3 })).toBeUndefined()
      expect(cache.getNextMissingRow({ rowStart: 2, rowEnd: 4 })).toStrictEqual({ firstByteToStore: 40, firstByteToFetch: 40, firstRow: 3 })
    })
  })

  describe('isStored', () => {
    it('should correctly identify stored and non-stored rows', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store a row
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 10,
        byteCount: 10,
        firstRow: 0,
      })
      cache.store({
        byteOffset: 60,
        byteCount: 10,
        firstRow: 1,
      })
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 80,
        byteCount: 10,
        firstRow: 1,
      })
      expect(cache.isStored({ byteOffset: 10 })).toBe(true)
      expect(cache.isStored({ byteOffset: 20 })).toBe(false)
      expect(cache.isStored({ byteOffset: 60 })).toBe(true)
      expect(cache.isStored({ byteOffset: 80 })).toBe(true)
    })
  })
})
