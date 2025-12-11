import { describe, expect, it } from 'vitest'

import { CSVCache, CSVRange, Estimator, RowsCache } from '../src/cache.js'

describe('RowsCache', () => {
  it('should initialize correctly', () => {
    const rowsCache = new RowsCache()
    expect(rowsCache.byteCount).toBe(0)
    expect(rowsCache.numRows).toBe(0)
    expect(rowsCache.rows).toEqual([])
  })

  it('should add rows correctly', () => {
    const rowsCache = new RowsCache()
    rowsCache.append({
      cells: ['d', 'e', 'f'],
      byteCount: 10,
    })
    rowsCache.prepend({
      cells: ['1', '2', '3'],
      byteCount: 10,
    })
    expect(rowsCache.byteCount).toBe(20)
    expect(rowsCache.numRows).toBe(2)
    expect(rowsCache.rows).toEqual([['1', '2', '3'], ['d', 'e', 'f']])
  })

  it('should merge with another rows cache correctly', () => {
    const rowsCache1 = new RowsCache()
    rowsCache1.append({
      cells: ['a', 'b', 'c'],
      byteCount: 10,
    })
    rowsCache1.append({
      cells: ['e', 'f', 'g'],
      byteCount: 10,
    })
    expect(rowsCache1.numRows).toBe(2)
    expect(rowsCache1.byteCount).toBe(20)

    const rowsCache2 = new RowsCache()
    rowsCache2.append({
      cells: ['h', 'i', 'j'],
      byteCount: 10,
    })
    expect(rowsCache2.numRows).toBe(1)
    expect(rowsCache2.byteCount).toBe(10)

    rowsCache1.merge(rowsCache2)

    expect(rowsCache1.numRows).toBe(3)
    expect(rowsCache1.byteCount).toBe(30)
    expect(rowsCache1.rows).toEqual([['a', 'b', 'c'], ['e', 'f', 'g'], ['h', 'i', 'j']])
  })
})

describe('CSVRange', () => {
  it('should initialize correctly', () => {
    const range = new CSVRange({ firstByte: 0 })
    expect(range.byteCount).toBe(0)
    expect(range.nextByte).toBe(0)
    expect(range.getRow(0)).toBeUndefined()
    expect(range.rowsCache.rows).toEqual([])
    expect(range.rowsCache.byteCount).toBe(0)
    expect(range.rowsCache.numRows).toBe(0)
  })

  it('should initialize correctly at a random position', () => {
    const range = new CSVRange({ firstByte: 100 })
    expect(range.byteCount).toBe(0)
    expect(range.nextByte).toBe(100)
    expect(range.getRow(0)).toBeUndefined()
    expect(range.rowsCache.byteCount).toBe(0)
    expect(range.rowsCache.rows).toEqual([])
    expect(range.rowsCache.numRows).toBe(0)
  })

  it('should add rows correctly', () => {
    const range = new CSVRange({ firstByte: 100 })
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
    expect(range.byteCount).toBe(40)
    expect(range.nextByte).toBe(120)
    expect(range.rowsCache.numRows).toBe(2)
    expect(range.rowsCache.byteCount).toBe(20)
    expect(range.rowsCache.rows).toEqual([['1', '2', '3'], ['d', 'e', 'f']])
    expect(range.getRow(-1)).toBeUndefined()
    expect(range.getRow(0)).toEqual(['1', '2', '3'])
    expect(range.getRow(1)).toEqual(['d', 'e', 'f'])
    expect(range.getRow(2)).toBeUndefined()
  })

  it('should throw when adding non-contiguous rows', () => {
    const range = new CSVRange({ firstByte: 100 })
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
    const range1 = new CSVRange({ firstByte: 0 })
    range1.append({
      byteOffset: 0,
      byteCount: 10,
    })
    range1.append({
      cells: ['b', 'c', 'd'],
      byteOffset: 10,
      byteCount: 10,
    })
    expect(range1.rowsCache.numRows).toBe(1)
    expect(range1.rowsCache.byteCount).toBe(10)

    const range2 = new CSVRange({ firstByte: 20 })
    range2.append({
      cells: ['e', 'f', 'g'],
      byteOffset: 20,
      byteCount: 10,
    })
    range2.append({
      byteOffset: 30,
      byteCount: 10,
    })
    expect(range2.rowsCache.numRows).toBe(1)
    expect(range2.rowsCache.byteCount).toBe(10)

    range1.merge(range2)

    expect(range1.byteCount).toBe(40)
    expect(range1.nextByte).toBe(40)
    expect(range1.rowsCache.numRows).toBe(2)
    expect(range1.rowsCache.byteCount).toBe(20)
    expect(range1.rowsCache.rows).toEqual([['b', 'c', 'd'], ['e', 'f', 'g']])
    expect(range1.getRow(0)).toEqual(['b', 'c', 'd'])
    expect(range1.getRow(1)).toEqual(['e', 'f', 'g'])
  })

  it('should throw when merging non-contiguous ranges', () => {
    const range1 = new CSVRange({ firstByte: 0 })
    const range2 = new CSVRange({ firstByte: 30 })
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
      expect(cache.byteLength).toBe(100)
      expect(cache.headerByteCount).toBe(15)
      expect(cache.delimiter).toBe(',')
      expect(cache.newline).toBe('\n')
      expect(cache.complete).toBe(false)
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
      expect(cache.byteLength).toBe(100)
      expect(cache.headerByteCount).toBe(15)
      expect(cache.delimiter).toBe(',')
      expect(cache.newline).toBe('\n')
      expect(cache.complete).toBe(false)
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

  describe('when storing rows in order', () => {
    it('should append rows to the serial range', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      const stored1 = cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 15,
        byteCount: 10,
      })
      const stored2 = cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 25,
        byteCount: 10,
      })
      expect(stored1).toBe(true)
      expect(stored2).toBe(true)
      expect(cache.complete).toBe(false)
      // internal state
      expect(cache.serialRange.rowsCache.numRows).toBe(2)
      expect(cache.serialRange.byteCount).toBe(35)
      expect(cache.randomRanges.length).toBe(0)
    })
    it('should account for the bytes of an empty row, but add no cells', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      const stored = cache.store({
        // no cells
        byteOffset: 15,
        byteCount: 10,
      })
      expect(stored).toBe(true)
      expect(cache.complete).toBe(false)
      // internal state
      expect(cache.serialRange.rowsCache.numRows).toBe(0)
      expect(cache.serialRange.byteCount).toBe(25)
      expect(cache.randomRanges.length).toBe(0)
    })
    it('should report that the cache is complete after storing all the rows', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      expect(cache.complete).toBe(false)
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
      expect(cache.complete).toBe(false)
      // Last row to complete the cache
      cache.store({
        cells: ['x', 'y', 'z'],
        byteOffset: 30,
        byteCount: 70,
      })
      expect(cache.complete).toBe(true)
    })
    it('should ignore storing a row inside the serial range', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 15,
        byteCount: 10,
      })
      expect(cache.serialRange.rowsCache.numRows).toBe(1)
      expect(cache.serialRange.byteCount).toBe(25)
      expect(cache.randomRanges.length).toBe(0)
      // store a row inside the serial range
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 10,
        byteCount: 2,
      })
      // it did not throw, but also did not change the internal state
      expect(cache.serialRange.rowsCache.numRows).toBe(1)
      expect(cache.serialRange.byteCount).toBe(25)
      expect(cache.randomRanges.length).toBe(0)
    })
    it('should throw if trying to store a row that overlaps the serial range', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 15,
        byteCount: 10,
      })
      expect(() => {
        cache.store({
          cells: ['d', 'e', 'f'],
          byteOffset: 20,
          byteCount: 10,
        })
      }).toThrowError(/^Cannot store the row/)
    })
  })
  describe('when storing a row beyond the first rows', () => {
    it('should create a random range', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      const stored = cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 25,
        byteCount: 10,
      })
      expect(stored).toBe(true)
      expect(cache.complete).toBe(false)
      // internal state
      expect(cache.serialRange.rowsCache.numRows).toBe(0)
      expect(cache.serialRange.byteCount).toBe(15)
      expect(cache.randomRanges.length).toBe(1)
      expect(cache.randomRanges[0]?.rowsCache.numRows).toBe(1)
      expect(cache.randomRanges[0]?.byteCount).toBe(10)
    })
    it('should append a row to an existing random range if the boundaries match', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 25,
        byteCount: 10,
      })
      const stored = cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 35,
        byteCount: 10,
      })
      expect(stored).toBe(true)
      expect(cache.complete).toBe(false)
      // internal state
      expect(cache.serialRange.rowsCache.numRows).toBe(0)
      expect(cache.serialRange.byteCount).toBe(15)
      expect(cache.randomRanges.length).toBe(1)
      expect(cache.randomRanges[0]?.rowsCache.numRows).toBe(2)
      expect(cache.randomRanges[0]?.byteCount).toBe(20)
    })
    it('should prepend a row to an existing random range if the boundaries match', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 35,
        byteCount: 10,
      })
      const stored = cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 25,
        byteCount: 10,
      })
      expect(stored).toBe(true)
      expect(cache.complete).toBe(false)
      // internal state
      expect(cache.serialRange.rowsCache.numRows).toBe(0)
      expect(cache.serialRange.byteCount).toBe(15)
      expect(cache.randomRanges.length).toBe(1)
      expect(cache.randomRanges[0]?.rowsCache.numRows).toBe(2)
      expect(cache.randomRanges[0]?.byteCount).toBe(20)
    })
    it('should ignore storing a row inside an existing random range', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 25,
        byteCount: 10,
      })
      expect(cache.randomRanges.length).toBe(1)
      // Try to store a row inside the random range
      cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 26,
        byteCount: 5,
      })
      // It did not throw, but also did not change the internal state
      expect(cache.randomRanges.length).toBe(1)
      expect(cache.randomRanges[0]?.rowsCache.numRows).toBe(1)
      expect(cache.randomRanges[0]?.byteCount).toBe(10)
    })
    it('should merge two adjacent ranges when storing rows between', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store first row, which creates the first random range
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 25,
        byteCount: 10,
      })
      // Store second row, which creates the second random range
      cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 45,
        byteCount: 10,
      })
      expect(cache.randomRanges.length).toBe(2)
      expect(cache.randomRanges[0]?.rowsCache.numRows).toBe(1)
      expect(cache.randomRanges[0]?.byteCount).toBe(10)
      expect(cache.randomRanges[1]?.rowsCache.numRows).toBe(1)
      expect(cache.randomRanges[1]?.byteCount).toBe(10)
      // Store a row in between, which should merge the two ranges
      cache.store({
        cells: ['g', 'h', 'i'],
        byteOffset: 35,
        byteCount: 10,
      })
      expect(cache.randomRanges.length).toBe(1)
      expect(cache.randomRanges[0]?.rowsCache.numRows).toBe(3)
      expect(cache.randomRanges[0]?.byteCount).toBe(30)
    })
    it('should merge a random range with the serial range when storing preceding rows', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      // Store a row to create a random range
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 25,
        byteCount: 10,
      })
      expect(cache.randomRanges.length).toBe(1)
      // Now store a row that precedes the random range, which should merge it with the serial range
      cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 15,
        byteCount: 10,
      })
      expect(cache.serialRange.rowsCache.numRows).toBe(2)
      expect(cache.serialRange.byteCount).toBe(35)
      expect(cache.randomRanges.length).toBe(0)
    })
    it('should throw if trying to store a row that partially overlaps with a preceding random range', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 25,
        byteCount: 10,
      })
      expect(cache.randomRanges.length).toBe(1)
      expect(() => {
        cache.store({
          cells: ['d', 'e', 'f'],
          byteOffset: 20,
          byteCount: 10,
        })
      }).toThrowError(/^Cannot store the row/)
    })
    it('should throw if trying to store a row that partially overlaps with a following random range', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 25,
        byteCount: 10,
      })
      expect(cache.randomRanges.length).toBe(1)
      expect(() => {
        cache.store({
          cells: ['d', 'e', 'f'],
          byteOffset: 20,
          byteCount: 10,
        })
      }).toThrowError(/^Cannot store the row/)
    })
  })
  it.each([
    { byteOffset: -10, byteCount: 10 },
    { byteOffset: 10, byteCount: -10 },
    { byteOffset: 100, byteCount: 60 },
    { byteOffset: 95, byteCount: 10 },
  ])('throws if trying to store a row outside of the cache bounds or with invalid byteCount: %o', ({ byteOffset, byteCount }) => {
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
      })
    }).toThrow()
  })
})

describe('Estimator', () => {
  describe('constructor', () => {
    it('should initialize correctly', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 15,
        delimiter: ',',
        newline: '\n' as const,
      })
      const estimator = new Estimator({ cache })
      expect(estimator.numRows).toBe(0)
      expect(estimator.isNumRowsEstimated).toBe(true)
    })
    it('should estimate on the contents of the cache only after calling refresh()', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        byteOffset: 10,
        byteCount: 10,
        cells: ['a', 'b', 'c'],
      })
      const estimator = new Estimator({ cache })
      expect(estimator.numRows).toBe(0)
      expect(estimator.isNumRowsEstimated).toBe(true)
      estimator.refresh()
      expect(estimator.numRows).toBe(9)
      expect(estimator.isNumRowsEstimated).toBe(true)
    })
    it('can be copied from another estimator, and then be independent', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        byteOffset: 10,
        byteCount: 10,
        cells: ['a', 'b', 'c'],
      })
      const estimator = new Estimator({ cache })
      estimator.refresh()

      const copy = estimator.copy()
      expect(copy.numRows).toBe(9)
      expect(copy.isNumRowsEstimated).toBe(true)

      // only two rows, the second one is empty
      cache.store({
        byteOffset: 20,
        byteCount: 80,
      })
      estimator.refresh()
      expect(estimator.numRows).toBe(1)
      expect(estimator.isNumRowsEstimated).toBe(false)
      expect(copy.numRows).toBe(9)
      expect(copy.isNumRowsEstimated).toBe(true)
    })
  })

  describe('refresh', () => {
    it('updates the internal state if the cache is now complete', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 100,
        delimiter: ',',
        newline: '\n' as const,
      })
      const estimator = new Estimator({ cache })
      const updated = estimator.refresh()
      expect(updated).toBe(true)
    })
    it('does not update the internal state if the cache was already complete', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 100,
        delimiter: ',',
        newline: '\n' as const,
      })
      const estimator = new Estimator({ cache })
      estimator.refresh()
      // refresh again
      const updated = estimator.refresh()
      expect(updated).toBe(false)
    })
    it('updates the internal state if the previous average row byte count was zero', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        byteOffset: 10,
        byteCount: 10,
        cells: ['a', 'b', 'c'],
      })
      const estimator = new Estimator({ cache })
      const updated = estimator.refresh()
      expect(updated).toBe(true)
    })
    it('updates the internal state if the difference with the previous average row byte count is significant (more than 1%)', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        byteOffset: 10,
        byteCount: 10,
        cells: ['a', 'b', 'c'],
      })
      const estimator = new Estimator({ cache })
      estimator.refresh()
      cache.store({
        byteOffset: 20,
        byteCount: 70,
        cells: ['a', 'b', 'c'],
      })
      const updated = estimator.refresh()
      expect(updated).toBe(true)
    })
    it('does not update the internal state if the difference with the previous average row byte count is not significant (less than 1%)', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 1000,
        headerByteCount: 0,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        byteOffset: 0,
        byteCount: 400,
        cells: ['a', 'b', 'c'],
      })
      const estimator = new Estimator({ cache })
      estimator.refresh()
      cache.store({
        byteOffset: 400,
        byteCount: 401,
        cells: ['a', 'b', 'c'],
      })
      const updated = estimator.refresh()
      expect(updated).toBe(false)
    })
  })

  describe('max row number, used for validation', () => {
    it('should be Infinity if the number of rows is an estimate', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      const estimator = new Estimator({ cache })
      estimator.refresh()
      expect(estimator.maxNumRows).toBe(Infinity)
      expect(estimator.isNumRowsEstimated).toBe(true)
    })
    it('should be numRows if the number of rows is not an estimate', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 100,
        delimiter: ',',
        newline: '\n' as const,
      })
      const estimator = new Estimator({ cache })
      estimator.refresh()
      expect(estimator.maxNumRows).toBe(0)
      expect(estimator.maxNumRows).toBe(estimator.numRows)
      expect(estimator.isNumRowsEstimated).toBe(false)
    })
  })

  describe('getRowNumber, getCell, guessFirstMissingRow and guessLastMissingRow', () => {
    it('return nothing for any row when the cache is empty', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      const estimator = new Estimator({ cache })

      expect(estimator.getRowNumber({ row: 0 })).toBeUndefined()
      expect(estimator.getRowNumber({ row: 10 })).toBeUndefined()
      expect(estimator.getRowNumber({ row: 100 })).toBeUndefined()

      expect(estimator.getCell({ row: 0, column: 0 })).toBeUndefined()
      expect(estimator.getCell({ row: 10, column: 1 })).toBeUndefined()
      expect(estimator.getCell({ row: 100, column: 2 })).toBeUndefined()
      expect(() => estimator.getCell({ row: 0, column: 3 })).toThrowError(/^Column index/)

      // The first byte offset is after the header
      expect(estimator.guessFirstMissingRow({ minRow: 0 })).toEqual({ byteOffset: 10, row: 0, isEstimate: false })
      // No estimation available
      expect(estimator.guessFirstMissingRow({ minRow: 1 })).toBeUndefined()
      expect(estimator.guessFirstMissingRow({ minRow: 10 })).toBeUndefined()
      expect(estimator.guessFirstMissingRow({ minRow: 100 })).toBeUndefined()

      expect(estimator.guessLastMissingRow({ maxRow: 0 })).toEqual({ byteOffset: 10, row: 0, isEstimate: false })
      expect(estimator.guessLastMissingRow({ maxRow: 10 })).toBeUndefined()
      expect(estimator.guessLastMissingRow({ maxRow: 100 })).toBeUndefined()
    })
    it('return the correct value for a complete cache', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 10,
        byteCount: 10,
      })
      cache.store({
        cells: ['d', 'e', 'f'],
        byteOffset: 20,
        byteCount: 80,
      })
      const estimator = new Estimator({ cache })
      estimator.refresh()

      expect(estimator.getRowNumber({ row: 0 })).toEqual({ value: 0 })
      expect(estimator.getRowNumber({ row: 1 })).toEqual({ value: 1 })
      expect(estimator.getRowNumber({ row: 2 })).toBeUndefined()

      expect(estimator.getCell({ row: 0, column: 0 })).toEqual({ value: 'a' })
      expect(estimator.getCell({ row: 1, column: 0 })).toEqual({ value: 'd' })
      expect(estimator.getCell({ row: 2, column: 0 })).toBeUndefined()

      // The cache is complete, so no estimation is needed
      expect(estimator.guessFirstMissingRow({ minRow: 2 })).toBeUndefined()
      expect(estimator.guessFirstMissingRow({ minRow: 10 })).toBeUndefined()
      expect(estimator.guessLastMissingRow({ maxRow: 0 })).toBeUndefined() // no rows before row 0
      expect(estimator.guessLastMissingRow({ maxRow: 1 })).toBeUndefined()
      expect(estimator.guessLastMissingRow({ maxRow: 100 })).toBeUndefined()
    })
    it('return the correct value for rows stored at the start (exact match)', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 0,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 0,
        byteCount: 10,
      })
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 10,
        byteCount: 10,
      })
      const estimator = new Estimator({ cache })
      estimator.refresh()

      expect(estimator.getRowNumber({ row: 0 })).toEqual({ value: 0 })
      expect(estimator.getRowNumber({ row: 1 })).toEqual({ value: 1 })
      // getRowNumber returns a value if it can estimate it, even if the row is not stored
      expect(estimator.getRowNumber({ row: 2 })).toEqual({ value: 2 })
      // getRowNumber returns undefined for rows way beyond the estimated number of rows
      expect(estimator.getRowNumber({ row: -1 })).toBeUndefined()
      expect(estimator.getRowNumber({ row: 1000 })).toBeUndefined()

      expect(estimator.getCell({ row: 0, column: 0 })).toEqual({ value: 'a' })
      expect(estimator.getCell({ row: 1, column: 0 })).toEqual({ value: 'a' })
      expect(estimator.getCell({ row: 2, column: 0 })).toBeUndefined()

      // just after the first rows (exact)
      expect(estimator.guessFirstMissingRow({ minRow: 2 })).toEqual({ byteOffset: 20, row: 2, isEstimate: false })
      // beyond the first rows (estimated)
      expect(estimator.guessFirstMissingRow({ minRow: 3 })).toEqual({ byteOffset: 30, row: 3, isEstimate: true })
      // no missing row before row 1
      expect(estimator.guessLastMissingRow({ maxRow: 1 })).toBeUndefined()
      // at the end of the stored rows (exact)
      expect(estimator.guessLastMissingRow({ maxRow: 2 })).toEqual({ byteOffset: 20, row: 2, isEstimate: false })
      // after the end of the stored rows (estimated)
      expect(estimator.guessLastMissingRow({ maxRow: 3 })).toEqual({ byteOffset: 30, row: 3, isEstimate: true })
    })
    it('return the correct value for rows stored in the middle of the file (estimated match)', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 0,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 10,
        byteCount: 10,
      })
      cache.store({
        // missing last cell
        cells: ['a', 'b'],
        byteOffset: 20,
        byteCount: 10,
      })
      const estimator = new Estimator({ cache })
      estimator.refresh()

      // getRowNumber returns a value if it can estimate it, even if the row is not stored
      expect(estimator.getRowNumber({ row: 0 })).toEqual({ value: 0 })
      expect(estimator.getRowNumber({ row: 1 })).toEqual({ value: 1 })
      expect(estimator.getRowNumber({ row: 2 })).toEqual({ value: 2 })
      expect(estimator.getRowNumber({ row: 3 })).toEqual({ value: 3 })
      // getRowNumber returns undefined for rows way beyond the estimated number of rows
      expect(estimator.getRowNumber({ row: -1 })).toBeUndefined()
      expect(estimator.getRowNumber({ row: 1000 })).toBeUndefined()

      expect(estimator.getCell({ row: 0, column: 0 })).toBeUndefined()
      expect(estimator.getCell({ row: 1, column: 0 })).toEqual({ value: 'a' })
      expect(estimator.getCell({ row: 2, column: 0 })).toEqual({ value: 'a' })
      // the missing cell should be returned as empty
      expect(estimator.getCell({ row: 2, column: 2 })).toEqual({ value: '' })
      expect(estimator.getCell({ row: 3, column: 0 })).toBeUndefined()

      // at the start (exact)
      expect(estimator.guessFirstMissingRow({ minRow: 0 })).toEqual({ byteOffset: 0, row: 0, isEstimate: false })
      // just after the first estimated rows (estimated)
      expect(estimator.guessFirstMissingRow({ minRow: 3 })).toEqual({ byteOffset: 30, row: 3, isEstimate: true })
      // beyond the estimated rows (estimated)
      expect(estimator.guessFirstMissingRow({ minRow: 8 })).toEqual({ byteOffset: 80, row: 8, isEstimate: true })

      // row 0 is missing
      expect(estimator.guessLastMissingRow({ maxRow: 0 })).toEqual({ byteOffset: 0, row: 0, isEstimate: false })
      // random rows (estimated) - this is incorrect, the row is stored. TODO(SL): check the rows stored in random ranges
      expect(estimator.guessLastMissingRow({ maxRow: 2 })).toEqual({ byteOffset: 20, row: 2, isEstimate: true })
      expect(estimator.guessLastMissingRow({ maxRow: 3 })).toEqual({ byteOffset: 30, row: 3, isEstimate: true })
      expect(estimator.guessLastMissingRow({ maxRow: 8 })).toEqual({ byteOffset: 80, row: 8, isEstimate: true })
    })
    it('return nothing if the estimator was not refreshed yet', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 0,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        cells: ['a', 'b', 'c'],
        byteOffset: 10,
        byteCount: 10,
      })
      const estimator = new Estimator({ cache })
      // not refreshed yet
      expect(estimator.getRowNumber({ row: 0 })).toBeUndefined()
      expect(estimator.getCell({ row: 0, column: 0 })).toBeUndefined()
      expect(estimator.guessFirstMissingRow({ minRow: 0 })).toEqual({ byteOffset: 0, row: 0, isEstimate: false })
      expect(estimator.guessFirstMissingRow({ minRow: 1 })).toBeUndefined()
      expect(estimator.guessLastMissingRow({ maxRow: 0 })).toEqual({ byteOffset: 0, row: 0, isEstimate: false })
      expect(estimator.guessLastMissingRow({ maxRow: 1 })).toBeUndefined()
    })
    it('return the correct value when the last rows have been stored', () => {
      const cache = new CSVCache({
        columnNames: ['col1', 'col2', 'col3'],
        byteLength: 100,
        headerByteCount: 10,
        delimiter: ',',
        newline: '\n' as const,
      })
      cache.store({
        cells: ['u', 'v', 'w'],
        byteOffset: 80,
        byteCount: 10,
      })
      cache.store({
        cells: ['x', 'y', 'z'],
        byteOffset: 90,
        byteCount: 10,
      })
      const estimator = new Estimator({ cache })
      estimator.refresh()

      expect(estimator.getRowNumber({ row: 0 })).toEqual({ value: 0 })
      expect(estimator.getRowNumber({ row: 6 })).toEqual({ value: 6 })
      expect(estimator.getRowNumber({ row: 7 })).toEqual({ value: 7 })
      expect(estimator.getRowNumber({ row: 8 })).toEqual({ value: 8 })
      expect(estimator.getRowNumber({ row: 9 })).toBeUndefined()

      expect(estimator.getCell({ row: 0, column: 0 })).toBeUndefined()
      expect(estimator.getCell({ row: 6, column: 0 })).toBeUndefined()
      expect(estimator.getCell({ row: 7, column: 0 })).toEqual({ value: 'u' })
      expect(estimator.getCell({ row: 8, column: 0 })).toEqual({ value: 'x' })
      expect(estimator.getCell({ row: 9, column: 0 })).toBeUndefined()

      // before the stored rows (estimated)
      expect(estimator.guessFirstMissingRow({ minRow: 0 })).toEqual({ byteOffset: 10, row: 0, isEstimate: false })
      expect(estimator.guessFirstMissingRow({ minRow: 6 })).toEqual({ byteOffset: 70, row: 6, isEstimate: true })
      expect(estimator.guessFirstMissingRow({ minRow: 7 })).toEqual({ byteOffset: 80, row: 7, isEstimate: true })
      // the following tests are incorrect, as these rows are stored. TODO(SL): check the rows stored in random ranges
      expect(estimator.guessFirstMissingRow({ minRow: 8 })).toEqual({ byteOffset: 90, row: 8, isEstimate: true })
      expect(estimator.guessFirstMissingRow({ minRow: 9 })).toEqual({ byteOffset: 99, row: 9, isEstimate: true })
      expect(estimator.guessFirstMissingRow({ minRow: 10 })).toEqual({ byteOffset: 99, row: 10, isEstimate: true })

      // before the stored rows (estimated)
      expect(estimator.guessLastMissingRow({ maxRow: 0 })).toEqual({ byteOffset: 10, row: 0, isEstimate: false })
      expect(estimator.guessLastMissingRow({ maxRow: 6 })).toEqual({ byteOffset: 70, row: 6, isEstimate: true })
      // the following tests are incorrect, as these rows are stored. TODO(SL): check the rows stored in random ranges
      expect(estimator.guessLastMissingRow({ maxRow: 7 })).toEqual({ byteOffset: 80, row: 7, isEstimate: true })
      expect(estimator.guessLastMissingRow({ maxRow: 8 })).toEqual({ byteOffset: 90, row: 8, isEstimate: true })
      expect(estimator.guessLastMissingRow({ maxRow: 9 })).toEqual({ byteOffset: 99, row: 9, isEstimate: true })
      expect(estimator.guessLastMissingRow({ maxRow: 10 })).toEqual({ byteOffset: 99, row: 10, isEstimate: true })
    })
  })
})
