import { toURL } from 'csv-range'
import { describe, expect, it } from 'vitest'

import { csvDataFrame } from '../src/dataframe'

describe('csvDataFrame', () => {
  describe('creation', () => {
    it('should create a dataframe from a CSV file', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      // Includes the extra character ' ' to handle bug in Node.js (see toURL)
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(df.numRows).toBe(3)
      expect(df.metadata).toEqual({ isNumRowsEstimated: false })
      expect(df.columnDescriptors).toStrictEqual(['a', 'b', 'c'].map(name => ({ name })))
      revoke()
    })

    it('should throw when creating a dataframe from an empty CSV file without a header (one column is required)', async () => {
      const text = '\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      await expect(csvDataFrame({
        url,
        byteLength: fileSize,
      })).rejects.toThrow()
      revoke()
    })

    it('should create a dataframe from a CSV file with only a header', async () => {
      const text = 'a'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(df.numRows).toBe(0)
      expect(df.metadata).toEqual({ isNumRowsEstimated: false })
      expect(df.columnDescriptors).toStrictEqual(['a'].map(name => ({ name })))
      expect(() => df.getRowNumber({ row: 0 })).toThrow()
      revoke()
    })

    it('should fetch initial rows when specified', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
      })
      expect(df.getCell({ row: 1, column: 'b' })).toStrictEqual({ value: '5' })
      expect(df.getCell({ row: 2, column: 'b' })).toBeUndefined()
      revoke()
    })

    it.each([
      { text: 'a,b,c\n1111,2222,3333\nn44,55,66\n77,88,99\n', expectedRows: 2 },
      { text: 'a,b,c\n11,22,33\n44,55,66\n77,88,99\n', expectedRows: 3 },
      { text: 'a,b,c\n1,2,3\nn44,55,66\n77,88,99\n', expectedRows: 4 },
    ])('when the CSV file is not fully loaded, the number of rows might be inaccurate: $expectedRows (correct: 3)', async ({ text, expectedRows }) => {
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 1,
      })
      // with only one row loaded, the average row size is not accurate enough to estimate the number of rows
      expect(df.numRows).toBe(expectedRows) // the estimate is not perfect
      expect(df.metadata).toEqual({ isNumRowsEstimated: true })
      revoke()
    })

    it('should fetch initial rows when specified, even if it is 0', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 0,
      })
      expect(df.getCell({ row: 1, column: 'b' })).toBeUndefined()
      revoke()
    })

    it('should ignore empty rows when fetching initial rows', async () => {
      const text = 'a,b,c\n1,2,3\n\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 4,
      })
      expect(df.getCell({ row: 1, column: 'b' })).toStrictEqual({ value: '8' })
      expect(() => df.getCell({ row: 2, column: 'b' })).toThrow()
      revoke()
    })

    it('should not ignore rows with empty cells when fetching initial rows', async () => {
      const text = 'a,b,c\n1,2,3\n,\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(df.getCell({ row: 1, column: 'b' })).toStrictEqual({ value: '' })
      expect(df.getCell({ row: 2, column: 'b' })).toStrictEqual({ value: '8' })
      revoke()
    })

    it('should ignore empty rows before the header', async () => {
      const text = '\n\n\na,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(df.numRows).toBe(3)
      expect(df.columnDescriptors).toStrictEqual(['a', 'b', 'c'].map(name => ({ name })))
      expect(df.getCell({ row: 0, column: 'a' })).toStrictEqual({ value: '1' })
      revoke()
    })

    it('should ignore rows with only whitespace and delimiters before the header', async () => {
      const text = '\n\t\n , , \n,,\na,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(df.numRows).toBe(3)
      expect(df.columnDescriptors).toStrictEqual(['a', 'b', 'c'].map(name => ({ name })))
      expect(df.getCell({ row: 0, column: 'a' })).toStrictEqual({ value: '1' })
      revoke()
    })
  })

  describe('getCell', () => {
    it('should return the correct cell values', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(df.getCell({ row: 0, column: 'a' })).toStrictEqual({ value: '1' })
      expect(df.getCell({ row: 1, column: 'b' })).toStrictEqual({ value: '5' })
      expect(df.getCell({ row: 2, column: 'c' })).toStrictEqual({ value: '9' })
      revoke()
    })

    it('should throw when called with invalid parameters', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(() => df.getCell({ row: -1, column: 'a' })).toThrow()
      expect(() => df.getCell({ row: 0, column: 'd' })).toThrow()
      revoke()
    })

    it('should return undefined for not yet cached cells', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
      })
      expect(df.getCell({ row: 2, column: 'a' })).toBeUndefined()
      revoke()
    })

    it('should return undefined for out-of-bound cells, if the dataframe is not fully loaded', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
      })
      expect(df.getCell({ row: 5, column: 'a' })).toBeUndefined()
      revoke()
    })

    it('should throw for out-of-bound cells, if the dataframe is fully loaded', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(() => df.getCell({ row: 5, column: 'a' })).toThrow()
      revoke()
    })

    it('should throw when called with an orderBy parameter', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(() => df.getCell({ row: 0, column: 'a', orderBy: [{ column: 'a', direction: 'ascending' }] })).toThrow()
      revoke()
    })
  })

  describe('getRowNumber', () => {
    it('should return the correct row numbers', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(df.getRowNumber({ row: 0 })).toStrictEqual({ value: 0 })
      expect(df.getRowNumber({ row: 1 })).toStrictEqual({ value: 1 })
      expect(df.getRowNumber({ row: 2 })).toStrictEqual({ value: 2 })
      revoke()
    })

    it('should throw when called with invalid parameters', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(() => df.getRowNumber({ row: -1 })).toThrow()
      revoke()
    })

    it('should return undefined for not yet cached rows', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
      })
      expect(df.getRowNumber({ row: 2 })).toBeUndefined()
      revoke()
    })

    it('should return undefined for out-of-bound rows, if the dataframe is not fully loaded', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
      })
      expect(df.getRowNumber({ row: 5 })).toBeUndefined()
      revoke()
    })

    it('should throw for out-of-bound rows, if the dataframe is fully loaded', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(() => df.getRowNumber({ row: 5 })).toThrow()
      revoke()
    })

    it('should throw when called with an orderBy parameter', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(() => df.getRowNumber({ row: 0, orderBy: [{ column: 'a', direction: 'ascending' }] })).toThrow()
      revoke()
    })
  })

  describe('fetch', () => {
    it('should fetch more rows', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n10,11,12\n13,14,15\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
      })
      expect(df.getCell({ row: 2, column: 'a' })).toBeUndefined()
      await df.fetch?.({ rowStart: 2, rowEnd: 5 })
      expect(df.getCell({ row: 2, column: 'a' })).toStrictEqual({ value: '7' })
      expect(df.getCell({ row: 3, column: 'b' })).toStrictEqual({ value: '11' })
      expect(df.getCell({ row: 4, column: 'c' })).toStrictEqual({ value: '15' })
      revoke()
    })

    it('should fetch rows even if some are empty', async () => {
      const text = 'a,b,c\n1,2,3\n\n4,5,6\n\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 1,
      })
      expect(df.getCell({ row: 1, column: 'a' })).toBeUndefined()
      await df.fetch?.({ rowStart: 1, rowEnd: 10 })
      expect(df.getCell({ row: 1, column: 'a' })).toStrictEqual({ value: '4' })
      expect(df.getCell({ row: 2, column: 'b' })).toStrictEqual({ value: '8' })
      expect(() => df.getCell({ row: 3, column: 'c' })).toThrow()
      revoke()
    })

    it('should use the chunk size when fetching rows', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n10,11,12\n13,14,15\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 1,
        chunkSize: 8, // small chunk size to force multiple fetches
      })
      expect(df.getCell({ row: 1, column: 'a' })).toBeUndefined()
      await df.fetch?.({ rowStart: 1, rowEnd: 5 })
      expect(df.getCell({ row: 1, column: 'a' })).toStrictEqual({ value: '4' })
      expect(df.getCell({ row: 2, column: 'b' })).toStrictEqual({ value: '8' })
      expect(df.getCell({ row: 3, column: 'c' })).toStrictEqual({ value: '12' })
      revoke()
    })

    it('should do nothing when fetching already cached rows', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 3,
      })
      expect(df.numRows).toBe(3)
      const before = df.getCell({ row: 0, column: 'b' })
      await df.fetch?.({ rowStart: 0, rowEnd: 1 })
      const after = df.getCell({ row: 0, column: 'b' })
      expect(after).toStrictEqual(before)
      revoke()
    })

    it('should throw when fetching out-of-bound rows, if the dataframe is fully loaded', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      await expect(df.fetch?.({ rowStart: 5, rowEnd: 10 })).rejects.toThrow()
      revoke()
    })

    it('should fetch out-of-bound rows, if the dataframe is not fully loaded', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
      })
      expect(df.getCell({ row: 2, column: 'a' })).toBeUndefined()
      await df.fetch?.({ rowStart: 2, rowEnd: 10 })
      expect(df.getCell({ row: 2, column: 'a' })).toStrictEqual({ value: '7' })
      revoke()
    })

    it('should fetch rows at a random position if requested', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n10,11,12\n13,14,15\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
      })
      expect(df.getCell({ row: 3, column: 'a' })).toBeUndefined()
      await df.fetch?.({ rowStart: 3, rowEnd: 4 })
      // row 3 is now cached, while row 2 is still not cached
      expect(df.getCell({ row: 3, column: 'a' })).toStrictEqual({ value: '10' })
      expect(df.getCell({ row: 2, column: 'a' })).toBeUndefined()
      revoke()
    })

    it('will fetch an incorrect row if the average row size had been overestimated', async () => {
      const text = 'a,b,c\n111,222,333\n4,5,6\n7,8,9\n10,11,12\n13,14,15\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 1,
      })
      expect(df.getCell({ row: 2, column: 'a' })).toBeUndefined()
      await df.fetch?.({ rowStart: 2, rowEnd: 5 })
      expect(df.getCell({ row: 2, column: 'a' })).toStrictEqual({ value: '10' }) // should be 7
      expect(df.getCell({ row: 3, column: 'b' })).toStrictEqual({ value: '14' }) // should be 11
      expect(df.getCell({ row: 4, column: 'c' })).toBeUndefined() // should be 15
      revoke()
    })

    it('fails to fetch the last rows if the average row size has been overestimated', async () => {
      const text = 'a,b,c\n111111111,222222222,333333333\n,4,5,6\n7,8,9\n10,11,12\n13,14,15\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 1,
      })
      expect(df.getCell({ row: 1, column: 'a' })).toBeUndefined()
      await df.fetch?.({ rowStart: 3, rowEnd: 5 })
      expect(df.getCell({ row: 1, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 2, column: 'b' })).toBeUndefined()
      expect(df.getCell({ row: 3, column: 'c' })).toBeUndefined()
      revoke()
    })

    it('should break the current parsing and start a new one if the next row is beyond one chunk size', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n10,11,12\n13,14,15\n16,17,18\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 1,
        chunkSize: 8, // small chunk size to force multiple fetches
      })
      expect(df.getCell({ row: 0, column: 'a' })).toStrictEqual({ value: '1' })
      expect(df.getCell({ row: 1, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 2, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 3, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 4, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 5, column: 'a' })).toBeUndefined()
      await df.fetch?.({ rowStart: 2, rowEnd: 5 })
      expect(df.getCell({ row: 1, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 2, column: 'a' })).toStrictEqual({ value: '7' })
      expect(df.getCell({ row: 3, column: 'a' })).toStrictEqual({ value: '10' })
      expect(df.getCell({ row: 4, column: 'a' })).toStrictEqual({ value: '13' })
      expect(df.getCell({ row: 5, column: 'a' })).toBeUndefined()
      await df.fetch?.({ rowStart: 1, rowEnd: 6 })
      expect(df.getCell({ row: 1, column: 'a' })).toStrictEqual({ value: '4' })
      expect(df.getCell({ row: 5, column: 'a' })).toStrictEqual({ value: '16' })
      revoke()
    })

    it('fetches incorrect rows if the row estimation is incorrect', async () => {
      const text = 'a,b,c\n111111,222222,333333\n4,5,6\n7,8,9\n10,11,12\n13,14,15\n16,17,18\n19,20,21\n22,23,24\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 1,
      })
      expect(df.getCell({ row: 0, column: 'a' })).toStrictEqual({ value: '111111' })
      expect(df.getCell({ row: 1, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 2, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 3, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 4, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 5, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 6, column: 'a' })).toBeUndefined()
      // average row size here is 21, because of the first row

      await df.fetch?.({ rowStart: 2, rowEnd: 6 })

      expect(df.getCell({ row: 1, column: 'a' })).toBeUndefined()
      // erroneously got row 4 instead of row 2, due to the overestimation of the average row size
      expect(df.getCell({ row: 2, column: 'a' })).toStrictEqual({ value: '13' })
      expect(df.getCell({ row: 3, column: 'a' })).toStrictEqual({ value: '16' })
      expect(df.getCell({ row: 4, column: 'a' })).toStrictEqual({ value: '19' })
      expect(df.getCell({ row: 5, column: 'a' })).toStrictEqual({ value: '22' })
      expect(df.getCell({ row: 6, column: 'a' })).toBeUndefined()
      revoke()
    })

    it('does not fetch all the rows, if the row estimation is incorrect', async () => {
      const text = 'a,b,c\n111111,222222,333333\n4,5,6\n7,8,9\n10,11,12\n13,14,15\n16,17,18\n19,20,21\n22,23,24\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 1,
      })
      expect(df.getCell({ row: 0, column: 'a' })).toStrictEqual({ value: '111111' })
      expect(df.getCell({ row: 1, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 2, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 3, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 4, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 5, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 6, column: 'a' })).toBeUndefined()
      // average row size here is 21, because of the first row

      await df.fetch?.({ rowStart: 2, rowEnd: 7 })

      expect(df.getCell({ row: 1, column: 'a' })).toBeUndefined()
      // erroneously got row 4 instead of row 2, due to the overestimation of the average row size
      expect(df.getCell({ row: 2, column: 'a' })).toStrictEqual({ value: '13' })
      expect(df.getCell({ row: 3, column: 'a' })).toStrictEqual({ value: '16' })
      expect(df.getCell({ row: 4, column: 'a' })).toStrictEqual({ value: '19' })
      expect(df.getCell({ row: 5, column: 'a' })).toStrictEqual({ value: '22' })
      // the last row, row 6, was not fetched, even if it was requested
      expect(df.getCell({ row: 6, column: 'a' })).toBeUndefined()
      revoke()
    })

    it('dispatches one "resolve" event per stored row in the range', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n10,11,12\n13,14,15\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
      })

      let resolveEventCount = 0
      df.eventTarget?.addEventListener('resolve', () => {
        resolveEventCount++
      })

      await df.fetch?.({ rowStart: 2, rowEnd: 4 })
      expect(resolveEventCount).toBe(2)

      // No event because rows are already resolved
      await df.fetch?.({ rowStart: 2, rowEnd: 4 })
      expect(resolveEventCount).toBe(2)

      await df.fetch?.({ rowStart: 2, rowEnd: 6 })
      expect(resolveEventCount).toBe(3)

      revoke()
    })

    it('can parse again the same rows, if the chunk has been fetched but some rows were already cached', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n10,11,12\n13,14,15\n'
      const { url, revoke, fileSize } = toURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 1,
      })
      expect(df.getCell({ row: 0, column: 'a' })).toStrictEqual({ value: '1' })
      expect(df.getCell({ row: 1, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 2, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 3, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 4, column: 'a' })).toBeUndefined()

      await df.fetch?.({ rowStart: 3, rowEnd: 4 })
      expect(df.getCell({ row: 1, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 2, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 3, column: 'a' })).toStrictEqual({ value: '10' })
      expect(df.getCell({ row: 4, column: 'a' })).toBeUndefined()

      // Fetch again the same chunk
      await df.fetch?.({ rowStart: 1, rowEnd: 5 })
      expect(df.getCell({ row: 1, column: 'a' })).toStrictEqual({ value: '4' })
      expect(df.getCell({ row: 2, column: 'a' })).toStrictEqual({ value: '7' })
      expect(df.getCell({ row: 3, column: 'a' })).toStrictEqual({ value: '10' })
      expect(df.getCell({ row: 4, column: 'a' })).toStrictEqual({ value: '13' })

      revoke()
    })
  })
})
