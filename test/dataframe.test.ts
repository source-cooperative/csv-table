import { toURL } from 'csv-range'
import { describe, expect, it } from 'vitest'

import { csvDataFrame } from '../src/dataframe'

describe('csvDataFrame', () => {
  describe('creation', () => {
    it('should create a dataframe from a CSV file', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      // Includes the extra character ' ' to handle bug in Node.js (see toURL)
      const { url, revoke, fileSize } = toURL(text)
      // TODO(SL): make it sync, by passing the header?
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(df.numRows).toBe(3)
      expect(df.columnDescriptors).toStrictEqual(['a', 'b', 'c'].map(name => ({ name })))
      revoke()
    })

    it('should throw when creating a dataframe from an empty CSV file without a header (one column is required)', async () => {
      const text = '\n'
      const { url, revoke, fileSize } = toURL(text)
      await expect(csvDataFrame({
        url,
        byteLength: fileSize,
      })).rejects.toThrow()
      revoke()
    })

    it('should create a dataframe from a CSV file with only a header', async () => {
      const text = 'a'
      const { url, revoke, fileSize } = toURL(text)
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(df.numRows).toBe(0)
      expect(df.columnDescriptors).toStrictEqual(['a'].map(name => ({ name })))
      expect(() => df.getRowNumber({ row: 0 })).toThrow()
      revoke()
    })

    it('should fetch initial rows when specified', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text)
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
      })
      expect(df.getCell({ row: 1, column: 'b' })).toStrictEqual({ value: '5' })
      expect(df.getCell({ row: 2, column: 'b' })).toBeUndefined()
      revoke()
    })

    it('when the CSV file is not fully loaded, the number of rows might be inaccurate', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text)
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 1,
      })
      // with only one row loaded, the average row size is not accurate enough to estimate the number of rows
      expect(df.numRows).toBe(4) // the estimate is not perfect
      expect(df.getCell({ row: 1, column: 'b' })).toBeUndefined()
      revoke()
    })

    it('should fetch initial rows when specified, even if it is 0', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(df.numRows).toBe(3)
      expect(df.columnDescriptors).toStrictEqual(['a', 'b', 'c'].map(name => ({ name })))
      expect(df.getCell({ row: 0, column: 'a' })).toStrictEqual({ value: '1' })
      revoke()
    })

    it('should ignore rows with only whitespace and delimitersbefore the header', async () => {
      const text = '\n\t\n , , \n,,\na,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(() => df.getCell({ row: 5, column: 'a' })).toThrow()
      revoke()
    })

    it('should throw when called with an orderBy parameter', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(() => df.getRowNumber({ row: -1 })).toThrow()
      revoke()
    })

    it('should return undefined for not yet cached rows', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(() => df.getRowNumber({ row: 5 })).toThrow()
      revoke()
    })

    it('should throw when called with an orderBy parameter', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
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
      const { url, revoke, fileSize } = toURL(text)
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      await expect(df.fetch?.({ rowStart: 5, rowEnd: 10 })).rejects.toThrow()
      revoke()
    })

    it('should fetch out-of-bound rows, if the dataframe is not fully loaded', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text)
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
  })
})
