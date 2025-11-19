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
      expect(df.getCell({ row: 1, column: 'b' })).toStrictEqual({ value: '5' })
      expect(df.getRowNumber({ row: 2 })).toStrictEqual({ value: 2 })
      // return undefined for out-of-bounds rows, since we cannot know exactly the number of rows
      // TODO(SL): throw if we know the exact number of rows?
      expect(df.getRowNumber({ row: 5 })).toBeUndefined()
      expect(df.getCell({ row: 5, column: 'a' })).toBeUndefined()
      // throws
      expect(() => df.getCell({ row: 0, column: 'd' })).toThrow()
      expect(() => df.getCell({ row: -1, column: 'a' })).toThrow()
      expect(() => df.getRowNumber({ row: -2 })).toThrow()
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
      expect(df.getCell({ row: 0, column: 'a' })).toBeUndefined()
      expect(df.getRowNumber({ row: 0 })).toBeUndefined()
      revoke()
    })

    it('should create fetch initial rows when specified', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toURL(text)
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
      })
      expect(df.numRows).toBe(4) // the estimate is not perfect
      expect(df.getCell({ row: 1, column: 'b' })).toStrictEqual({ value: '5' })
      expect(df.getCell({ row: 2, column: 'b' })).toBeUndefined()
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
  })
})
