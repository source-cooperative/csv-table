import { toBlobURL } from 'cosovo'
import { describe, expect, it } from 'vitest'

import { csvDataFrame } from '../src/dataframe'

describe('csvDataFrame', () => {
  describe('creation', () => {
    it('should create a dataframe from a CSV file', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      // Includes the extra character ' ' to handle bug in Node.js (see toBlobURL)
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
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
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      await expect(csvDataFrame({
        url,
        byteLength: fileSize,
      })).rejects.toThrow()
      revoke()
    })

    it('should create a dataframe from a CSV file with only a header', async () => {
      const text = 'a'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
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
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
        chunkSize: 5,
      })
      expect(df.getCell({ row: 1, column: 'b' })).toStrictEqual({ value: '5' })
      expect(df.getCell({ row: 2, column: 'b' })).toBeUndefined()
      revoke()
    })

    it('should fetch more than initial rows when specified if the chunk size is bigger', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
        chunkSize: 500,
      })
      expect(df.getCell({ row: 1, column: 'b' })).toStrictEqual({ value: '5' })
      expect(df.getCell({ row: 2, column: 'b' })).not.toBeUndefined()
      revoke()
    })

    it.each([
      { text: 'a,b,c\n1111,2222,3333\nn44,55,66\n77,88,99\n', expectedRows: 2 },
      { text: 'a,b,c\n11,22,33\n44,55,66\n77,88,99\n', expectedRows: 3 },
      { text: 'a,b,c\n1,2,3\nn44,55,66\n77,88,99\n', expectedRows: 4 },
    ])('when the CSV file is not fully loaded, the number of rows might be inaccurate: $expectedRows (correct: 3)', async ({ text, expectedRows }) => {
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 1,
        chunkSize: 5,
      })
      // with only one row loaded, the average row size is not accurate enough to estimate the number of rows
      expect(df.numRows).toBe(expectedRows) // the estimate is not perfect
      expect(df.metadata).toEqual({ isNumRowsEstimated: true })
      revoke()
    })

    it('should fetch initial rows when specified, even if it is 0', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 0,
        chunkSize: 5,
      })
      expect(df.getCell({ row: 1, column: 'b' })).toBeUndefined()
      revoke()
    })

    it('should ignore empty rows when fetching initial rows', async () => {
      const text = 'a,b,c\n1,2,3\n\n7,8,9\n'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
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
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
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
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
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
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
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
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
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
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
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
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
        chunkSize: 5,
      })
      expect(df.getCell({ row: 2, column: 'a' })).toBeUndefined()
      revoke()
    })

    it('should return undefined for out-of-bound cells, if the dataframe is not fully loaded', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
        chunkSize: 5,
      })
      expect(df.getCell({ row: 5, column: 'a' })).toBeUndefined()
      revoke()
    })

    it('should throw for out-of-bound cells, if the dataframe is fully loaded', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(() => df.getCell({ row: 5, column: 'a' })).toThrow()
      revoke()
    })

    it('should throw when called with an orderBy parameter', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
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
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
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
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(() => df.getRowNumber({ row: -1 })).toThrow()
      revoke()
    })

    it('should return an estimated value when possible for not yet cached rows', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
        chunkSize: 5,
      })
      expect(df.getRowNumber({ row: 2 })).toEqual({ value: 2 })
      revoke()
    })

    it('should return undefined for out-of-bound rows, if the dataframe is not fully loaded', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
        chunkSize: 5,
      })
      expect(df.getRowNumber({ row: 5 })).toBeUndefined()
      revoke()
    })

    it('should throw for out-of-bound rows, if the dataframe is fully loaded', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      expect(() => df.getRowNumber({ row: 5 })).toThrow()
      revoke()
    })

    it('should throw when called with an orderBy parameter', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
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
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
        chunkSize: 5,
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
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 1,
        chunkSize: 5,
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
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
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
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
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

    it('does not throw when fetching out-of-bound rows, if the dataframe is fully loaded', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
      })
      await expect(df.fetch?.({ rowStart: 5, rowEnd: 10 })).resolves.toBeUndefined()
      revoke()
    })

    it('should fetch out-of-bound rows, if the dataframe is not fully loaded', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
        chunkSize: 5,
      })
      expect(df.getCell({ row: 2, column: 'a' })).toBeUndefined()
      await df.fetch?.({ rowStart: 2, rowEnd: 10 })
      expect(df.getCell({ row: 2, column: 'a' })).toStrictEqual({ value: '7' })
      revoke()
    })

    it('should fetch rows at a random position if requested', async () => {
      // padding the strings to have more consistent row sizes
      const text = 'a\n' + Array(100).fill(0).map((_, i) => i.toString().padStart(2, '0')).join('\n') + '\n'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 5,
        chunkSize: 5,
      })
      expect(df.getCell({ row: 0, column: 'a' })).toStrictEqual({ value: '00' })
      expect(df.getCell({ row: 30, column: 'a' })).toBeUndefined()
      await df.fetch?.({ rowStart: 30, rowEnd: 31 })
      // row 30 is now cached
      expect(df.getRowNumber({ row: 30 })).toStrictEqual({ value: 30 })
      expect(df.getCell({ row: 30, column: 'a' })).toStrictEqual({ value: '30' })
      // some rows around too
      expect(df.getCell({ row: 29, column: 'a' })).toStrictEqual({ value: '29' })
      expect(df.getCell({ row: 31, column: 'a' })).toStrictEqual({ value: '31' })
      // but not far away
      expect(df.getCell({ row: 20, column: 'a' })).toBeUndefined()
      expect(df.getCell({ row: 40, column: 'a' })).toBeUndefined()

      revoke()
    })

    it('will fetch an incorrect row if the average row size had been overestimated', async () => {
      const text = 'a\n' + '0'.repeat(10) + '\n' + Array(99).fill(0).map((_, i) => (i + 1).toString().padStart(2, '0')).join('\n')
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 1,
        chunkSize: 5,
      })
      expect(df.getCell({ row: 10, column: 'a' })).toBeUndefined()
      await df.fetch?.({ rowStart: 10, rowEnd: 11 })
      expect(df.getCell({ row: 10, column: 'a' })).toStrictEqual({ value: '27' }) // should be 10
      // fetch again, which might refresh the average row size
      await df.fetch?.({ rowStart: 10, rowEnd: 11 })
      expect(df.getCell({ row: 10, column: 'a' })).toStrictEqual({ value: '10' }) // should be 10
      // fetch again, which might refresh the average row size
      await df.fetch?.({ rowStart: 10, rowEnd: 11 })
      expect(df.getCell({ row: 10, column: 'a' })).toStrictEqual({ value: '09' }) // should be 10
      // fetch again, which might refresh the average row size
      await df.fetch?.({ rowStart: 10, rowEnd: 11 })
      expect(df.getCell({ row: 10, column: 'a' })).toStrictEqual({ value: '09' }) // should be 10
      revoke()
    })

    it('dispatches one "resolve" event per stored row in the range', async () => {
      const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n10,11,12\n13,14,15\n'
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 2,
        chunkSize: 5,
      })

      let resolveEventCount = 0
      df.eventTarget?.addEventListener('resolve', () => {
        resolveEventCount++
      })

      await df.fetch?.({ rowStart: 2, rowEnd: 5 })
      expect(resolveEventCount).toBe(3)

      // No event because rows are already resolved
      await df.fetch?.({ rowStart: 2, rowEnd: 5 })
      expect(resolveEventCount).toBe(3)

      revoke()
    })

    it('does nothing if trying to fetch random rows, and no initial rows are cached', async () => {
      const text = 'aaaaaaaaaa\n' + Array(100).fill(0).map((_, i) => i.toString().padStart(2, '0')).join('\n')
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        // no initial rows! -> no estimate
        initialRowCount: 0,
        chunkSize: 5,
      })

      let resolveEventCount = 0
      df.eventTarget?.addEventListener('resolve', () => {
        resolveEventCount++
      })

      // no estimate -> does nothing
      await df.fetch?.({ rowStart: 30, rowEnd: 31 })
      expect(resolveEventCount).toBe(0)

      // the first row can always be fetched
      await df.fetch?.({ rowStart: 0, rowEnd: 5 })
      expect(resolveEventCount).toBe(8)

      // now, the offset for row 30 can be estimated, and rows can be fetched
      await df.fetch?.({ rowStart: 30, rowEnd: 31 })
      expect(resolveEventCount).toBe(15)

      revoke()
    })

    it('does fetches only what is needed', async () => {
      const text = 'aa\n' + Array(100).fill(0).map((_, i) => i.toString().padStart(2, '0')).join('\n')
      const { url, revoke, fileSize } = toBlobURL(text, { withNodeWorkaround: true })
      const df = await csvDataFrame({
        url,
        byteLength: fileSize,
        initialRowCount: 20,
        chunkSize: 5,
      })

      let resolveEventCount = 0
      df.eventTarget?.addEventListener('resolve', () => {
        resolveEventCount++
      })

      // fetch the last rows
      await df.fetch?.({ rowStart: 80, rowEnd: 100 })
      expect(resolveEventCount).toBe(22)

      // fetch all the rows: only the missing rows should be fetched
      await df.fetch?.({ rowStart: 0, rowEnd: 100 })
      expect(resolveEventCount).toBe(80)

      revoke()
    })
  })
})
