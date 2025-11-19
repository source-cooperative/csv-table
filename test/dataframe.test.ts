import { toURL } from 'csv-range'
import { describe, expect, it } from 'vitest'

import { csvDataFrame } from '../src/dataframe'

describe('csvDataFrame', () => {
  it('should create a dataframe from a CSV file', async () => {
    const text = 'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
    const { url, revoke, fileSize } = toURL(text)
    // TODO(SL): make it sync, by passing the header?
    const df = await csvDataFrame({
      url,
      byteLength: fileSize - 1, // Exclude the extra character ' ' added by toURL to handle nug in Node.js (see toURL)
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
})
