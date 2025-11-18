// TODO(SL): should we return the dataframe after parsing one row, and then keep parsing the chunk, but triggering updates?)
// TODO(SL): configure if the CSV has a header or not?
// TODO(SL): evict old rows (or only cell contents?) if needed
// TODO(SL): handle fetching (and most importantly storing) only part of the columns?
// Note that source.coop does not support negative ranges for now https://github.com/source-cooperative/data.source.coop/issues/57 (for https://github.com/hyparam/hightable/issues/298#issuecomment-3381567614)
import { isEmptyLine, parseURL } from 'csv-range'
import type {
  DataFrame,
  DataFrameEvents,
  OrderBy,
  ResolvedValue,
} from 'hightable'
import {
  checkSignal,
  createEventTarget,
  validateFetchParams,
  validateGetCellParams,
  validateGetRowNumberParams,
} from 'hightable'

import { CSVCache } from './cache'

const defaultChunkSize = 50 * 1024 // 50 KB
const initialRowCount = 50 // number of rows to fetch initially to estimate the average row size
// const paddingRowCount = 20 // fetch a bit before and after the requested range, to avoid cutting rows

interface Params {
  url: string
  byteLength: number // total byte length of the file
  chunkSize?: number // download chunk size
  maxCachedBytes?: number // max number of bytes to keep in cache before evicting old rows
}

/**
 * Helpers to load a CSV file as a dataframe
 * @param options - options for creating the dataframe
 * @param options.url - URL of the CSV file
 * @param options.byteLength - total byte length of the file
 * @param options.chunkSize - download chunk size
 * @returns DataFrame representing the CSV file
 */
export async function csvDataFrame({ url, byteLength, chunkSize }: Params): Promise<DataFrame> {
  chunkSize ??= defaultChunkSize

  const eventTarget = createEventTarget<DataFrameEvents>()
  const cache = await CSVCache.fromURL({ url, byteLength, chunkSize, initialRowCount })
  const averageRowByteCount = cache.averageRowByteCount
  if (averageRowByteCount === undefined || averageRowByteCount === 0) {
    throw new Error('Cannot create dataframe: not enough data to estimate number of rows')
  }
  const numRows = cache.allRowsCached ? cache.rowCount : Math.floor(byteLength / averageRowByteCount)
  // TODO(SL): add metadata to tell if the number of rows is an estimate or exact?
  const columnDescriptors: DataFrame['columnDescriptors'] = cache.columnNames.map(name => ({ name }))

  /**
   * Get the cached cell value at the given row and column.
   * @param options - options
   * @param options.row - row index
   * @param options.column - column name
   * @param options.orderBy - optional sorting order
   * @returns The cell value, or undefined if not cached
   */
  function getCell({
    row,
    column,
    orderBy,
  }: {
    row: number
    column: string
    orderBy?: OrderBy
  }): ResolvedValue | undefined {
    // TODO(SL): how to handle the last rows when the number of rows is uncertain?
    validateGetCellParams({
      row,
      column,
      orderBy,
      data: {
        numRows: Infinity, // we don't (always) know the exact number of rows yet
        columnDescriptors,
      },
    })
    const columnIndex = columnDescriptors.findIndex(
      cd => cd.name === column,
    )
    if (columnIndex === -1) {
      // should not happen because of the validation above
      throw new Error(`Column not found: ${column}`)
    }
    return cache.getCell({ row, column: columnIndex })
  }

  /**
   * Get the cached row number for the given row index.
   * @param options - options
   * @param options.row - row index
   * @param options.orderBy - optional sorting order
   * @returns The row number, or undefined if not cached
   */
  function getRowNumber({
    row,
    orderBy,
  }: {
    row: number
    orderBy?: OrderBy
  }): ResolvedValue<number> | undefined {
    // TODO(SL): how to handle the last rows when the number of rows is uncertain?
    validateGetRowNumberParams({
      row,
      orderBy,
      data: {
        numRows: Infinity, // we don't (always) know the exact number of rows yet
        columnDescriptors,
      },
    })
    return cache.getRowNumber({ row })
  }

  /**
   * Fetch the given range of rows, filling the cache as needed.
   * @param options - options
   * @param options.rowStart - starting row index
   * @param options.rowEnd - ending row index (exclusive)
   * @param options.columns - optional list of columns to fetch
   * @param options.orderBy - optional sorting order
   * @param options.signal - optional abort signal
   */
  async function fetch({
    rowStart,
    rowEnd,
    columns,
    orderBy,
    signal,
  }: {
    rowStart: number
    rowEnd: number
    columns?: string[]
    orderBy?: OrderBy
    signal?: AbortSignal
  }): Promise<void> {
    checkSignal(signal)

    validateFetchParams({
      rowStart,
      rowEnd,
      columns,
      orderBy,
      data: {
        numRows: Infinity, // we don't (always) know the exact number of rows yet
        columnDescriptors,
      },
    })

    const maxLoops = (rowEnd - rowStart) + 10 // safety to avoid infinite loops
    const fetchChunkSize = chunkSize ?? defaultChunkSize
    let next = cache.getNextMissingRow({ rowStart, rowEnd })
    let i = 0
    while (next) {
      i++
      if (i > maxLoops) {
        throw new Error('Maximum fetch loops exceeded')
      }
      const firstByte = next.firstByte
      const ignoreFirstRow = next.isEstimate // if it's an estimate, we may be cutting a row
      let isFirstRow = true
      let j = 0
      for await (const result of parseURL(url, {
        delimiter: cache.delimiter,
        newline: cache.newline,
        chunkSize: fetchChunkSize,
        firstByte,
      })) {
        checkSignal(signal)
        j++
        if (j > maxLoops) {
          throw new Error('Maximum parse loops exceeded')
        }
        if (isFirstRow && ignoreFirstRow) {
          isFirstRow = false
          continue
        }
        if (result.meta.byteOffset < next.firstByte) {
          // already cached
          continue
        }
        const isEmpty = isEmptyLine(result.row)
        cache.store({
          cells: isEmpty ? undefined : result.row,
          byteOffset: result.meta.byteOffset,
          byteCount: result.meta.byteCount,
        })
        if (!isEmpty) {
          eventTarget.dispatchEvent(new CustomEvent('resolve'))
        }

        // next row
        next = cache.getNextMissingRow({ rowStart, rowEnd })
        if (!next) {
          // no more missing ranges
          break
        }
        const nextByte = result.meta.byteOffset + result.meta.byteCount
        if (next.firstByte > nextByte + fetchChunkSize) {
          // the next missing range is beyond the current chunk, so we can stop the current loop and start a new fetch
          break
        }
        if (next.firstByte <= nextByte) {
          // after storing the current row, the next missing row is estimated to be before the current cursor, so we have to stop fetching and start a new loop
          break
        }
        // otherwise, continue fetching in the current loop, even if some rows are already cached
      }
    }
  }

  return {
    numRows,
    columnDescriptors,
    getCell,
    getRowNumber,
    fetch,
    eventTarget,
  }
}
