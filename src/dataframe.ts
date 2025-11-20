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
import { checkNonNegativeInteger } from './helpers.js'

const defaultChunkSize = 500 * 1024 // 500 KB
const defaultInitialRowCount = 500
// const paddingRowCount = 20 // fetch a bit before and after the requested range, to avoid cutting rows

interface Params {
  url: string
  byteLength: number // total byte length of the file
  chunkSize?: number // download chunk size
  initialRowCount?: number // number of rows to fetch at dataframe creation
}

export type CSVDataFrame = DataFrame<{ isNumRowsEstimated: boolean }>

/**
 * Helpers to load a CSV file as a dataframe
 * @param params - params for creating the dataframe
 * @param params.url - URL of the CSV file
 * @param params.byteLength - total byte length of the file
 * @param params.chunkSize - download chunk size
 * @param params.initialRowCount - number of rows to fetch at dataframe creation
 * @returns DataFrame representing the CSV file
 */
export async function csvDataFrame(params: Params): Promise<CSVDataFrame> {
  const chunkSize = params.chunkSize ?? defaultChunkSize
  const initialRowCount = params.initialRowCount ?? defaultInitialRowCount
  const { url, byteLength } = params

  const eventTarget = createEventTarget<DataFrameEvents>()
  const cache = await initializeCSVCachefromURL({ url, byteLength, chunkSize, initialRowCount })
  const averageRowByteCount = cache.averageRowByteCount
  const numRows = cache.allRowsCached
    ? cache.rowCount
    : averageRowByteCount === 0 || averageRowByteCount === undefined
      ? 0
      : Math.round((byteLength - cache.headerByteCount) / averageRowByteCount)
  const metadata = {
    isNumRowsEstimated: !cache.allRowsCached,
  }
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
    // until the CSV is fully loaded, we don't know the exact number of rows
    const numRows = cache.allRowsCached ? cache.rowCount : Infinity
    validateGetCellParams({
      row,
      column,
      orderBy,
      data: {
        numRows,
        columnDescriptors,
      },
    })
    const columnIndex = columnDescriptors.findIndex(
      cd => cd.name === column,
    )
    // v8 ignore if -- @preserve
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
    // until the CSV is fully loaded, we don't know the exact number of rows
    const numRows = cache.allRowsCached ? cache.rowCount : Infinity
    validateGetRowNumberParams({
      row,
      orderBy,
      data: {
        numRows,
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

    if (cache.allRowsCached) {
      // all rows are already cached
      if (rowEnd > cache.rowCount) {
        // requested rows are beyond the end of the file
        throw new Error(`Requested rows are beyond the end of the file: ${rowEnd} > ${cache.rowCount}`)
      }
      // else nothing to do
      return
    }

    const maxLoops = (rowEnd - rowStart) + 10 // safety to avoid infinite loops
    let hasFetchedSomeRows = false

    let i = 0
    while (true) {
      // fetch all missing ranges
      i++
      let next = cache.getNextMissingRow({ rowStart, rowEnd })
      let j = 0
      while (next) {
      // fetch next missing range
        j++
        // v8 ignore if -- @preserve
        if (j > maxLoops) {
        // should not happen
          throw new Error('Maximum fetch loops exceeded')
        }
        const firstByte = next.firstByte
        const ignoreFirstRow = next.isEstimate // if it's an estimate, we may be cutting a row
        let isFirstRow = true
        let k = 0
        for await (const result of parseURL(url, {
          delimiter: cache.delimiter,
          newline: cache.newline,
          chunkSize,
          firstByte,
          lastByte: byteLength - 1,
        })) {
          checkSignal(signal)
          k++
          // v8 ignore if -- @preserve
          if (k > maxLoops) {
          // should not happen
            throw new Error('Maximum parse loops exceeded')
          }
          if (isFirstRow && ignoreFirstRow) {
            isFirstRow = false
            continue
          }
          if (result.meta.byteCount === 0) {
          // no progress, avoid infinite loop
          // it's the last line in the file and it's empty
            next = undefined
            break
          }

          // Store the new row in the cache
          if (!cache.isStored({ byteOffset: result.meta.byteOffset })) {
            const isEmpty = isEmptyLine(result.row)
            cache.store({
              cells: isEmpty ? undefined : result.row,
              byteOffset: result.meta.byteOffset,
              byteCount: result.meta.byteCount,
            })
            hasFetchedSomeRows ||= !isEmpty
          }

          // next row
          next = cache.getNextMissingRow({ rowStart, rowEnd })
          if (!next) {
          // no more missing ranges
            break
          }
          const nextByte = result.meta.byteOffset + result.meta.byteCount
          if (next.firstByte > nextByte + chunkSize) {
          // the next missing range is beyond the current chunk, so we can stop the current loop and start a new fetch
            break
          }
          // otherwise, continue fetching in the current loop,
          // Note that some rows might already be cached. It's ok since fetching takes more time than parsing.
        }

        if (k === 0) {
          // No progress (no row fetched in this missing range)
          // Break to avoid infinite loop
          break
          // For example, it occurs when the estimated byte offset is beyond the end of the file.
          // To fix that, we could fetch more rows at the start to improve the estimation, then retry.
          // See https://github.com/source-cooperative/csv-table/issues/11
        }
      }

      // update the cache stats (average row size, firstRow of each random access block, etc.)
      cache.updateRowEstimates()

      // and then check again if all the requested rows have been fetched
      const updatedNext = cache.getNextMissingRow({ rowStart, rowEnd })

      // if all the requested rows are now cached, we can exit
      if (!updatedNext) {
        break
      }

      // if we made no progress, we can also exit to avoid infinite loops
      if (next && updatedNext.firstByte === next.firstByte) {
        break
      }

      // v8 ignore if -- @preserve
      if (i >= maxLoops) {
        // should not happen
        throw new Error('Maximum estimation loops exceeded')
      }

      // else, continue the loop
      next = updatedNext
    }

    // Dispatch resolve event if some rows were fetched
    // We do it only at the end, because the row numbers might change while fetching, producing instable behavior.
    if (hasFetchedSomeRows) {
      eventTarget.dispatchEvent(new CustomEvent('resolve'))
    }
  }

  return {
    metadata,
    numRows,
    columnDescriptors,
    getCell,
    getRowNumber,
    fetch,
    eventTarget,
  }
}

/**
 * Create a CSVCache from a remote CSV file URL
 * @param options Options
 * @param options.url The URL of the CSV file
 * @param options.byteLength The byte length of the CSV file
 * @param options.chunkSize The chunk size to use when fetching the CSV file
 * @param options.initialRowCount The initial number of rows to fetch
 * @returns A promise that resolves to the CSVCache
 */
async function initializeCSVCachefromURL({ url, byteLength, chunkSize, initialRowCount }: { url: string, byteLength: number, chunkSize: number, initialRowCount: number }): Promise<CSVCache> {
  checkNonNegativeInteger(byteLength)
  checkNonNegativeInteger(chunkSize)
  checkNonNegativeInteger(initialRowCount)

  // type assertion is needed because Typescript cannot see if variable is updated in the Papa.parse step callback
  let cache: CSVCache | undefined = undefined

  // Fetch the first rows, including the header
  for await (const result of parseURL(url, { chunkSize, lastByte: byteLength - 1 })) {
    if (cache === undefined) {
      if (isEmptyLine(result.row, { greedy: true })) {
        continue // skip empty lines before the header
      }
      // first non-empty row is the header
      cache = CSVCache.fromHeader({ header: result, byteLength })
      continue
    }
    else if (cache.rowCount >= initialRowCount) {
      // enough rows for now
      break
    }
    else {
      // data row
      cache.store({
        // ignore empty lines
        cells: isEmptyLine(result.row) ? undefined : result.row,
        byteOffset: result.meta.byteOffset,
        byteCount: result.meta.byteCount,
      })
    }
  }

  if (cache === undefined) {
    throw new Error('No row found in the CSV file')
  }

  return cache
}
