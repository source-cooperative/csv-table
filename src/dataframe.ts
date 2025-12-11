import { isEmptyLine, parseURL } from 'cosovo'
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

import { CSVCache, Estimator } from './cache'
import { checkNonNegativeInteger } from './helpers.js'

const defaultChunkSize = 100 * 1024 // 100 KB
const defaultInitialRowCount = 50
// const paddingRowCount = 20 // fetch a bit before and after the requested range, to avoid cutting rows

interface Params {
  url: string
  byteLength: number // total byte length of the file
  chunkSize?: number // download chunk size
  initialRowCount?: number // number of rows to fetch at dataframe creation
}

// Note that when sending a 'numrowsupdate' event, the isNumRowsEstimated flag is also updated if needed
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
  const estimator = new Estimator({ cache })
  estimator.refresh()
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
    validateGetCellParams({
      row,
      column,
      orderBy,
      data: {
        // until the CSV is fully loaded, we don't know the exact number of rows
        numRows: estimator.maxNumRows,
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
    return estimator.getCell({ row, column: columnIndex })
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
    validateGetRowNumberParams({
      row,
      orderBy,
      data: {
        // until the CSV is fully loaded, we don't know the exact number of rows
        numRows: estimator.maxNumRows,
        columnDescriptors,
      },
    })
    return estimator.getRowNumber({ row })
  }

  /**
   * Fetch the given range of rows, filling the cache as needed.
   * The row numbers are only known exactly for the first range of rows, the rest are estimated. To avoid
   * unstability, the estimation is updated only at the end of the fetch.
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

    const updated = estimator.refresh()
    if (updated) {
      // propagate event
      eventTarget.dispatchEvent(new CustomEvent('numrowschange'))
    }

    if (cache.complete) {
      // all rows are cached
      return
    }

    validateFetchParams({
      rowStart,
      rowEnd,
      columns,
      orderBy,
      data: {
        // until the CSV is fully loaded, we don't know the exact number of rows
        numRows: Infinity,
        columnDescriptors,
      },
    })

    // Compute the byte range to fetch
    for (let r = rowStart; r < rowEnd; r++) {
      if (!estimator.isStored({ row: r })) {
        break
      }
      rowStart++
    }
    for (let r = rowEnd; r > rowStart; r--) {
      if (!estimator.isStored({ row: r - 1 })) {
        break
      }
      rowEnd--
    }
    if (rowEnd <= rowStart) {
      // all rows are already cached
      return
    }

    // fetch rows from rowStart to rowEnd (exclusive), with 3 extra rows before and after
    const extraRows = 3
    const fetchRowStart = Math.max(0, rowStart - extraRows)
    const fetchRowEnd = Math.min(rowEnd + extraRows)

    // we could set initialState to 'default' if firstByte is exactly at the start of a row
    // TODO(SL): implement it, by inspecting all the cache ranges, instead of doing a global average
    const firstByte = estimator.guessByteOffset({ row: fetchRowStart })
    const lastBytePlusOne = estimator.guessByteOffset({ row: fetchRowEnd })
    if (firstByte === undefined) {
      // cannot estimate
      return
    }
    const lastByte = lastBytePlusOne ? lastBytePlusOne - 1 : firstByte - 1 // fetch at least one row

    const stats = {
      parsedRows: 0,
      alreadyStored: 0,
      newEmpty: 0,
      newFull: 0,
      ignored: 0,
      reachedEOF: false,
    }

    try {
      for await (const result of parseURL(url, {
        delimiter: cache.delimiter,
        newline: cache.newline,
        chunkSize,
        firstByte,
        lastByte: byteLength - 1,
        initialState: 'detect',
      })) {
        stats.parsedRows++
        // Check if the signal has been aborted
        checkSignal(signal)

        if (stats.parsedRows <= 1) {
          // we might have started parsing in the middle of a row, ignore this first row
          stats.ignored += 1
          continue
        }
        if (result.meta.byteCount === 0) {
          // no progress, avoid infinite loop
          // it's the last line in the file and it's empty
          stats.ignored += 1
          break
        }

        // Store the new row in the cache
        const isEmpty = isEmptyLine(result.row)

        // store if not in the cache yet
        const stored = cache.store({
          cells: isEmpty ? undefined : result.row,
          byteOffset: result.meta.byteOffset,
          byteCount: result.meta.byteCount,
        })

        if (!stored) {
          stats.alreadyStored++
        }
        else if (isEmpty) {
          stats.newEmpty++
        }
        else {
          eventTarget.dispatchEvent(new CustomEvent('resolve'))
          stats.newFull++
        }

        if (result.meta.byteOffset > byteLength - 1) {
          // end of file
          stats.reachedEOF = true
        }
        if (result.meta.byteOffset > lastByte) {
          // end of the requested range
          stats.ignored += 1
          break
        }
      }
    }
    finally {
      // Note: we don't update the estimates after the fetch, to avoid unstability during user interactions.
      // Exception if the cache is now complete, or we reached the end of the file.
      if (cache.complete || stats.reachedEOF === true) {
        estimator.refresh()
        eventTarget.dispatchEvent(new CustomEvent('numrowschange'))
      }
    }
  }

  return {
    metadata: {
      get isNumRowsEstimated() {
        return estimator.isNumRowsEstimated
      },
    },
    get numRows() {
      return estimator.numRows
    },
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
  let storedRows = 0
  for await (const result of parseURL(url, { chunkSize, lastByte: byteLength - 1 })) {
    if (cache === undefined) {
      if (isEmptyLine(result.row, { greedy: true })) {
        continue // skip empty lines before the header
      }
      // first non-empty row is the header
      cache = CSVCache.fromHeader({ header: result, byteLength })
      continue
    }
    else if (storedRows >= initialRowCount && result.meta.byteOffset > 0.9 * chunkSize) {
      // enough rows for now
      break
    }
    else {
      const isEmpty = isEmptyLine(result.row)
      // data row
      const stored = cache.store({
        // ignore empty lines
        cells: isEmpty ? undefined : result.row,
        byteOffset: result.meta.byteOffset,
        byteCount: result.meta.byteCount,
      })
      if (stored && !isEmpty) {
        storedRows++
      }
    }
  }

  if (cache === undefined) {
    throw new Error('No row found in the CSV file')
  }

  return cache
}
