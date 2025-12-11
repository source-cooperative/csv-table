import type { Newline, ParseResult } from 'cosovo'

import { checkInteger, checkNonNegativeInteger } from './helpers.js'

interface RowStored {
  status: 'stored'
  range: CSVRange
  cells: string[]
  firstRangeRow: {
    value: number
    isEstimate: boolean
  }
}

interface RowMissing {
  status: 'missing'
  leftRange: CSVRange
  rightRange?: CSVRange
  byteOffset: {
    value: number
    isEstimate: boolean
  }
}

interface RowBeyondEOF {
  status: 'beyond-eof'
  isEstimate: boolean
}

interface RowUnknown {
  status: 'unknown'
}

type RowStatus = RowStored | RowMissing | RowBeyondEOF | RowUnknown

/**
 * Cache of parsed rows
 */
export class RowsCache {
  #rows: string[][] = []
  #byteCount = 0

  get rows(): string[][] {
    return this.#rows
  }

  get byteCount(): number {
    return this.#byteCount
  }

  get numRows(): number {
    return this.#rows.length
  }

  append(row: { byteCount: number, cells: string[] }) {
    checkNonNegativeInteger(row.byteCount)
    this.#rows.push(row.cells.slice())
    this.#byteCount += row.byteCount
  }

  prepend(row: { byteCount: number, cells: string[] }) {
    checkNonNegativeInteger(row.byteCount)
    this.#rows.unshift(row.cells.slice())
    this.#byteCount += row.byteCount
  }

  merge(other: RowsCache) {
    for (const row of other.rows) {
      this.#rows.push(row)
    }
    this.#byteCount += other.byteCount
  }
}

/**
 * A byte range in a CSV file, with the parsed rows
 */
export class CSVRange {
  #firstByte: number // byte position of the start of the range (excludes the ignored bytes if the range starts in the middle of a row)
  #byteCount: number = 0 // number of bytes in the range (includes ignored bytes)
  #rowsCache: RowsCache // the cached rows. It excludes the ignored rows (empty rows and header if any)

  constructor({ firstByte }: { firstByte: number }) {
    this.#firstByte = checkNonNegativeInteger(firstByte)
    this.#rowsCache = new RowsCache()
  }

  /*
    * Get the slice of cached rows
    * @returns The slice of cached rows
    */
  get rowsCache() {
    return this.#rowsCache
  }

  /**
   * Get the number of bytes covered by the range
   * @returns The number of bytes in the range
   */
  get byteCount() {
    return this.#byteCount
  }

  /**
   * Get the first byte of the range
   * @returns The first byte of the range
   */
  get firstByte(): number {
    return this.#firstByte
  }

  /**
   * Get the first byte of the next row in the range
   * @returns The next row's first byte
   */
  get nextByte(): number {
    return this.#firstByte + this.#byteCount
  }

  /**
   * Append a new row into the range
   * @param row The row to append. It must be contiguous to the last row.
   * @param row.byteOffset The byte offset of the row in the file.
   * @param row.byteCount The number of bytes of the row.
   * @param row.cells The cells of the row. If not provided, the row is considered ignored and not cached (i.e., empty or header).
   */
  append(row: { byteOffset: number, byteCount: number, cells?: string[] }) {
    checkNonNegativeInteger(row.byteOffset)
    checkNonNegativeInteger(row.byteCount)
    if (row.byteOffset !== this.#firstByte + this.#byteCount) {
      throw new Error('Cannot append the row: it is not contiguous with the last row')
    }
    this.#byteCount = row.byteOffset + row.byteCount - this.#firstByte
    if (row.cells) {
      this.#rowsCache.append({ cells: row.cells, byteCount: row.byteCount })
    }
  }

  /**
   * Prepend a new row into the range
   * @param row The row to prepend. It must be contiguous to the first row.
   * @param row.byteOffset The byte offset of the row in the file.
   * @param row.byteCount The number of bytes of the row.
   * @param row.cells The cells of the row. If not provided, the row is considered ignored and not cached (i.e., empty or header).
   */
  prepend(row: { byteOffset: number, byteCount: number, cells?: string[] }): void {
    checkNonNegativeInteger(row.byteOffset)
    checkNonNegativeInteger(row.byteCount)
    if (row.byteOffset + row.byteCount !== this.#firstByte) {
      throw new Error('Cannot prepend the row: it is not contiguous with the first row')
    }
    this.#firstByte = row.byteOffset
    this.#byteCount += row.byteCount
    if (row.cells) {
      this.#rowsCache.prepend({ cells: row.cells, byteCount: row.byteCount })
    }
  }

  /**
   * Merge another CSVRange into this one. The other range must be immediately after this one.
   * @param followingRange The range to merge
   */
  merge(followingRange: CSVRange): void {
    if (this.nextByte !== followingRange.firstByte) {
      throw new Error('Cannot merge ranges: not contiguous')
    }
    this.#byteCount += followingRange.byteCount
    this.#rowsCache.merge(followingRange.rowsCache)
  }

  /**
   * Get the cells of a given cached row (0-based index in this range, no ignored rows)
   * @param rowIndex  The row index in this range (0-based)
   * @returns The cells of the row, or undefined if the row is not in this range
   */
  getRow(rowIndex: number): string[] | undefined {
    // if negative or out of bounds, return undefined
    checkInteger(rowIndex)
    return this.#rowsCache.rows[rowIndex]
  }
}

/**
 * Cache of a remote CSV file
 */
export class CSVCache {
  /**
   * The total byte length of the CSV file
   */
  #byteLength: number
  /**
   * The column names
   */
  #columnNames: string[]
  /**
   * The header byte count
   */
  #headerByteCount: number
  /**
   * The serial range, starting at byte 0
   */
  #serial: CSVRange
  /**
   * The random access ranges, after the serial range
   */
  #random: CSVRange[]
  /**
   * The CSV delimiter
   */
  #delimiter: string
  /**
   * The CSV newline character(s)
   */
  #newline: Newline

  constructor({ columnNames, headerByteCount, byteLength, delimiter, newline }: { columnNames: string[], headerByteCount?: number, byteLength: number, delimiter: string, newline: Newline }) {
    headerByteCount ??= 0
    checkNonNegativeInteger(headerByteCount)
    checkNonNegativeInteger(byteLength)
    if (columnNames.length === 0) {
      throw new Error('Cannot create CSVCache: no column names provided')
    }
    if (headerByteCount > byteLength) {
      throw new Error('Initial byte count exceeds byte length')
    }
    this.#byteLength = byteLength
    this.#columnNames = columnNames.slice()
    this.#headerByteCount = headerByteCount
    this.#delimiter = delimiter
    this.#newline = newline

    const serial = new CSVRange({ firstByte: 0 })
    // Account for the header row and previous ignored rows if any
    serial.append({
      byteOffset: 0,
      byteCount: headerByteCount,
    })
    this.#serial = serial
    this.#random = []
  }

  /**
   * Create a CSVCache from a header row
   * @param options Options
   * @param options.header The parsed header row
   * @param options.byteLength The byte length of the CSV file
   * @returns A new CSVCache instance
   */
  static fromHeader({ header, byteLength }: { header: ParseResult, byteLength: number }): CSVCache {
    return new CSVCache({
      columnNames: header.row,
      byteLength,
      delimiter: header.meta.delimiter,
      newline: header.meta.newline,
      headerByteCount: header.meta.byteOffset + header.meta.byteCount,
    })
  }

  /**
   * Get the number of bytes in the CSV file
   * @returns The number of bytes in the CSV file
   */
  get byteLength(): number {
    return this.#byteLength
  }

  /**
   * Get the CSV column names
   * @returns The column names
   */
  get columnNames(): string[] {
    return this.#columnNames.slice()
  }

  /**
   * Get the header byte count
   * @returns The header byte count
   */
  get headerByteCount(): number {
    return this.#headerByteCount
  }

  /**
   * Get the CSV delimiter
   * @returns The CSV delimiter
   */
  get delimiter(): string {
    return this.#delimiter
  }

  /**
   * Get the CSV newline character(s)
   * @returns The CSV newline character(s)
   */
  get newline(): Newline {
    return this.#newline
  }

  /**
   * Get the serial range
   * @returns The serial range
   */
  get serialRange(): CSVRange {
    return this.#serial
  }

  /**
   * Get the random access ranges
   * @returns The random access ranges
   */
  get randomRanges(): CSVRange[] {
    return this.#random.slice()
  }

  /**
   * Check if the entire CSV file is cached
   * @returns True if the entire CSV file is cached
   */
  get complete(): boolean {
    // v8 ignore if -- @preserve
    if (this.#serial.nextByte > this.#byteLength) {
      throw new Error('Inconsistent state: serial range exceeds the file length')
    }
    const complete = this.#serial.nextByte === this.#byteLength
    // v8 ignore if -- @preserve
    if (complete && this.#random.length > 0) {
      throw new Error('Inconsistent state: serial range covers the entire file, but there are random ranges')
    }
    return complete
  }

  /**
   * Store a new row
   * If the byte range is already cached, false is immediately returned.
   * If only part of the byte range is cached, an error is thrown.
   * @param row The row to store.
   * @param row.byteOffset The byte offset of the row in the file.
   * @param row.byteCount The number of bytes of the row.
   * @param row.cells The cells of the row. If not provided, the row is considered ignored and not cached (i.e., empty or header).
   * @returns True if the row was stored successfully.
   */
  store(row: { byteOffset: number, byteCount: number, cells?: string[] }): boolean {
    checkNonNegativeInteger(row.byteOffset)
    checkNonNegativeInteger(row.byteCount)

    if (row.byteOffset + row.byteCount > this.#byteLength) {
      throw new Error('Cannot store the row: byte range is out of bounds')
    }

    let leftRange = this.#serial

    // loop on the ranges to find where to put the row
    for (const [i, rightRange] of [...this.#random, undefined].entries()) {
      // at this point, the row cannot start before the left range

      // v8 ignore if -- @preserve
      if (row.byteOffset < leftRange.firstByte) {
        throw new Error('Inconsistent state: row is before the left range')
      }

      if (row.byteOffset < leftRange.nextByte) {
        // the row starts inside the left range
        if (row.byteOffset + row.byteCount > leftRange.nextByte) {
          // but it ends after the left range: we cannot store the row
          throw new Error('Cannot store the row: the first bytes are already cached, but not the last ones.')
        }
        // it ends inside the left range: the row is already cached
        return false
      }
      // at this point, the row starts after the left range

      if (rightRange && row.byteOffset >= rightRange.firstByte) {
        // the row starts inside the right range or after. Go to the next range.
        leftRange = rightRange
        continue
      }

      // at this point, the row starts before the right range (or there is no right range)

      if (rightRange && row.byteOffset + row.byteCount > rightRange.firstByte) {
        // but it ends after the start of the right range: we cannot store the row
        throw new Error('Cannot store the row: the first bytes are not cached, but other bytes are already cached.')
      }

      // at this point, the row ends before the right range (or there is no right range).
      // The row can be contiguous to the left range, or to the right range, or to both, or isolated.
      // It will be stored.

      if (row.byteOffset === leftRange.nextByte) {
        // The row is contiguous to the left range: append it.
        leftRange.append(row)
        // merge with the right range if needed (after appending to the left range)
        if (rightRange && leftRange.nextByte === rightRange.firstByte) {
          // merge the left and right ranges
          this.#merge(leftRange, rightRange)
        }
        return true
      }

      // at this point, the row is not contiguous to the left range

      if (rightRange && row.byteOffset + row.byteCount === rightRange.firstByte) {
        // The row is contiguous to the right range: prepend it.
        rightRange.prepend(row)
        return true
      }

      // at this point, the row is not contiguous to either range

      // create a new random range and insert it after the left range
      const newRange = new CSVRange({ firstByte: row.byteOffset })
      newRange.append(row)
      this.#random.splice(i, 0, newRange)
      return true
    }

    // v8 ignore next -- @preserve
    throw new Error('Inconsistent state: this point should not be reachable')
  }

  /**
   * Merge two CSV ranges
   * @param range The first range. It can be the serial range, or a random range.
   * @param followingRange The second range, must be immediately after the first range. It is a random range.
   */
  #merge(range: CSVRange, followingRange: CSVRange): void {
    const index = this.#random.indexOf(followingRange)
    // v8 ignore if -- @preserve
    if (index === -1) {
      throw new Error('Cannot merge ranges: following range not found in cache')
    }
    range.merge(followingRange)
    // remove followingRange from the random ranges
    this.#random.splice(index, 1)
  }
}

export class Estimator {
  /**
   * The CSV cache
   */
  #cache: CSVCache

  /**
   * The average number of bytes per row, used for estimating row positions. Undefined if the cache is complete.
   */
  #averageRowByteCount: number | undefined = 0

  constructor({ cache }: { cache: CSVCache }) {
    this.#cache = cache
  }

  /**
   * Get a copy of the estimator
   * @returns A copy of the estimator
   */
  copy(): Estimator {
    const copy = new Estimator({ cache: this.#cache })
    copy.#averageRowByteCount = this.#averageRowByteCount
    return copy
  }

  /**
   * Get the estimated number of rows in the CSV file
   * @returns The estimated number of rows
   */
  get numRows(): number {
    return this.#averageRowByteCount === 0
      ? 0
      : this.#averageRowByteCount === undefined
        ? this.#computeNumCachedRows()
        : Math.round((this.#cache.byteLength - this.#cache.headerByteCount) / this.#averageRowByteCount)
  }

  /**
   * Get if the number of rows is estimated
   * @returns True if the number of rows is estimated
   */
  get isNumRowsEstimated(): boolean {
    return this.#averageRowByteCount !== undefined
  }

  /**
   * Get the maximum number of rows (infinity if estimated, numRows if exact)
   * @returns The maximum number of rows
   */
  get maxNumRows(): number {
    return this.isNumRowsEstimated ? Infinity : this.numRows
  }

  /**
   * Get the cell value at the given row and column
   * @param options Options
   * @param options.row The row index (0-based)
   * @param options.column The column index (0-based)
   * @returns The cell value, or undefined if the row is not in the cache
   */
  getCell({ row, column }: { row: number, column: number }): { value: string } | undefined {
    checkNonNegativeInteger(column)
    if (column >= this.#cache.columnNames.length) {
      throw new Error(`Column index out of bounds: ${column}`)
    }
    const status = this.getStatus({ row, snapEOFToNumRows: true })
    if (status.status !== 'stored') {
      return undefined
    }
    return {
      // return empty string for missing columns in existing row
      value: status.cells[column] ?? '',
    }
  }

  /**
   * Get the row number for the given row index.
   * @param options Options
   * @param options.row The row index (0-based)
   * @returns The row number, or undefined if not found
   */
  getRowNumber({ row }: { row: number }): { value: number } | undefined {
    if (row >= 0 && row < this.numRows) {
      return { value: row }
    }
  }

  /**
   * Guess the next missing row, searching from minRow
   * @param options Options
   * @param options.minRow The minimum row number (0-based)
   * @returns An object defining the first missing row, with the byte offset,
   * the row number, and if the offset is estimated.
   * Returns undefined if all the rows from minRow are already cached,
   * or if no estimation is possible.
   */
  getFirstMissingRow({ minRow }: { minRow: number }): {
    row: number
    byteOffset: {
      value: number
      isEstimate: boolean
    }
  } | undefined {
    const status = this.getStatus({ row: minRow })
    if (status.status === 'missing') {
      return {
        row: minRow,
        byteOffset: status.byteOffset,
      }
    }

    if (status.status === 'stored') {
      const nextRow = status.firstRangeRow.value + status.range.rowsCache.numRows
      if (status.range.nextByte >= this.#cache.byteLength) {
        return undefined
      }
      return {
        row: nextRow,
        byteOffset: {
          value: status.range.nextByte,
          isEstimate: false, // the previous row is stored, so the offset is exact
        },
      }
    }

    // other cases: beyond-eof, unknown
    return undefined
  }

  /**
   * Guess the last missing row, searching backwards from maxRow
   * @param options Options
   * @param options.maxRow The maximum row number (0-based)
   * @returns The last missing row number.
   * Returns undefined if all the rows before maxRow are already cached,
   * or if no estimation is possible.
   */
  getLastMissingRowNumber({ maxRow }: { maxRow: number }): number | undefined {
    const status = this.getStatus({ row: maxRow })

    if (status.status === 'missing') {
      return maxRow
    }

    if (status.status === 'stored') {
      const firstRangeRow = status.firstRangeRow.value
      if (firstRangeRow === 0) {
        return undefined
      }
      return firstRangeRow - 1
    }

    // other cases: beyond-eof, unknown
    return undefined
  }

  /**
   * Refresh the internal state (average row byte count)
   * Don't update the internal state if the change is not significant (<1%)
   * @returns True if the internal state has been updated
   */
  refresh(): boolean {
    const numCachedBytes = this.#computeNumCachedBytes()
    const numCachedRows = this.#computeNumCachedRows()
    const complete = this.#cache.complete

    if (complete) {
      if (this.#averageRowByteCount === undefined) {
        // already in the same state
        return false
      }
      // update
      this.#averageRowByteCount = undefined
      return true
    }

    // v8 ignore if -- @preserve
    if (this.#averageRowByteCount === undefined) {
      throw new Error('Incoherent state: the cache state cannot go from complete to incomplete')
    }

    if (numCachedRows === 0) {
      // no progress
      return false
    }

    const averageRowByteCount = numCachedBytes / numCachedRows

    if (
      this.#averageRowByteCount === 0
      || Math.abs(averageRowByteCount - this.#averageRowByteCount) / this.#averageRowByteCount > 0.01
    ) {
      this.#averageRowByteCount = averageRowByteCount
      return true
    }

    // no significant changes, ignore
    return false
  }

  /**
   * Get the status of a given row
   * @param options Options
   * @param options.row The row number (0-based, non-negative integer)
   * @param options.snapEOFToNumRows If true, if the last range ends at the EOF, ensure the row numbers match the estimated number of rows
   * @returns The status of the row
   */
  getStatus({ row, snapEOFToNumRows }: { row: number, snapEOFToNumRows?: boolean }): RowStatus {
    checkNonNegativeInteger(row)

    if (this.numRows > 0 && row >= this.numRows) {
      return {
        status: 'beyond-eof',
        isEstimate: this.isNumRowsEstimated,
      }
    }

    let left = {
      range: this.#cache.serialRange,
      firstRow: 0,
      isEstimate: false,
    }

    // 4 cases to consider:
    // - inside the left range
    // - just after the left range
    // - after the left range and before the right range (the right range can be undefined, meaning the end of the file)
    // - not before the right range (continue to the next range)
    for (const rightRange of [...this.#cache.randomRanges, undefined]) {
      const leftNextRow = left.firstRow + left.range.rowsCache.numRows

      // first case: inside a range
      if (row < leftNextRow) {
        const cells = left.range.getRow(row - left.firstRow)
        // v8 ignore if -- @preserve
        if (cells === undefined) {
          // sanity check: the range should contain at least one row
          throw new Error('Incoherent state: the range should contain at least one row')
        }
        return {
          status: 'stored',
          range: left.range,
          cells,
          firstRangeRow: {
            value: left.firstRow,
            isEstimate: left.isEstimate,
          },
        }
      }

      // second case: just after a range
      if (row === leftNextRow) {
        return {
          status: 'missing',
          leftRange: left.range,
          rightRange,
          byteOffset: {
            value: left.range.nextByte,
            isEstimate: false, // the previous row is stored, so the offset is exact
          },
        }
      }

      // third case: between two ranges

      // v8 ignore if -- @preserve
      if (this.#averageRowByteCount === undefined) {
        // the cache is complete, no need to fetch
        throw new Error('Incoherent state: the cache is complete, we should have returned earlier.')
      }
      if (this.#averageRowByteCount === 0) {
        // no estimation available (empty cache, and asking for a row at the middle of the file)
        return {
          status: 'unknown',
        }
      }

      // Estimate the number of the first row in the right range
      const rightFirstRow = rightRange === undefined
        ? this.numRows
        // special case: if the right range ends at the end of the file, we can compute from the total number of rows
        // (under a flag, as it can lead to gaps between rows). Detail: removing -1, because it seems to fix a bug
        // in cosovo? (weirdly, the last row is sometimes not counted properly)
        : snapEOFToNumRows && (rightRange.nextByte >= this.#cache.byteLength - 1) && (rightRange.rowsCache.numRows > 0)
          ? this.numRows - rightRange.rowsCache.numRows
          : leftNextRow + Math.round((rightRange.firstByte - left.range.nextByte) / this.#averageRowByteCount)

      // third case: between two ranges
      if (row < rightFirstRow) {
        return {
          status: 'missing',
          leftRange: left.range,
          rightRange,
          byteOffset: {
            value: left.range.nextByte + Math.round((row - leftNextRow) * this.#averageRowByteCount),
            isEstimate: true, // estimated offset
          },
        }
      }

      // fourth case: not before the right range (continue to the next range)
      // v8 ignore else -- @preserve
      if (rightRange !== undefined) {
        left = {
          range: rightRange,
          firstRow: rightFirstRow,
          isEstimate: true,
        }
      }
    }

    // v8 ignore next -- @preserve
    throw new Error('Incoherent state: this point should not be reachable')
  }

  /**
   * Get the number of cached rows
   * @returns The number of cached rows
   */
  #computeNumCachedRows(): number {
    return this.#cache.serialRange.rowsCache.numRows + this.#cache.randomRanges.reduce((sum, range) => sum + range.rowsCache.numRows, 0)
  }

  /**
   * Get the number of cached bytes
   * @returns The number of cached bytes
   */
  #computeNumCachedBytes(): number {
    return this.#cache.serialRange.rowsCache.byteCount + this.#cache.randomRanges.reduce((sum, range) => sum + range.rowsCache.byteCount, 0)
  }
}
