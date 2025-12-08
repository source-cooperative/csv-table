import type { Newline, ParseResult } from 'cosovo'
import { createEventTarget } from 'hightable'

import { checkNonNegativeInteger } from './helpers.js'

export interface CSVCacheEvents {
  'num-rows-estimate-updated': {
    numRows: number
    isEstimate: boolean
  }
}

/**
 * A byte range in a CSV file, with the parsed rows
 */
export class CSVRange {
  #firstByte: number // byte position of the start of the range (excludes the ignored bytes if the range starts in the middle of a row)
  #byteCount = 0 // number of bytes in the range
  #rowByteCount = 0 // number of bytes in the range's rows (excludes ignored bytes)
  #rows: string[][] = [] // sorted array of the range rows, filtering out the empty rows and the header if any
  #firstRow: number // index of the first row in the range (0-based)

  constructor({ firstByte, firstRow }: { firstByte: number, firstRow: number }) {
    this.#firstByte = checkNonNegativeInteger(firstByte)
    this.#firstRow = checkNonNegativeInteger(firstRow)
  }

  /*
    * Get the number of rows in the range
    * @returns The number of rows in the range
    */
  get rowCount() {
    return this.#rows.length
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
   * Get the first row number of the range
   * @returns The first row number of the range
   */
  get firstRow(): number {
    return this.#firstRow
  }

  /**
   * Set the first row number of the range
   * @param value The first row number
   */
  set firstRow(value: number) {
    this.#firstRow = checkNonNegativeInteger(value)
  }

  /**
   * Get the row number and first byte of the next row in the range
   * @returns The next row number and first byte
   */
  get next(): { row: number, firstByte: number } {
    return {
      row: this.#firstRow + this.#rows.length,
      firstByte: this.#firstByte + this.#byteCount,
    }
  }

  /**
   * Get the rows in the range
   * @returns The rows in the range
   */
  get rows(): string[][] {
    return this.#rows
  }

  /**
   * Get the number of bytes covered by the rows in the range
   * @returns The number of bytes in the range's rows
   */
  get rowByteCount(): number {
    return this.#rowByteCount
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
      this.#rows.push(row.cells.slice())
      this.#rowByteCount += row.byteCount
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
      this.#firstRow -= 1
      this.#rows.unshift(row.cells.slice())
      this.#rowByteCount += row.byteCount
    }
  }

  /**
   * Get the cells of a given row
   * @param options Options
   * @param options.row  The row number (0-based)
   * @returns The cells of the row, or undefined if the row is not in this range
   */
  getCells({ row }: { row: number }): string[] | undefined {
    checkNonNegativeInteger(row)
    const rowIndex = row - this.#firstRow
    if (rowIndex < 0 || rowIndex >= this.#rows.length) {
      return undefined
    }
    return this.#rows[rowIndex]
  }

  /**
   * Merge another CSVRange into this one. The other range must be immediately after this one.
   * @param followingRange The range to merge
   */
  merge(followingRange: CSVRange): void {
    if (this.next.firstByte !== followingRange.firstByte) {
      throw new Error('Cannot merge ranges: not contiguous')
    }
    this.#byteCount += followingRange.byteCount
    this.#rowByteCount += followingRange.rowByteCount
    for (const row of followingRange.rows) {
      this.#rows.push(row)
    }
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
  /**
   * The average number of bytes per row, used for estimating row positions
   */
  #averageRowByteCount: number | undefined = undefined
  /**
   * The estimated number of rows in the CSV file
   */
  #numRowsEstimate: { numRows: number, isEstimate: boolean } = { numRows: 0, isEstimate: true }
  /**
   * An event target to emit events
   */
  #eventTarget = createEventTarget<CSVCacheEvents>()

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
    this.#serial = new CSVRange({ firstByte: 0, firstRow: 0 })
    // Account for the header row and previous ignored rows if any
    this.#serial.append({
      byteOffset: 0,
      byteCount: headerByteCount,
    })
    this.#random = []

    this.#updateAverageRowByteCount()
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
   * Get the number of rows in the cache
   * @returns The number of rows in the cache
   */
  get rowCount(): number {
    return this.#serial.rowCount + this.#random.reduce((sum, range) => sum + range.rowCount, 0)
  }

  /**
   * Get the CSV delimiter
   * @returns The CSV delimiter
   */
  get delimiter(): string {
    return this.#delimiter
  }

  /**
   * Get the number of columns
   * @returns The number of columns
   */
  get columnCount(): number {
    return this.#columnNames.length
  }

  /**
   * Get the CSV newline character(s)
   * @returns The CSV newline character(s)
   */
  get newline(): Newline {
    return this.#newline
  }

  /**
   * Get an estimate of the total number of rows in the CSV file
   * @returns The estimated number of rows and if it's an estimate
   */
  get numRowsEstimate(): { numRows: number, isEstimate: boolean } {
    return this.#numRowsEstimate
  }

  /**
   * Get the event target to listen to cache events
   * @returns The event target
   */
  get eventTarget(): ReturnType<typeof createEventTarget<CSVCacheEvents>> {
    return this.#eventTarget
  }

  /**
   * Update the average row byte count based on the cached rows
   */
  #updateAverageRowByteCount(): void {
    const rowByteCount = this.#serial.rowByteCount + this.#random.reduce((sum, range) => sum + range.rowByteCount, 0)
    const rowCount = this.#serial.rowCount + this.#random.reduce((sum, range) => sum + range.rowCount, 0)
    if (rowCount === 0) {
      this.#averageRowByteCount = undefined
    }
    else {
      this.#averageRowByteCount = rowByteCount / rowCount
    }
    this.#updateNumRowsEstimate()
  }

  /**
   * Re-assign row numbers in random ranges to reduce overlaps
   */
  updateRowEstimates(): void {
    const averageRowByteCount = this.averageRowByteCount
    if (averageRowByteCount === undefined || averageRowByteCount === 0) {
      return
    }

    let previousRange = this.#serial

    // loop on the random ranges
    for (const range of this.#random) {
      // v8 ignore if -- @preserve
      if (range.firstByte <= previousRange.next.firstByte) {
        // should not happen
        throw new Error('Cannot update row estimates: overlap with previous range')
      }

      const firstRow = Math.max(
        // ensure at least one row gap
        previousRange.next.row + 1,
        // estimate based on byte position
        Math.round(previousRange.next.row + (range.firstByte - previousRange.next.firstByte) / averageRowByteCount),
      )

      range.firstRow = firstRow

      previousRange = range
    }
  }

  get averageRowByteCount(): number | undefined {
    return this.#averageRowByteCount
  }

  get allRowsCached(): boolean {
    return this.#serial.next.firstByte >= this.#byteLength
  }

  /**
   * Update the last range row number, if it ends at the end of the file.
   */
  #updateLastRangeRowNumber(): void {
    const last = this.#random[this.#random.length - 1]
    if (last === undefined || last.next.firstByte < this.#byteLength) {
      return
    }
    // update the last range first row number
    const totalRows = this.#numRowsEstimate.numRows
    const lastRangeRowCount = last.rowCount
    last.firstRow = totalRows - lastRangeRowCount
    // dispatch an event to let the listeners know that the row numbers have changed
    this.#eventTarget.dispatchEvent(new CustomEvent('resolve'))
  }

  /**
   * Update the estimated number of rows in the CSV file
   */
  #updateNumRowsEstimate(): void {
    const averageRowByteCount = this.averageRowByteCount
    const numRows = this.allRowsCached
      ? this.rowCount
      : averageRowByteCount === 0 || averageRowByteCount === undefined
        ? 0
        : Math.round((this.#byteLength - this.headerByteCount) / averageRowByteCount)
    const isEstimate = !this.allRowsCached
    if (this.#numRowsEstimate.numRows !== numRows || this.#numRowsEstimate.isEstimate !== isEstimate) {
      this.#numRowsEstimate = { numRows, isEstimate }
      this.#eventTarget.dispatchEvent(new CustomEvent('num-rows-estimate-updated'))
    }
    this.#updateLastRangeRowNumber()
  }

  /**
   * Get the cells of a given row
   * @param options Options
   * @param options.row  The row number (0-based)
   * @returns The cells of the row, or undefined if the row is not in this range
   */
  #getCells({ row }: { row: number }): string[] | undefined {
    const cells = this.#serial.getCells({ row })
    if (cells !== undefined) {
      return cells
    }
    // find the range containing this row
    for (const range of this.#random) {
      const cells = range.getCells({ row })
      if (cells !== undefined) {
        return cells
      }
    }
    return undefined
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
    if (column >= this.columnCount) {
      throw new Error(`Column index out of bounds: ${column}`)
    }
    const cells = this.#getCells({ row })
    if (cells === undefined) {
      return undefined
    }
    return {
      // return empty string for missing columns in existing row
      value: cells[column] ?? '',
    }
  }

  /**
   * Get the row number for the given row index.
   * @param options Options
   * @param options.row The row index (0-based)
   * @returns The row number, or undefined if not found
   */
  getRowNumber({ row }: { row: number }): { value: number } | undefined {
    if (this.#getCells({ row }) === undefined) {
      return undefined
    }
    return { value: row }
  }

  /**
   * Store a new row
   * @param row The row to store.
   * @param row.byteOffset The byte offset of the row in the file.
   * @param row.byteCount The number of bytes of the row.
   * @param row.firstRow The first row index (0-based) used if a new range is created.
   * @param row.cells The cells of the row. If not provided, the row is considered ignored and not cached (i.e., empty or header).
   */
  store(row: { byteOffset: number, byteCount: number, firstRow: number, cells?: string[] }): void {
    checkNonNegativeInteger(row.byteOffset)
    checkNonNegativeInteger(row.byteCount)
    if (row.byteOffset + row.byteCount > this.#byteLength) {
      throw new Error('Cannot store the row: byte range is out of bounds')
    }

    let previousRange = this.#serial

    // loop on the ranges to find where to put the row
    for (const [i, nextRange] of [...this.#random, undefined].entries()) {
      if (row.byteOffset < previousRange.next.firstByte) {
        throw new Error('Cannot store the row: overlap with previous range')
      }

      // the row is after the next range
      if (nextRange && row.byteOffset >= nextRange.next.firstByte) {
        previousRange = nextRange
        continue
      }

      if (nextRange && row.byteOffset + row.byteCount > nextRange.firstByte) {
        throw new Error('Cannot store the row: overlap with next range')
      }

      // append to the previous range
      if (row.byteOffset === previousRange.next.firstByte) {
        previousRange.append(row)
        // merge with the next range if needed
        if (nextRange && previousRange.next.firstByte === nextRange.firstByte) {
          // merge nextRange into previousRange
          this.#merge(previousRange, nextRange)
        }
        break
      }

      // prepend to the next range
      if (nextRange && row.byteOffset + row.byteCount === nextRange.firstByte) {
        nextRange.prepend(row)
        break
      }

      // create a new random range between previousRange and nextRange (if any)
      // Note that we might have a situation where firstRow overlaps with other ranges.
      const newRange = new CSVRange({ firstByte: row.byteOffset, firstRow: row.firstRow })
      newRange.append(row)
      this.#random.splice(i, 0, newRange)
      break
    }

    // Update the average row byte count
    this.#updateAverageRowByteCount()
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

  /**
   * Get the next missing row for the given row range
   * @param options Options
   * @param options.rowStart The start row index (0-based, inclusive)
   * @param options.rowEnd The end row index (0-based, exclusive)
   * @param options.prefixRowsToEstimate Number of average prefix rows to prefetch when estimating (because the first row may be partial). Defaults to 3.
   * @returns undefined if no missing row, or an object with:
   *  - the first byte to start fetching data
   *  - the first byte to store data (after ignoring some bytes)
   *  - the expected row number of the first fetched row
   */
  getNextMissingRow({ rowStart, rowEnd, prefixRowsToEstimate }: { rowStart: number, rowEnd: number, prefixRowsToEstimate?: number }): { firstByteToFetch: number, firstByteToStore: number, firstRow: number } | undefined {
    checkNonNegativeInteger(rowStart)
    checkNonNegativeInteger(rowEnd)
    prefixRowsToEstimate ??= 3
    checkNonNegativeInteger(prefixRowsToEstimate)

    // try every empty range between cached rows
    let cursor = this.#serial.next

    if (cursor.firstByte >= this.#byteLength) {
      // No missing row if all rows are cached
      return undefined
    }

    for (const nextRange of [...this.#random, { firstRow: Infinity, next: { row: Infinity, firstByte: this.#byteLength } }]) {
      if (rowStart < cursor.row) {
        // ignore cached rows
        rowStart = cursor.row
      }
      if (rowEnd <= rowStart) {
        // no missing row (rowEnd is exclusive)
        return
      }
      if (rowStart < nextRange.firstRow) {
        // the next row to fetch is between the cursor and the next range
        if (rowStart === cursor.row || this.averageRowByteCount === undefined) {
          // if the requested row is the cursor, we can use its firstByte property directly
          // Same if we cannot estimate positions
          return { firstByteToFetch: cursor.firstByte, firstByteToStore: cursor.firstByte, firstRow: cursor.row }
        }

        // estimate the byte position based on the average row byte count.
        const gapRows = rowStart - cursor.row
        const gapBytes = Math.round(gapRows * this.averageRowByteCount)
        // Start storing here:
        const firstByteToStore = Math.max(cursor.firstByte + gapBytes, 0)
        // avoid going beyond the end of the file
        if (firstByteToStore >= this.#byteLength) {
          return undefined
        }

        // Start fetching some rows before:
        const prefixBytes = Math.round(prefixRowsToEstimate * this.averageRowByteCount) // number of bytes to ignore at the start when estimating
        const firstByteToFetch = Math.max(firstByteToStore - prefixBytes, 0)

        return {
          firstByteToFetch,
          firstByteToStore,
          firstRow: rowStart,
        }
      }
      // try the next missing range
      cursor = nextRange.next
    }
  }

  /**
   * Check if the given byte range is stored in the cache.
   * @param options Options
   * @param options.byteOffset The byte offset of the range.
   * @returns True if the byte range is stored, false otherwise.
   */
  isStored({ byteOffset }: { byteOffset: number }): boolean {
    checkNonNegativeInteger(byteOffset)

    for (const range of [this.#serial, ...this.#random]) {
      if (range.firstByte <= byteOffset && byteOffset < range.next.firstByte) {
        return true
      }
    }

    return false
  }

  static fromHeader({ header, byteLength }: { header: ParseResult, byteLength: number }): CSVCache {
    return new CSVCache({
      columnNames: header.row,
      byteLength,
      delimiter: header.meta.delimiter,
      newline: header.meta.newline,
      headerByteCount: header.meta.byteOffset + header.meta.byteCount,
    })
  }
}
