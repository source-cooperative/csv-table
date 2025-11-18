import type { Newline, ParseResult } from 'csv-range'
import { isEmptyLine, parseURL } from 'csv-range'

import { formatBytes } from './helpers.js'

// TODO(SL): store the byte ranges for the rows, to be able to retrieve evicted rows later
// TODO(SL): store the byte ranges for the lines, not only the rows?

interface MissingRange {
  firstByte: number
  ignoreFirstRow: boolean
  lastByte: number
  ignoreLastRow: boolean
  maxNumRows: number | undefined
}

/**
 * A byte range in a CSV file, with the parsed rows
 */
export class CSVRange {
  #firstByte: number // byte position of the start of the range (excludes the ignored bytes if the range starts in the middle of a row)
  #byteCount = 0 // number of bytes in the range
  #rows: string[][] = [] // sorted array of the range rows, filtering out the empty rows and the header if any
  #firstRow: number // index of the first row in the range (0-based)

  constructor({ firstByte, firstRow }: { firstByte: number, firstRow: number }) {
    this.#firstByte = firstByte
    this.#firstRow = firstRow
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
   * Get the row number and last byte of row before the range
   * @returns The previous row number and last byte, or undefined if this is the first range
   */
  get previous(): { row: number, lastByte: number } | undefined {
    if (this.#firstByte === 0 || this.#firstRow === 0) {
      return undefined
    }
    const lastByte = this.#firstByte - 1
    const row = this.#firstRow - 1
    return { row, lastByte }
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

  get rows(): string[][] {
    return this.#rows
  }

  /**
   * Append a new row into the range
   * @param row The row to append. It must be contiguous to the last row.
   * @param row.byteOffset The byte offset of the row in the file.
   * @param row.byteCount The number of bytes of the row.
   * @param row.cells The cells of the row. If not provided, the row is considered ignored and not cached (i.e., empty or header).
   */
  append(row: { byteOffset: number, byteCount: number, cells?: string[] }) {
    if (row.byteOffset !== this.#firstByte + this.#byteCount) {
      throw new Error('Cannot append the row: it is not contiguous with the last row')
    }
    this.#byteCount = row.byteOffset + row.byteCount - this.#firstByte
    if (row.cells) {
      this.#rows.push(row.cells)
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
    if (row.byteOffset + row.byteCount !== this.#firstByte) {
      throw new Error('Cannot prepend the row: it is not contiguous with the first row')
    }
    this.#firstByte = row.byteOffset
    this.#byteCount += row.byteCount
    if (row.cells) {
      this.#firstRow -= 1
      this.#rows.unshift(row.cells)
    }
  }

  /**
   * Get the cells of a given row
   * @param options Options
   * @param options.row  The row number (0-based)
   * @returns The cells of the row, or undefined if the row is not in this range
   */
  getCells({ row }: { row: number }): string[] | undefined {
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
   * The header row
   */
  #header: ParseResult
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

  constructor({ header, byteLength }: { header: ParseResult, byteLength: number }) {
    if (header.meta.byteOffset + header.meta.byteCount > byteLength) {
      throw new Error('Header exceeds byte length')
    }
    this.#byteLength = byteLength
    this.#header = header
    this.#delimiter = header.meta.delimiter
    this.#newline = header.meta.newline
    this.#serial = new CSVRange({ firstByte: 0, firstRow: 0 })
    // Account for the header row and previous ignored rows if any
    this.#serial.append({
      byteOffset: 0,
      byteCount: header.meta.byteCount,
    })
    this.#random = []
    // TODO(SL): keep track of the errors
  }

  /**
   * Get the CSV column names
   * @returns The column names
   */
  get columnNames(): string[] {
    return this.#header.row.slice()
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
   * Get the CSV newline character(s)
   * @returns The CSV newline character(s)
   */
  get newline(): Newline {
    return this.#newline
  }

  /**
   * Update the average row byte count based on the cached rows
   */
  updateAverageRowByteCount(): void {
    // TODO(SL): after updating the average, we could try to re-assign row numbers in the random ranges to reduce overlaps
    const totalBytes = this.#serial.byteCount + this.#random.reduce((sum, range) => sum + range.byteCount, 0)
    const totalRows = this.#serial.rowCount + this.#random.reduce((sum, range) => sum + range.rowCount, 0)
    if (totalRows === 0) {
      this.#averageRowByteCount = undefined
    }
    const averageRowBytes = totalBytes / totalRows
    this.#averageRowByteCount = averageRowBytes
  }

  /**
   * Estimate the number of rows in the CSV file
   * @returns An object containing the estimated number of rows and a boolean indicating if it's an estimate
   */
  estimateNumRows(): { numRows: number, isEstimate: boolean } {
    if (this.#serial.byteCount === this.#byteLength) {
      return { numRows: this.#serial.rowCount, isEstimate: false }
    }
    if (this.#averageRowByteCount === undefined) {
      throw new Error('Cannot estimate number of rows: average row byte count is undefined')
    }
    if (this.#averageRowByteCount === 0) {
      throw new Error('Cannot estimate number of rows: average row byte count is zero')
    }
    return { numRows: Math.floor(this.#byteLength / this.#averageRowByteCount), isEstimate: true }
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
    // TODO(SL): how could we convey the fact that the row number is approximate (in #random)?
    return { value: row }
  }

  /**
   * Store a new row
   * @param row The row to store.
   * @param row.byteOffset The byte offset of the row in the file.
   * @param row.byteCount The number of bytes of the row.
   * @param row.cells The cells of the row. If not provided, the row is considered ignored and not cached (i.e., empty or header).
   */
  store(row: { byteOffset: number, byteCount: number, cells?: string[] }): void {
    // TODO(SL): add tests!
    let previousRange = this.#serial

    // loop on the ranges to find where to put the row
    for (const nextRange of [...this.#random, undefined]) {
      if (row.byteOffset < previousRange.next.firstByte) {
        throw new Error('Cannot cache the row: overlap with existing range')
      }
      if (row.byteOffset + row.byteCount > (nextRange?.previous?.lastByte ?? this.#byteLength - 1)) {
        throw new Error('Cannot cache the row: overlap with existing range')
      }
      if (row.byteOffset === previousRange.next.firstByte) {
        // append to the previous range
        previousRange.append(row)
        // merge with the next range if needed
        if (nextRange && previousRange.next.firstByte - 1 === (nextRange.previous?.lastByte ?? Infinity)) {
          // merge nextRange into previousRange
          this.#merge(previousRange, nextRange)
        }
      }
      else if (nextRange && row.byteOffset + row.byteCount === (nextRange.previous?.lastByte ?? Infinity) + 1) {
        // prepend to the next range
        nextRange.prepend(row)
      }
      else if (row.byteOffset < (nextRange?.previous?.lastByte ?? this.#byteLength - 1) + 1) {
        // create a new random range between previousRange and nextRange
        if (this.#averageRowByteCount === undefined || this.#averageRowByteCount === 0) {
          throw new Error('Cannot insert new range: average row byte count is undefined or zero')
        }
        const firstRow = Math.min(
          Math.round(previousRange.next.row + (row.byteOffset - previousRange.next.firstByte) / this.#averageRowByteCount),
          previousRange.next.row + 1, // ensure at least one row gap
        )
        // Note that we might have a situation where firstRow overlaps with nextRange.previous.row. It will be fixed the next time we update the average row byte count.
        const newRange = new CSVRange({ firstByte: row.byteOffset, firstRow })
        newRange.append(row)
        const nextIndex = nextRange ? this.#random.indexOf(nextRange) : this.#random.length
        const insertIndex = nextIndex === -1 ? this.#random.length : nextIndex
        this.#random.splice(insertIndex, 0, newRange)
      }
      else {
        // continue to next range
        previousRange = nextRange!
        continue
      }
      break
    }
  }

  /**
   * Merge two CSV ranges
   * @param range The first range. It can be the serial range, or a random range.
   * @param followingRange The second range, must be immediately after the first range. It is a random range.
   */
  #merge(range: CSVRange, followingRange: CSVRange): void {
    const index = this.#random.indexOf(followingRange)
    if (index === -1) {
      throw new Error('Cannot merge ranges: following range not found in cache')
    }
    range.merge(followingRange)
    // remove followingRange from the random ranges
    this.#random.splice(index, 1)
  }

  /**
   * Get the missing byte ranges for the given row range
   * @param options Options
   * @param options.rowStart The start row index (0-based, inclusive)
   * @param options.rowEnd The end row index (0-based, exclusive)
   * @returns An array of byte ranges to fetch
   */
  getMissingRowRanges({ rowStart, rowEnd }: { rowStart: number, rowEnd: number }): MissingRange[] {
    const missingRanges: MissingRange[] = []

    // try every empty range between known rows
    let first = this.#serial.next
    for (const { previous: last, next } of [...this.#random, { previous: { row: Infinity, lastByte: this.#byteLength - 1 }, next: { row: Infinity, firstByte: this.#byteLength } }]) {
      if (last === undefined) {
        // only the serial range should have no previous range
        throw new Error('Invalid range: missing previous range')
      }
      rowStart = Math.max(rowStart, first.row)
      if (rowEnd < rowStart) {
        // finished
        return missingRanges
      }
      if (rowStart <= last.row) {
        // there is an overlap
        const isStartContiguous = rowStart === first.row
        const ignoreFirstRow = !isStartContiguous // if not contiguous, we need to ignore the first row, because it could be partial
        const firstByte = isStartContiguous ? first.firstByte : first.firstByte + Math.floor((rowStart - first.row) * (this.#averageRowByteCount ?? 0))

        const rangeRowEnd = Math.min(rowEnd, last.row)
        const isEndContiguous = rangeRowEnd === last.row
        const ignoreLastRow = !isEndContiguous && last.row !== Infinity // if not contiguous and the last byte is defined, we need to ignore the last row, because it could be partial
        const lastByte = isEndContiguous || last.row === Infinity
          ? last.lastByte
          : last.lastByte - Math.floor((last.row - rangeRowEnd) * (this.#averageRowByteCount ?? 0))
        const maxNumRows = last.row === Infinity ? rangeRowEnd - rowStart : undefined

        missingRanges.push({ firstByte, ignoreFirstRow, lastByte, ignoreLastRow, maxNumRows })
      }
      first = next
    }

    return missingRanges
  }

  /**
   * Create a CSVCache from a remote CSV file URL
   * @param options Options
   * @param options.url The URL of the CSV file
   * @param options.byteLength The byte length of the CSV file
   * @param options.chunkSize The chunk size to use when fetching the CSV file
   * @param options.initialRowCount The initial number of rows to fetch
   * @param options.maxCachedBytes The maximum number of bytes to cache
   * @returns A promise that resolves to the CSVCache
   */
  static async fromURL({ url, byteLength, chunkSize, initialRowCount, maxCachedBytes }: { url: string, byteLength: number, chunkSize: number, initialRowCount: number, maxCachedBytes: number }): Promise<CSVCache> {
    if (chunkSize > maxCachedBytes) {
      throw new Error(
        `chunkSize (${formatBytes(chunkSize)}) cannot be greater than maxCachedBytes (${formatBytes(maxCachedBytes)})`,
      )
    }

    // type assertion is needed because Typescript cannot see if variable is updated in the Papa.parse step callback
    let cache: CSVCache | undefined = undefined

    // Fetch the first rows, including the header
    for await (const result of parseURL(url, { chunkSize })) {
      if (cache === undefined) {
        if (isEmptyLine(result.row, { greedy: true })) {
          continue // skip empty lines before the header
        }
        // first non-empty row is the header
        cache = new CSVCache({ header: result, byteLength })
        continue
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
      if (cache.rowCount >= initialRowCount) {
        // enough rows for now
        break
      }
    }

    if (cache === undefined) {
      throw new Error('No row found in the CSV file')
    }

    cache.updateAverageRowByteCount()

    return cache
  }
}
