import type { Newline, ParseResult } from 'csv-range'

import { checkNonNegativeInteger } from './helpers.js'

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
   * Update the average row byte count based on the cached rows
   */
  #updateAverageRowByteCount(): void {
    const rowByteCount = this.#serial.rowByteCount + this.#random.reduce((sum, range) => sum + range.rowByteCount, 0)
    const rowCount = this.#serial.rowCount + this.#random.reduce((sum, range) => sum + range.rowCount, 0)
    if (rowCount === 0) {
      this.#averageRowByteCount = undefined
      return
    }
    const averageRowByteCount = rowByteCount / rowCount
    this.#averageRowByteCount = averageRowByteCount
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
   * @param row.cells The cells of the row. If not provided, the row is considered ignored and not cached (i.e., empty or header).
   */
  store(row: { byteOffset: number, byteCount: number, cells?: string[] }): void {
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

      // create a new random range between previousRange and nextRange
      const averageRowByteCount = this.averageRowByteCount
        ? this.averageRowByteCount
        : row.byteCount // use the current row byte count if we don't have an average yet (0 or undefined)
      const firstRow = Math.max(
        Math.round(previousRange.next.row + (row.byteOffset - previousRange.next.firstByte) / averageRowByteCount),
        previousRange.next.row + 1, // ensure at least one row gap
      )
      // Note that we might have a situation where firstRow overlaps with nextRange.previous.row. It will be fixed the next time we update the average row byte count.
      const newRange = new CSVRange({ firstByte: row.byteOffset, firstRow })
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
   * @returns The first byte of the next missing row and if it's an estimate, or undefined if no missing row
   */
  getNextMissingRow({ rowStart, rowEnd }: { rowStart: number, rowEnd: number }): { firstByte: number, isEstimate: boolean } | undefined {
    checkNonNegativeInteger(rowStart)
    checkNonNegativeInteger(rowEnd)

    // try every empty range between cached rows
    let first = this.#serial.next

    if (first.firstByte >= this.#byteLength) {
      // No missing row if all rows are cached
      return undefined
    }

    for (const { firstRow, next } of [...this.#random, { firstRow: Infinity, next: { row: Infinity, firstByte: this.#byteLength } }]) {
      if (rowStart < first.row) {
        // ignore cached rows
        rowStart = first.row
      }
      if (rowEnd <= rowStart) {
        // no missing row (rowEnd is exclusive)
        return
      }
      if (rowStart < firstRow) {
        // the first row is in this missing range
        if (rowStart === first.row || this.averageRowByteCount === undefined) {
          // if the start row is the same as the first row, we can use the first byte directly
          // Same if we cannot estimate positions
          return { firstByte: first.firstByte, isEstimate: false }
        }
        // estimate the byte position based on the average row byte count, trying to get the middle of the previous row
        const delta = Math.floor((rowStart - first.row - 0.5) * this.averageRowByteCount)
        const firstByte = first.firstByte + Math.max(0, delta)

        // avoid going beyond the end of the file
        if (firstByte >= this.#byteLength) {
          return undefined
        }

        return {
          firstByte,
          isEstimate: true,
        }
      }
      // try the next missing range
      first = next
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
