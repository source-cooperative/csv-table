import type {
  CustomEventTarget,
  DataFrame,
  DataFrameEvents,
  OrderBy,
  ResolvedValue,
} from "hightable";
import {
  checkSignal,
  createEventTarget,
  validateFetchParams,
  validateGetCellParams,
  validateGetRowNumberParams,
} from "hightable";
import { formatBytes } from "./helpers.js";
import { parseCSV, CSVRow } from "./csv.js";

interface Metadata {
  isPartial: boolean;
  cachedBytes: number;
}
export type CSVDataFrame = DataFrame<Metadata>;

const defaultChunkSize = 50 * 1024; // 50 KB, same as Papaparse default
const defaultMaxCachedBytes = 20 * 1024 * 1024; // 20 MB
const paddingRows = 20; // fetch a bit before and after the requested range, to avoid cutting rows

interface Params {
  url: string;
  byteLength: number; // total byte length of the file
  chunkSize?: number; // download chunk size
  maxCachedBytes?: number; // max number of bytes to keep in cache before evicting old rows
  signal?: AbortSignal; // to abort the DataFrame creation and any ongoing fetches
}

// // ranges are sorted. We use binary search to find the missing ranges, and then merge them if needed
// interface CSVRange {
//   start: number; // byte position of the start of the range (excludes the ignored bytes if the range starts in the middle of a row)
//   bytes: number; // number of bytes in the range
//   end: number; // first byte after the range (redundant: start + bytes)
//   validRows: CSVRow[]; // sorted array of the range rows, filtering out the empty rows and the header if any
// }

class CSVRange {
  start: number; // byte position of the start of the range (excludes the ignored bytes if the range starts in the middle of a row)
  bytes: number; // number of bytes in the range
  validRows: CSVRow[]; // sorted array of the range rows, filtering out the empty rows and the header if any

  constructor(start: number, bytes: number, validRows: CSVRow[]) {
    this.start = start;
    this.bytes = bytes;
    this.validRows = validRows;
  }

  // first byte after the range (redundant: start + bytes)
  get end(): number {
    return this.start + this.bytes;
  }
}

interface Cache {
  header: CSVRow;
  serial: CSVRange;
  random: CSVRange[];
  cachedBytes: number; // total number of bytes cached (for statistics)
  chunkSize: number; // chunk size used for fetching
  url: string;
  averageRowBytes: number; // average number of bytes per row
}

/**
 * Helpers to load a CSV file as a dataframe
 */
export async function csvDataFrame({
  url,
  byteLength,
  chunkSize,
  maxCachedBytes,
  signal,
}: Params): Promise<CSVDataFrame> {
  checkSignal(signal);
  chunkSize ??= defaultChunkSize;
  maxCachedBytes ??= defaultMaxCachedBytes;

  if (chunkSize > maxCachedBytes) {
    throw new Error(
      `chunkSize (${formatBytes(chunkSize)}) cannot be greater than maxCachedBytes (${formatBytes(maxCachedBytes)})`
    );
  }

  const eventTarget = createEventTarget<DataFrameEvents>();

  // const parsedRowIndex: ParsedRowIndex = new Map(); // first byte offset -> parsed row // TODO(SL): delete? I think it's not needed

  // type assertion is needed because Typescript cannot see if variable is updated in the Papa.parse step callback
  let header = undefined as CSVRow | undefined;
  let cachedBytes = 0;

  // Fetch the first chunk (stop at 80% of the chunk size, to avoid doing another fetch, as we have no way to limit to one chunk in Papaparse)
  // TODO(SL): should we return the dataframe after parsing one row, and then keep parsing the chunk, but triggering updates?)
  const serial: CSVRange = {
    start: 0,
    bytes: 0,
    end: 0,
    validRows: [],
  };
  const { partial: isPartial } = await parseCSV(url, {
    chunkSize,
    delimiter: ",", // TODO(SL): auto detect
    newline: "\n", // TODO(SL): auto detect
    step: (row, parser) => {
      // update the range size, even if the row is empty
      serial.bytes += row.bytes;
      if (
        serial.bytes >= 0.8 * chunkSize || // stop at 80% of the chunk size, to avoid doing another fetch, as we have no way to limit to one chunk in Papaparse
        serial.validRows.length >= 100
      ) {
        // abort the parsing, we have enough rows for now
        parser.abort();
        return;
      }
      if (isEmpty(row.data)) {
        // empty row, ignore
        return;
      }
      // for the statistics (we store the data, be it the header or a data row):
      cachedBytes += row.bytes;
      if (header === undefined) {
        // first non-empty row: header
        header = row;
      } else {
        // valid row: add it to the range
        serial.validRows.push(row);
      }
      // the errors field is ignored
    },
  });
  console.log(serial);
  if (header === undefined) {
    throw new Error("No header row found in the CSV file");
  }

  const averageRowBytes = getAverageRowBytes({
    serial,
    header,
    random: [],
  });

  const cache: Cache = {
    header,
    serial,
    random: [],
    cachedBytes,
    chunkSize,
    url,
    averageRowBytes,
  };

  const numRows =
    isPartial && averageRowBytes
      ? Math.floor(byteLength / averageRowBytes) // see https://github.com/hyparam/hightable/issues/298
      : serial.validRows.length;

  const columnDescriptors: DataFrame["columnDescriptors"] = header.data.map(
    (name) => ({ name })
  );
  const metadata: Metadata = {
    isPartial,
    cachedBytes,
  };

  function getCell({
    row,
    column,
    orderBy,
  }: {
    row: number;
    column: string;
    orderBy?: OrderBy;
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
    });
    const parsedRow = findParsedRow({ cache, row });
    if (parsedRow) {
      const columnIndex = columnDescriptors.findIndex(
        (cd) => cd.name === column
      );
      if (columnIndex === -1) {
        // should not happen because of the validation above
        throw new Error(`Column not found: ${column}`);
      }
      const value = parsedRow.row.data[columnIndex]; // TODO(SL): we could convert to a type, here or in the cache
      // return value ? { value } : undefined;
      return { value: value ?? "" }; // return empty cells as empty strings, because we assume that all the row has been parsed
    }
    return undefined;
  }

  function getRowNumber({
    row,
    orderBy,
  }: {
    row: number;
    orderBy?: OrderBy;
  }): ResolvedValue<number> | undefined {
    // TODO(SL): how to handle the last rows when the number of rows is uncertain?
    validateGetRowNumberParams({
      row,
      orderBy,
      data: {
        numRows: Infinity, // we don't (always) know the exact number of rows yet
        columnDescriptors,
      },
    });
    const parsedRow = findParsedRow({ cache, row });
    if (parsedRow?.type === "serial") {
      return { value: row };
    }
    if (parsedRow?.type === "random") {
      // TODO(SL): how could we convey the fact that the row number is approximate?
      return { value: row };
    }
  }

  async function fetch({
    rowStart,
    rowEnd,
    columns,
    orderBy,
    signal,
  }: {
    rowStart: number;
    rowEnd: number;
    columns?: string[];
    orderBy?: OrderBy;
    signal?: AbortSignal;
  }): Promise<void> {
    checkSignal(signal);

    validateFetchParams({
      rowStart,
      rowEnd,
      columns,
      orderBy,
      data: {
        numRows: Infinity, // we don't (always) know the exact number of rows yet
        columnDescriptors,
      },
    });

    let previousAverageRowBytes = undefined as number | undefined;
    let i = 0;
    while (previousAverageRowBytes !== cache.averageRowBytes && i < 10) {
      i++; // to avoid infinite loops in case of instability

      if (rowEnd < cache.serial.validRows.length) {
        // all rows are in the serial range
        return;
      }
      if (rowStart < cache.serial.validRows.length) {
        // ignore the rows already cached
        rowStart = cache.serial.validRows.length;
      }

      const estimatedStart = Math.floor(
        serial.end +
          (rowStart - cache.serial.validRows.length) * cache.averageRowBytes
      );
      const estimatedEnd = Math.min(
        byteLength,
        Math.ceil(
          serial.end +
            (rowEnd - cache.serial.validRows.length) * cache.averageRowBytes
        )
      );
      // find the ranges of rows we don't have yet
      // start with the full range, and then remove the parts we have
      const missingRange = {
        start: estimatedStart,
        end: estimatedEnd,
      };
      const missingRanges: { start: number; end: number }[] = [];
      // Loop on the random ranges, which are sorted and non-overlapping
      for (const range of cache.random) {
        if (missingRange.end <= range.start) {
          // no overlap, and no more overlap possible
          missingRanges.push(missingRange);
          break;
        }
        if (missingRange.start >= range.end) {
          // no overlap, check the next range
          continue;
        }
        // overlap
        if (missingRange.start < range.start) {
          // add the part before the overlap
          missingRanges.push({
            start: missingRange.start,
            end: range.start,
          });
        }
        // move the start to the end of the range
        missingRange.start = range.end;
        if (missingRange.start >= missingRange.end) {
          // no more missing range
          break;
        }
      }
      if (missingRange.start < missingRange.end) {
        // add the remaining part
        missingRanges.push(missingRange);
      }

      if (missingRanges.length === 0) {
        // all rows are already cached
        return;
      }

      // fetch each missing range and fill the cache
      await Promise.all(
        missingRanges.map(({ start, end }) =>
          fetchRange({ start, end, signal, cache, eventTarget })
        )
      ).finally(() => {
        // TODO(SL): Update the average size of a row?
        // For now, we keep it constant, to provide stability - otherwise empty rows appear after the update
        previousAverageRowBytes = cache.averageRowBytes;
        cache.averageRowBytes = getAverageRowBytes(cache);
        //eventTarget.dispatchEvent(new CustomEvent("resolve")); // to refresh the table (hmmm. Or better call fetch again until we reach stability?)
      });
    }

    // TODO(SL): evict old rows (or only cell contents?) if needed
    // TODO(SL): handle fetching (and most importantly storing) only part of the columns?
    // Note that source.coop does not support negative ranges for now https://github.com/source-cooperative/data.source.coop/issues/57 (for https://github.com/hyparam/hightable/issues/298#issuecomment-3381567614)
  }

  return {
    metadata,
    numRows,
    columnDescriptors,
    getCell,
    getRowNumber,
    fetch,
    eventTarget,
  };
}

function findParsedRow({ cache, row }: { cache: Cache; row: number }):
  | {
      type: "serial" | "random";
      row: CSVRow;
    }
  | undefined {
  // TODO(SL): optimize (cache the row numbers?)
  const serialRow = cache.serial.validRows[row];
  if (serialRow) {
    return {
      type: "serial",
      row: serialRow,
    };
  }
  const serialEnd = cache.serial.start + cache.serial.bytes;
  const estimatedStart =
    serialEnd + (row - cache.serial.validRows.length) * cache.averageRowBytes;
  // find the range containing this row
  const range = cache.random.find(
    (r) => estimatedStart >= r.start && estimatedStart < r.start + r.bytes
  );
  if (!range) {
    return; // not found
  }
  // estimate the row index of the first row in the range
  const firstRowIndex =
    cache.serial.validRows.length +
    Math.round(
      // is .round() better than .floor() or .ceil()?
      (range.start - serialEnd) / cache.averageRowBytes
    );
  // get the row in the range. This way, we ensure that calls to findParsedRow() with increasing row numbers
  // will return rows in the same order, without holes or duplicates, even if the averageRowBytes is not accurate.
  const randomRow = range.validRows[row - firstRowIndex];
  if (!randomRow) {
    return; // not found
  }
  return {
    type: "random",
    row: randomRow,
  };
}

function getAverageRowBytes(
  cache: Pick<Cache, "serial" | "header" | "random">
): number {
  let numRows = cache.serial.validRows.length;
  let numBytes = cache.serial.bytes - cache.header.bytes;

  for (const range of cache.random) {
    numRows += range.validRows.length;
    numBytes += range.bytes;
  }
  if (numRows === 0 || numBytes === 0) {
    throw new Error("No data row found in the CSV file");
  }
  return numBytes / numRows;
}

async function fetchRange({
  start,
  end,
  signal,
  cache,
  eventTarget,
}: {
  start: number;
  end: number;
  signal?: AbortSignal;
  cache: Cache;
  eventTarget: CustomEventTarget<DataFrameEvents>;
}): Promise<void> {
  checkSignal(signal);

  const offset = Math.max(
    cache.serial.end, // don't fetch known rows again
    Math.floor(start - paddingRows * cache.averageRowBytes) // fetch a bit before, to ensure we get a complete first row
  );
  let isFirstStep = true;
  const endCursor = Math.ceil(end + paddingRows * cache.averageRowBytes); // fetch a bit after, just in case the average is not accurate

  await parseCSV(cache.url, {
    delimiter: ",", // TODO(SL): auto detect - or pass from cache?
    newline: "\n", // TODO(SL): auto detect - or pass from cache?
    chunkSize: cache.chunkSize,
    offset, // TODO(SL): replace with start/end?
    step: (row, parser) => {
      if (signal?.aborted) {
        parser.abort();
        return;
      }

      // ignore the first row, because we cannot know if it's partial or complete
      if (isFirstStep) {
        isFirstStep = false;
        return;
      }

      // add the row to the cache
      if (addRowToCache({ cache, row })) {
        // send an event for the new row
        eventTarget.dispatchEvent(new CustomEvent("resolve"));
      }

      if (row.end >= endCursor) {
        // abort the parsing, we have enough rows for now
        parser.abort();
        return;
      }

      // the errors field is ignored
    },
  });
}

function isEmpty(data: string[]): boolean {
  return data.length <= 1 && data[0]?.trim() === "";
}

/**
 * Returns true if the row was added to the cache, false if it was already present or empty
 */
function addRowToCache({ cache, row }: { cache: Cache; row: CSVRow }): boolean {
  // TODO(SL): optimize
  const inserted = !isEmpty(row.data);
  const allRanges = [cache.serial, ...cache.random];

  if (allRanges.some((r) => row.start < r.end && row.end > r.start)) {
    // an overlap means the row is already in the cache. ignore it
    return false;
  }

  for (const [i, range] of allRanges.entries()) {
    if (row.end < range.start) {
      // create a new random range before this one
      const newRange = new CSVRange(row.start, row.bytes, []);
      if (inserted) {
        newRange.validRows.push(row);
        cache.cachedBytes += row.end - row.start;
      }
      // the range cannot be cache.serial because of the check above, let's assert it
      if (i < 1) {
        throw new Error(
          "Unexpected state: cannot insert before the serial range"
        );
      }
      cache.random.splice(i - 1, 0, newRange);
      return inserted;
    }
    if (row.end === range.start) {
      // expand this range at the beginning
      range.start = row.start;
      if (inserted) {
        range.validRows.unshift(row);
        cache.cachedBytes += row.bytes;
      }
      return inserted;
    }
    if (row.start === range.end) {
      // expand this range at the end
      range.bytes += row.bytes;
      if (inserted) {
        range.validRows.push(row);
        cache.cachedBytes += row.bytes;
      }
      // try to merge with the next range
      const nextRange = cache.random[i]; // equivalent to allRanges[i + 1]
      if (nextRange && range.end === nextRange.start) {
        range.bytes += nextRange.bytes;
        for (const r of nextRange.validRows) {
          range.validRows.push(r);
        }
        // remove the next range
        cache.random.splice(i, 1);
      }
      return inserted;
    }
  }
  // add a new range at the end
  const newRange = new CSVRange(row.start, row.bytes, []);
  if (inserted) {
    newRange.validRows.push(row);
    cache.cachedBytes += row.bytes;
  }
  cache.random.push(newRange);
  return inserted;
}
