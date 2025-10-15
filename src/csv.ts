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
import Papa from "@severo_tests/papaparse";
import { formatBytes } from "./helpers.js";

interface Metadata {
  isPartial: boolean;
  cachedBytes: number;
}
export type CSVDataFrame = DataFrame<Metadata>;

const defaultChunkSize = 50 * 1024; // 50 KB, same as Papaparse default
const defaultMaxCachedBytes = 20 * 1024 * 1024; // 20 MB

interface Params {
  url: string;
  byteLength: number; // total byte length of the file
  chunkSize?: number; // download chunk size
  maxCachedBytes?: number; // max number of bytes to keep in cache before evicting old rows
  signal?: AbortSignal; // to abort the DataFrame creation and any ongoing fetches
}

// rows are indexed by their first byte position. Includes empty row and the header
interface ParsedRow {
  start: number; // byte position of the start of the row
  end: number; // byte position of the end of the row, including the delimiters and the following linebreak if any (exclusive)
  data: string[]; // raw string values, as parsed by Papaparse (no eviction yet - we could handle them with "undefined" cells)
}

// ranges are sorted. We use binary search to find the missing ranges, and then merge them if needed
interface ParsedRange {
  start: number; // byte position of the start of the range (excludes the ignored bytes if the range starts in the middle of a row)
  end: number; // byte position of the end of the range (exclusive)
  validRows: ParsedRow[]; // sorted array of the range rows, filtering out the empty rows and the header if any
}

interface Cache {
  header: CSVHeader;
  serial: ParsedRange;
  random: ParsedRange[];
  cachedBytes: number; // total number of bytes cached (for statistics)
  chunkSize: number; // chunk size used for fetching
  url: string;
  averageRowBytes: number; // average number of bytes per row
}

interface CSVHeader extends ParsedRow {
  delimiter: string;
  newline: Exclude<Papa.ParseConfig<string[]>["newline"], undefined>;
  bytes: number; // number of bytes used by the header row, including the delimiters and the following linebreak if any
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
  let header = undefined as CSVHeader | undefined;
  let cursor = 0;
  let cachedBytes = 0;

  // Fetch the first chunk (stop at 80% of the chunk size, to avoid doing another fetch, as we have no way to limit to one chunk in Papaparse)
  // TODO(SL): should we return the dataframe after parsing one row, and then keep parsing the chunk, but triggering updates?)
  const firstParsedRange: ParsedRange = {
    start: cursor,
    end: cursor,
    validRows: [],
  };
  const isPartial = await new Promise<boolean>((resolve, reject) => {
    Papa.parse<string[]>(url, {
      download: true,
      chunkSize,
      header: false,
      worker: false, // don't use the worker! because it does not provide the cursor at a line level!
      skipEmptyLines: false, // to be able to compute the byte ranges. Beware, it requires post processing (see result.rows.at(-1), for example, when fetching all the rows)
      dynamicTyping: false, // keep strings, and let the user convert them if needed
      step: ({ data, meta }, parser) => {
        const start = cursor;
        cursor = meta.cursor;
        const end = cursor;
        if (
          cursor >= 0.8 * chunkSize ||
          firstParsedRange.validRows.length >= 100
        ) {
          // abort the parsing, we have enough rows for now
          parser.abort();
          return;
        }

        const parsedRow = { start, end, data };
        // parsedRowIndex.set(start, parsedRow); // TODO(SL): remove?
        // for the statistics:
        cachedBytes += parsedRow.end - parsedRow.start;

        firstParsedRange.end = end;

        if (isEmpty(data)) {
          // empty row, ignore
          return;
        }
        if (header === undefined) {
          // first non-empty row: header
          header = {
            ...parsedRow,
            delimiter: meta.delimiter,
            newline: getNewline(meta.linebreak),
            bytes: parsedRow.end - parsedRow.start,
          };
        } else {
          if (meta.delimiter !== header.delimiter) {
            reject(
              new Error(
                `Delimiter changed from ${header.delimiter} to ${meta.delimiter}`
              )
            );
          }
          if (meta.linebreak !== header.newline) {
            reject(
              new Error(
                `Linebreak changed from ${header.newline} to ${meta.linebreak}`
              )
            );
          }
          // valid row: add it to the range
          firstParsedRange.validRows.push(parsedRow);
        }
        // the errors field is ignored
      },
      complete: ({ meta }) => {
        const isPartial = meta.aborted;
        resolve(isPartial);
      },
    });
  });
  if (header === undefined) {
    throw new Error("No header row found in the CSV file");
  }

  const averageRowBytes = getAverageRowBytes({
    serial: firstParsedRange,
    header,
    random: [],
  });

  const cache: Cache = {
    header,
    serial: firstParsedRange,
    random: [],
    cachedBytes,
    chunkSize,
    url,
    averageRowBytes,
  };

  const numRows =
    isPartial && averageRowBytes
      ? Math.floor(byteLength / averageRowBytes) // see https://github.com/hyparam/hightable/issues/298
      : firstParsedRange.validRows.length;

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
      const value = parsedRow.data[columnIndex]; // TODO(SL): we could convert to a type, here or in the cache
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

    // await Promise.resolve(); // ensure async

    if (rowEnd < cache.serial.validRows.length) {
      // all rows are in the serial range
      return;
    }
    if (rowStart < cache.serial.validRows.length) {
      // ignore the rows already cached
      rowStart = cache.serial.validRows.length;
    }

    const estimatedStart = Math.floor(
      cache.header.bytes + rowStart * cache.averageRowBytes
    );
    const estimatedEnd = Math.min(
      byteLength,
      Math.ceil(cache.header.bytes + rowEnd * cache.averageRowBytes)
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

    console.debug({
      rowStart,
      rowEnd,
      estimatedStart,
      estimatedEnd,
      missingRanges,
      cache,
    });

    // fetch each missing range and fill the cache

    await Promise.all(
      missingRanges.map(({ start, end }) =>
        fetchRange({ start, end, signal, cache, eventTarget })
      )
      // TODO(SL): check the signal?
    ).finally(() => {
      // TODO(SL): Update the average size of a row?
      // For now, we keep it constant, to provide stability - otherwise empty rows appear after the update
      // cache.averageRowBytes = getAverageRowBytes(cache);
    });

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
  | (ParsedRow & {
      type: "serial" | "random";
    })
  | undefined {
  // TODO(SL): optimize (cache the row numbers?)
  const serialParsedRow = cache.serial.validRows[row];
  if (serialParsedRow) {
    return {
      type: "serial",
      ...serialParsedRow,
    };
  }
  const estimatedStart =
    cache.header.bytes +
    cache.serial.end +
    (row - cache.serial.validRows.length) * cache.averageRowBytes;
  // find the range containing this row
  const range = cache.random.find(
    (r) => estimatedStart >= r.start && estimatedStart < r.end
  );
  if (!range) {
    return; // not found
  }
  // estimate the row index of the first row in the range
  const firstRowIndex = Math.round(
    // is .round() better than .floor() or .ceil()?
    (range.start - cache.header.bytes) / cache.averageRowBytes
  );
  // get the row in the range. This way, we ensure that calls to findParsedRow() with increasing row numbers
  // will return rows in the same order, without holes or duplicates, even if the averageRowBytes is not accurate.
  const parsedRow = range.validRows[row - firstRowIndex];
  if (!parsedRow) {
    return; // not found
  }
  return {
    type: "random",
    ...parsedRow,
  };
}

function getAverageRowBytes(
  cache: Pick<Cache, "serial" | "header" | "random">
): number {
  let numRows = cache.serial.validRows.length;
  let numBytes = cache.serial.end - cache.serial.start - cache.header.bytes;

  for (const range of cache.random) {
    numRows += range.validRows.length;
    numBytes += range.end - range.start;
  }
  if (numRows === 0 || numBytes === 0) {
    throw new Error("No data row found in the CSV file");
  }
  return numBytes / numRows;
}

function getNewline(
  linebreak: string
): Exclude<Papa.ParseConfig<string[]>["newline"], undefined> {
  switch (linebreak) {
    case "\r\n":
    case "\n":
    case "\r":
      return linebreak;
    default:
      throw new Error(`Unsupported linebreak: ${linebreak}`); // should not happen
  }
}

function fetchRange({
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

  let cursor = start;
  let isFirstStep = true;

  return new Promise<void>((resolve, reject) => {
    Papa.parse<string[]>(cache.url, {
      download: true,
      header: false,
      worker: false, // don't use the worker! because it does not provide the cursor at a line level!
      skipEmptyLines: false, // to be able to compute the byte ranges. Beware, it requires post processing (see result.rows.at(-1), for example, when fetching all the rows)
      dynamicTyping: false, // keep strings, and let the user convert them if needed
      delimiter: cache.header.delimiter,
      newline: cache.header.newline,
      chunkSize: cache.chunkSize,
      firstChunkOffset: start, // custom option, only available in the modified Papaparse @severo_tests/papaparse
      step: ({ data, meta }, parser) => {
        if (signal?.aborted) {
          parser.abort();
          return;
        }

        const parsedRow = { start: cursor, end: start + meta.cursor, data };
        cursor = start + meta.cursor;

        if (meta.delimiter !== cache.header.delimiter) {
          reject(
            new Error(
              `Delimiter changed from ${cache.header.delimiter} to ${meta.delimiter}`
            )
          );
        }
        if (meta.linebreak !== cache.header.newline) {
          reject(
            new Error(
              `Linebreak changed from ${cache.header.newline} to ${meta.linebreak}`
            )
          );
        }

        // add the row to the cache
        if (addParsedRowToCache({ cache, parsedRow, isFirstStep })) {
          // send an event for the new row
          eventTarget.dispatchEvent(new CustomEvent("resolve"));
        }

        if (cursor >= end) {
          // abort the parsing, we have enough rows for now
          parser.abort();
          return;
        }

        isFirstStep = false;
      },
      complete: () => {
        resolve();
      },
    });
  });
}

function isEmpty(data: string[]): boolean {
  return data.length <= 1 && data[0]?.trim() === "";
}

/**
 * Returns true if the row was added to the cache, false if it was already present or empty
 */
function addParsedRowToCache({
  cache,
  parsedRow,
  isFirstStep,
}: {
  cache: Cache;
  parsedRow: ParsedRow;
  isFirstStep: boolean; // to handle the case where we start in the middle of a row
}): boolean {
  if (isFirstStep && parsedRow.data.length < cache.header.data.length) {
    // the first parsed row is partial, we ignore it, it must be part of the previous row
    return false;
  }

  // TODO(SL): optimize
  const inserted = !isEmpty(parsedRow.data);
  const allRanges = [cache.serial, ...cache.random];

  if (
    allRanges.some((r) => parsedRow.start < r.end && parsedRow.end > r.start)
  ) {
    // an overlap means the row is already in the cache. ignore it
    return false;
  }

  for (const [i, range] of allRanges.entries()) {
    if (parsedRow.end < range.start) {
      // create a new random range before this one
      const newRange: ParsedRange = {
        start: parsedRow.start,
        end: parsedRow.end,
        validRows: [],
      };
      if (inserted) {
        newRange.validRows.push(parsedRow);
        cache.cachedBytes += parsedRow.end - parsedRow.start;
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
    if (parsedRow.end === range.start) {
      // expand this range at the beginning
      range.start = parsedRow.start;
      if (inserted) {
        range.validRows.unshift(parsedRow);
        cache.cachedBytes += parsedRow.end - parsedRow.start;
      }
      return inserted;
    }
    if (parsedRow.start === range.end) {
      // expand this range at the end
      range.end = parsedRow.end;
      if (inserted) {
        range.validRows.push(parsedRow);
        cache.cachedBytes += parsedRow.end - parsedRow.start;
      }
      // try to merge with the next range
      const nextRange = cache.random[i]; // equivalent to allRanges[i + 1]
      if (nextRange && range.end === nextRange.start) {
        range.end = nextRange.end;
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
  const newRange: ParsedRange = {
    start: parsedRow.start,
    end: parsedRow.end,
    validRows: [],
  };
  if (inserted) {
    newRange.validRows.push(parsedRow);
    cache.cachedBytes += parsedRow.end - parsedRow.start;
  }
  cache.random.push(newRange);
  return inserted;
}
