import type {
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

interface RowEntry {
  data: (ResolvedValue<string> | undefined)[]; // TODO(SL): convert from strings to typed values?
  bytesRead: number;
  firstByte: number;
}

interface CSVHeader {
  columnNames: string[];
  delimiter: string;
  linebreak: string;
  bytesRead: number;
  firstByte: number;
}

interface Cache {
  header: CSVHeader;
  rows: Map<number, RowEntry>; // row index -> row data
  estimatedNumRows: number; // based on average row length
  firstChunkOffset: number; // byte offset of the first chunk (0 for the first fetch, >0 for subsequent fetches)
  cachedBytes: number; // total number of bytes cached
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

  const rows = new Map<number, RowEntry>(); // row index -> row data,
  // type assertion is needed because Typescript cannot see if variable is updated in the Papa.parse step callback
  let header = undefined as CSVHeader | undefined;
  let cursor = 0;
  let rowIndex = 0;
  let cachedBytes = 0;

  // Fetch the first chunk (stop at 80% of the chunk size, to avoid doing another fetch, as we have no way to limit to one chunk in Papaparse)
  // TODO(SL): should we return the dataframe after parsing one row, and then keep parsing the chunk, but triggering updates?)
  const isPartial = await new Promise<boolean>((resolve, reject) => {
    Papa.parse<string[]>(url, {
      download: true,
      step: ({ data, meta }, parser) => {
        const bytesRead = meta.cursor - cursor;
        const firstByte = cursor;
        cursor = meta.cursor;
        if (cursor >= 0.8 * chunkSize || rowIndex >= 100) {
          // abort the parsing, we have enough rows for now
          parser.abort();
          return;
        }

        if (data.length <= 1 && data[0]?.trim() === "") {
          // empty row, ignore
          return;
        }
        if (header === undefined) {
          // first non-empty row: header
          header = {
            columnNames: [...data],
            delimiter: meta.delimiter,
            linebreak: meta.linebreak,
            firstByte,
            bytesRead,
          };
        } else {
          if (meta.delimiter !== header.delimiter) {
            reject(
              new Error(
                `Delimiter changed from ${header.delimiter} to ${meta.delimiter}`
              )
            );
            return;
          }
          if (meta.linebreak !== header.linebreak) {
            reject(
              new Error(
                `Linebreak changed from ${header.linebreak} to ${meta.linebreak}`
              )
            );
            return;
          }
          const rowEntry: RowEntry = {
            data: header.columnNames.map((_, i) => {
              return data[i] === undefined ? undefined : { value: data[i] }; // TODO(SL): convert to typed value?
            }),
            bytesRead,
            firstByte,
          };
          rows.set(rowIndex, rowEntry); // we don't evict in the first chunk pass, as the max cache size is greater than the chunk size
          cachedBytes += bytesRead;
          rowIndex++;
        }
        // the errors field is ignored
      },
      complete: ({ meta }) => {
        const isPartial = meta.aborted;
        resolve(isPartial);
      },
      chunkSize,
      header: false,
      worker: false, // don't use the worker! because it does not provide the cursor at a line level!
      skipEmptyLines: false, // to be able to compute the byte ranges. Beware, it requires post processing (see result.rows.at(-1), for example, when fetching all the rows)
      dynamicTyping: false, // keep strings, and let the user convert them if needed
    });
  });
  if (header === undefined) {
    throw new Error("No header row found in the CSV file");
  }

  const cache: Cache = {
    header,
    rows,
    firstChunkOffset: cursor,
    cachedBytes,
    estimatedNumRows: isPartial
      ? Math.floor((rows.size * byteLength) / cursor)
      : rows.size, // see https://github.com/hyparam/hightable/issues/298
  };
  console.log(cache);

  const numRows = cache.estimatedNumRows; // Update when we fetch more rows?
  const columnDescriptors: DataFrame["columnDescriptors"] =
    header.columnNames.map((name) => ({ name }));
  const metadata: Metadata = {
    isPartial,
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
      data: { numRows, columnDescriptors },
    });
    const rowEntry = cache.rows.get(row);
    if (rowEntry) {
      const colIndex = columnDescriptors.findIndex((cd) => cd.name === column);
      if (colIndex === -1) {
        // should not happen because of the validation above
        throw new Error(`Column not found: ${column}`);
      }
      return rowEntry.data[colIndex];
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
      data: { numRows, columnDescriptors },
    });
    if (cache.rows.has(row)) {
      return { value: row };
    }
    return undefined;
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
      data: { numRows, columnDescriptors },
    });
    await Promise.resolve(); // to make the function async for now

    // TODO(SL): implement fetching more rows, updating the cache, evicting old rows if needed
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
