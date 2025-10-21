/**
 * CSV Parser
 *
 * The input is a remote CSV file URL.
 * The parser uses range requests to fetch only the needed parts of the file.
 * No use of workers for now.
 * Only decodes to arrays of UTF8 strings.
 * Auto detects the delimiter, line endings, and quote characters (code from PapaParse).
 */

export type Newline = "\n" | "\r\n" | "\r";

export class CSVRow {
  // To fetch the row bytes: 'Range' header: `bytes=${start}-${start + bytes - 1}`

  data: string[];
  start: number; // first byte of the row
  bytes: number; // number of bytes of the row. Includes the delimiters, quotes, spaces and final line ending, if any.
  //   delimiter: string;
  //   newline: Newline | undefined;

  constructor(data: string[], start: number, bytes: number) {
    this.data = data;
    this.start = start;
    this.bytes = bytes;
  }

  // first byte after the row (redundant: start + bytes)
  get end(): number {
    return this.start + this.bytes;
  }
}

// Note that the "start" field in CSVRow takes the "offset" into account, if any. It's the byte position in the file.
export type Step = (row: CSVRow, parser: { abort: () => void }) => void;

export interface Options {
  chunkSize: number;
  delimiter: string; // TODO(SL): optional, auto detect if not provided
  newline: Newline; // TODO(SL): optional, auto detect if not provided
  offset?: number;
  signal?: AbortSignal;
  step: Step;
}

export interface Result {
  partial: boolean; // whether the CSV was only partially parsed (interrupted or aborted)
}

export async function parseCSV(url: string, options: Options): Promise<Result> {
  return await Promise.resolve({
    partial: true,
  });
}

// function getNewline(linebreak: string): Newline {
//   switch (linebreak) {
//     case "\r\n":
//     case "\n":
//     case "\r":
//       return linebreak;
//     default:
//       throw new Error(`Unsupported linebreak: ${linebreak}`); // should not happen
//   }
// }
