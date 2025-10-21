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

export type Step = (
  inputs: {
    data: string[];
    meta: {
      cursor: number;
      delimiter: string;
      linebreak: Newline;
    };
  },
  parser: { abort: () => void }
) => void;

export interface Options {
  chunkSize: number;
  delimiter?: string;
  newline?: Newline;
  offset?: number;
  step: Step;
}

export interface Result {
  partial: boolean;
}

export async function parseCSV(url: string, options: Options): Promise<Result> {
  return await Promise.resolve({
    partial: true,
  });
}
