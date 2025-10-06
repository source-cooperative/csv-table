import HighTable, { type DataFrame } from "hightable";
import { type ReactNode } from "react";

export interface PageProps {
  df: DataFrame;
  name: string;
  byteLength?: number;
  setError: (e: unknown) => void;
  iframe?: boolean;
}

/**
 * CSV viewer page
 * @param {Object} props
 * @returns {ReactNode}
 */
export default function Page({
  df,
  name,
  byteLength,
  setError,
  iframe = false,
}: PageProps): ReactNode {
  console.log(df);
  return (
    <>
      {iframe ? "" : <div className="top-header">{name}</div>}
      <div className="view-header">
        {byteLength !== undefined && (
          <span title={byteLength.toLocaleString() + " bytes"}>
            {formatFileSize(byteLength)}
          </span>
        )}
        <span>{df.numRows.toLocaleString()} rows</span>
      </div>
      <HighTable
        cacheKey={name}
        data={df}
        onError={setError}
        className="hightable"
      />
    </>
  );
}

/**
 * Returns the file size in human readable format.
 *
 * @param {number} bytes file size in bytes
 * @returns {string} formatted file size string
 */
function formatFileSize(bytes: number): string {
  const sizes = ["b", "kb", "mb", "gb", "tb"];
  if (bytes === 0) return "0 b";
  const i = Math.floor(Math.log2(bytes) / 10);
  if (i === 0) return `${bytes.toString()} b`;
  const base = bytes / Math.pow(1024, i);
  const size = sizes[i];
  if (!size) {
    throw new Error("File size too large");
  }
  return `${base < 10 ? base.toFixed(1) : Math.round(base).toString()} ${size}`;
}
