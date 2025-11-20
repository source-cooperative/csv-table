import { type DataFrame, HighTable } from 'hightable'
import { type ReactNode } from 'react'

import type { } from './dataframe.js'
import Loading from './Loading.js'

export interface PageProps {
  df?: DataFrame
  name?: string
  byteLength?: number
  setError: (e: unknown) => void
  iframe: boolean
}

/**
 * CSV viewer page
 * @param props - page properties
 * @param props.df - data frame to display
 * @param props.name - name of the file
 * @param props.byteLength - size of the file in bytes
 * @param props.setError - function to call on error
 * @param props.iframe - whether the page is displayed in an iframe
 * @returns Page component
 */
export default function Page({
  df,
  name,
  byteLength,
  setError,
  iframe = false,
}: PageProps): ReactNode {
  return (
    <>
      {iframe ? '' : <div className="top-header">{name}</div>}
      <div className="view-header">
        {byteLength !== undefined && (
          <span title={byteLength.toLocaleString() + ' bytes'}>
            {formatFileSize(byteLength)}
          </span>
        )}
        {df
          ? (
              <span>
                {df.numRows.toLocaleString()}
                {' '}
                row
                {df.numRows > 1 ? 's' : ''}
                {df.metadata?.isNumRowsEstimated ? ' (estimated)' : ''}
              </span>
            )
          : null}
      </div>
      {!df
        ? (
            <Loading />
          )
        : (
            <HighTable
              cacheKey={name}
              data={df}
              onError={setError}
              className="hightable"
            />
          )}
    </>
  )
}

/**
 * Returns the file size in human readable format.
 * @param {number} bytes file size in bytes
 * @returns {string} formatted file size string
 */
function formatFileSize(bytes: number): string {
  const sizes = ['b', 'kb', 'mb', 'gb', 'tb']
  if (bytes === 0) return '0 b'
  const i = Math.floor(Math.log2(bytes) / 10)
  if (i === 0) return `${bytes.toString()} b`
  const base = bytes / Math.pow(1024, i)
  const size = sizes[i]
  if (!size) {
    throw new Error('File size too large')
  }
  return `${base < 10 ? base.toFixed(1) : Math.round(base).toString()} ${size}`
}
