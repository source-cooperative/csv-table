/**
 * Copy from hyperparam
 */

// Serializable constructors for AsyncBuffers
interface AsyncBufferFromFile {
  file: File
  byteLength: number
}
interface AsyncBufferFromUrl {
  url: string
  byteLength: number
  requestInit?: RequestInit
}
export type AsyncBufferFrom = AsyncBufferFromFile | AsyncBufferFromUrl

/**
 * Helper function to join class names
 * 
 */
export function cn(...names: (string | undefined | false)[]): string {
  return names.filter((n) => n).join(' ')
}

export function asyncBufferFrom(from: AsyncBufferFrom): Promise<AsyncBuffer> {
  if ('url' in from) {
    return asyncBufferFromUrl(from)
  } else {
    return from.file.arrayBuffer()
  }
}

/**
 * Copy from hyparquet
 */

/**
 * File-like object that can read slices of a file asynchronously.
 */
export interface AsyncBuffer {
  byteLength: number
  slice(start: number, end?: number): Awaitable<ArrayBuffer>
}
export type Awaitable<T> = T | Promise<T>

/**
 * Get the byte length of a URL using a HEAD request.
 * If requestInit is provided, it will be passed to fetch.
 * 
 * @param {string} url
 * @param {RequestInit} [requestInit] fetch options
 * @param {typeof globalThis.fetch} [customFetch] fetch function to use
 * @returns {Promise<number>}
 */
export async function byteLengthFromUrl(url: string, requestInit?: RequestInit, customFetch?: typeof globalThis.fetch): Promise<number> {
  const fetch = customFetch ?? globalThis.fetch
  return await fetch(url, { ...requestInit, method: 'HEAD' })
    .then(res => {
      if (!res.ok) throw new Error(`fetch head failed ${res.status.toString()}`)
      const length = res.headers.get('Content-Length')
      if (!length) throw new Error('missing content length')
      return parseInt(length)
    })
}

/**
 * Construct an AsyncBuffer for a URL.
 * If byteLength is not provided, will make a HEAD request to get the file size.
 * If fetch is provided, it will be used instead of the global fetch.
 * If requestInit is provided, it will be passed to fetch.
 *
 * @param {object} options
 * @param {string} options.url
 * @param {number} [options.byteLength]
 * @param {typeof globalThis.fetch} [options.fetch] fetch function to use
 * @param {RequestInit} [options.requestInit]
 * @returns {Promise<AsyncBuffer>}
 */
export async function asyncBufferFromUrl({ url, byteLength, requestInit, fetch: customFetch }: {
    url: string, byteLength?: number, requestInit?: RequestInit, fetch?: typeof globalThis.fetch
}): Promise<AsyncBuffer> {
  if (!url) throw new Error('missing url')
  const fetch = customFetch ?? globalThis.fetch
  // byte length from HEAD request
  byteLength ??= await byteLengthFromUrl(url, requestInit, fetch)

  /**
   * A promise for the whole buffer, if range requests are not supported.
   */
  let buffer: Promise<ArrayBuffer>|undefined = undefined
  const init = requestInit ?? {}

  return {
    byteLength,
    async slice(start, end) {
      if (buffer) {
        return buffer.then(buffer => buffer.slice(start, end))
      }

      const headers = new Headers(init.headers)
      const endStr = end === undefined ? '' : end - 1
      headers.set('Range', `bytes=${start.toString()}-${endStr.toString()}`)

      const res = await fetch(url, { ...init, headers })
      if (!res.ok || !res.body) throw new Error(`fetch failed ${res.status.toString()}`)

      if (res.status === 200) {
        // Endpoint does not support range requests and returned the whole object
        buffer = res.arrayBuffer()
        return buffer.then(buffer => buffer.slice(start, end))
      } else if (res.status === 206) {
        // The endpoint supports range requests and sent us the requested range
        return res.arrayBuffer()
      } else {
        throw new Error(`fetch received unexpected status code ${res.status.toString()}`)
      }
    },
  }
}
