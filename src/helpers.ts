/**
 * Helper function to join class names
 * 
 */
export function cn(...names: (string | undefined | false)[]): string {
  return names.filter((n) => n).join(' ')
}

/**
 * Copy from hyparquet
 */

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

const bytesFormat = new Intl.NumberFormat('en-US', { 
   style: 'unit',
    unit: 'byte',
    unitDisplay: 'narrow',
    maximumFractionDigits: 0,
 })

export function formatBytes(bytes: number): string {
  return bytesFormat.format(bytes)
}
