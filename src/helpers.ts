/**
 * Helper function to join class names
 * @param names - class names to join
 * @returns Joined class names
 */
export function cn(...names: (string | undefined | false)[]): string {
  return names.filter(n => n).join(' ')
}

/**
 * Copy from hyparquet
 */

/**
 * Get the byte length of a URL using a HEAD request.
 * If requestInit is provided, it will be passed to fetch.
 * @param url - The URL to fetch
 * @param requestInit - Fetch options
 * @param customFetch - Fetch function to use
 * @returns The byte length of the URL
 */
export async function byteLengthFromUrl(url: string, requestInit?: RequestInit, customFetch?: typeof globalThis.fetch): Promise<number> {
  const fetch = customFetch ?? globalThis.fetch
  return await fetch(url, { ...requestInit, method: 'HEAD' })
    .then((res) => {
      if (!res.ok) throw new Error(`fetch head failed ${res.status.toString()}`)
      const length = res.headers.get('Content-Length')
      if (!length) throw new Error('missing content length')
      return parseInt(length)
    })
}

/**
 * Throws if the provided value is not a non-negative integer.
 * @param value The desired value.
 * @returns The validated value: a non-negative integer.
 */
export function checkNonNegativeInteger(value: number): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error('Value is not a non-negative integer')
  }
  return value
}

/**
 * Throws if the provided value is not an integer.
 * @param value The desired value.
 * @returns The validated value: an integer.
 */
export function checkInteger(value: number): number {
  if (!Number.isInteger(value)) {
    throw new Error('Value is not an integer')
  }
  return value
}
