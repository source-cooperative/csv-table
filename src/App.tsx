// import { sortableDataFrame } from "hightable";
import type { DataFrame } from 'hightable'
import type { ReactNode } from 'react'
import { useCallback, useReducer, useState } from 'react'

import { csvDataFrame } from './dataframe.js'
import Dropzone from './Dropzone.js'
import { byteLengthFromUrl } from './helpers.js'
import Layout from './Layout.js'
import Page from './Page.js'
import Welcome from './Welcome.js'

interface State {
  url?: string
  name?: string
  byteLength?: number
  df?: DataFrame
}

type Action = {
  type: 'setUrl'
  url: string
  name?: string
} | {
  type: 'setByteLength'
  byteLength: number
} | {
  type: 'setDataFrame'
  df: DataFrame
}
/**
 * Reducer function for managing state
 * @param state - The current state
 * @param action - The action to perform
 * @returns The new state
 */
function reducer(state: State, action: Action): State {
  switch (action.type) {
    case 'setUrl':
      if (state.url) {
        // revoke obsolete object URL, if any (silently ignores error if not an object URL)
        URL.revokeObjectURL(state.url)
      }
      return { url: action.url, name: action.name ?? action.url }
    case 'setByteLength':
      return { ...state, byteLength: action.byteLength }
    case 'setDataFrame':
      return { ...state, df: action.df }
    default:
      throw new Error('Unknown action')
  }
}

/**
 * App component
 * @returns App component
 */
export default function App(): ReactNode {
  const params = new URLSearchParams(location.search)
  const url = params.get('url') ?? undefined
  const iframe = params.get('iframe') ? true : false

  const [state, dispatch] = useReducer(reducer, {})
  const [error, setError] = useState<Error>()

  const setUnknownError = useCallback((e: unknown) => {
    setError(e instanceof Error ? e : new Error(String(e)))
  }, [setError])

  const prepareDataFrame = useCallback(async function ({ url, byteLength }: { url: string, byteLength: number }) {
    const df = await csvDataFrame({ url, byteLength })
    dispatch({ type: 'setDataFrame', df })
  }, [])

  const setUrl = useCallback((url: string) => {
    dispatch({ type: 'setUrl', url })
    byteLengthFromUrl(url).then((byteLength) => {
      dispatch({ type: 'setByteLength', byteLength })
      return prepareDataFrame({ url, byteLength })
    }).catch(setUnknownError)
  }, [setUnknownError, prepareDataFrame])

  const onUrlDrop = useCallback((url: string) => {
    // Add url=url to query string
    const params = new URLSearchParams(location.search)
    params.set('url', url)
    history.pushState({}, '', `${location.pathname}?${params}`)
    setUrl(url)
  }, [setUrl])

  const onFileDrop = useCallback((file: File) => {
    // Clear query string
    history.pushState({}, '', location.pathname)
    const url = URL.createObjectURL(file)
    dispatch({ type: 'setUrl', url, name: file.name })
    dispatch({ type: 'setByteLength', byteLength: file.size })
    prepareDataFrame({ url, byteLength: file.size }).catch(setUnknownError)
  }, [setUnknownError, prepareDataFrame])

  if (url !== undefined && url !== state.url) {
    // if we have a url in the query string, it's not the same as the current one, load it
    setUrl(url)
  }
  return (
    <Layout error={error}>
      <Dropzone
        onError={(e) => {
          setError(e)
        }}
        onFileDrop={onFileDrop}
        onUrlDrop={onUrlDrop}
      >
        {state.url
          ? (
              <Page {...state} iframe={iframe} setError={setUnknownError} />
            )
          : (
              <Welcome />
            )}
      </Dropzone>
    </Layout>
  )
}
