// import { sortableDataFrame } from "hightable";
import type { ReactNode } from "react";
import { useCallback, useReducer, useState } from "react";

import { type CSVDataFrame, csvDataFrame } from "./dataframe.js";
import { byteLengthFromUrl } from "./helpers.js";
import Dropzone from "./Dropzone.js";
import Layout from "./Layout.js";
import Page from "./Page.js";
import Welcome from "./Welcome.js";

interface State {
  url?: string
  name?: string
  byteLength?: number
  controller?: AbortController
  df?: CSVDataFrame
}

type Action = {
  type: 'setUrl',
  url: string,
  name?: string,
} | {
  type: "setByteLength",
  byteLength: number,
} | {
  type: "setController",
  controller: AbortController,
} | {
  type: "setDataFrame",
  df: CSVDataFrame,
}
function reducer(state: State, action: Action): State {
  switch (action.type) {
    case 'setUrl':
      state.controller?.abort();
      if (state.url) {
        // revoke obsolete object URL, if any (silently ignores error if not an object URL)
        URL.revokeObjectURL(state.url);
      }
      return { url: action.url, name: action.name ?? action.url }
    case 'setByteLength':
      return { ...state, byteLength: action.byteLength }
    case 'setController':
      return { ...state, controller: action.controller }
    case 'setDataFrame':
      return { ...state, df: action.df }
    default:
      throw new Error("Unknown action");
  }
}

export default function App(): ReactNode {
  const params = new URLSearchParams(location.search);
  const url = params.get("url") ?? undefined;
  const iframe = params.get("iframe") ? true : false;

  const [state, dispatch] = useReducer(reducer, {});
  const [error, setError] = useState<Error>();

  const setUnknownError = useCallback((e: unknown) => {
    setError(e instanceof Error ? e : new Error(String(e)));
  }, [setError]);

  const prepareDataFrame = useCallback(async function ({ url, byteLength }: { url: string, byteLength: number }) {
    const controller = new AbortController();
    dispatch({ type: 'setController', controller });
    const df = await csvDataFrame({ url, byteLength, signal: controller.signal });
    dispatch({ type: 'setDataFrame', df });
    // sortableDataFrame( ... // TODO(SL): enable sorting? (requires all the data - maybe on small data?)
  }, []);

  const setUrl = useCallback((url: string) => {
    dispatch({ type: 'setUrl', url });
    byteLengthFromUrl(url).then(byteLength => {
      dispatch({ type: 'setByteLength', byteLength });
      return prepareDataFrame({ url, byteLength })
    }).catch(setUnknownError);
  }, [setUnknownError, prepareDataFrame]);

  const onUrlDrop = useCallback((url: string) => {
    // Add url=url to query string
    const params = new URLSearchParams(location.search);
    params.set("url", url);
    history.pushState({}, "", `${location.pathname}?${params}`);
    setUrl(url);
  }, [setUrl]);

  const onFileDrop = useCallback((file: File) => {
    // Clear query string
    history.pushState({}, "", location.pathname);
    const url = URL.createObjectURL(file);
    dispatch({ type: 'setUrl', url, name: file.name });
    dispatch({ type: 'setByteLength', byteLength: file.size });
    prepareDataFrame({ url, byteLength: file.size }).catch(setUnknownError)
  }, [setUnknownError, prepareDataFrame]);

  if (url !== undefined && url !== state.url) {
    // if we have a url in the query string, it's not the same as the current one, load it
    setUrl(url);
  }
  return (
    <Layout error={error}>
      <Dropzone
        onError={(e) => {
          setError(e);
        }}
        onFileDrop={onFileDrop}
        onUrlDrop={onUrlDrop}
      >
        {state.url ? (
          <Page {...state} iframe={iframe} setError={setUnknownError} />
        ) : (
          <Welcome />
        )}
      </Dropzone>
    </Layout>
  );
}
