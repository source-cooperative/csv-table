import type { ReactNode } from "react";
import Page, { type PageProps } from "./Page.js";
import Welcome from "./Welcome.js";
import { useCallback, useEffect, useState } from "react";
import Dropzone from "./Dropzone.js";
import Layout from "./Layout.js";
import Loading from "./Loading.js";
// import { sortableDataFrame } from "hightable";

// import { asyncBufferFrom, byteLengthFromUrl } from './helpers.js';
import { byteLengthFromUrl } from './helpers.js';
import type { AsyncBufferFrom } from './helpers.js';
import { csvDataFrame } from "./csv.js";

export default function App(): ReactNode {
  const params = new URLSearchParams(location.search);
  const url = params.get("url") ?? undefined;
  const iframe = params.get("iframe") ? true : false;

  const [error, setError] = useState<Error>();
  const [pageProps, setPageProps] = useState<PageProps>();
  const [loading, setLoading] = useState<boolean>(!pageProps && url !== undefined);

  const setUnknownError = useCallback((e: unknown) => {
    setError(e instanceof Error ? e : new Error(String(e)));
    setLoading(false);
  }, []);

  const setAsyncBuffer = useCallback(
    async function (name: string, from: AsyncBufferFrom) {
      // const asyncBuffer = await asyncBufferFrom(from);
      // const df = csvDataFrame(asyncBuffer);
      // const df = sortableDataFrame(
      //   csvDataFrame(asyncBuffer)
      // ); // TODO(SL): enable sorting?
      const df = await csvDataFrame();
      setPageProps({
        df,
        name,
        byteLength: from.byteLength,
        setError: setUnknownError,
        iframe,
      });
      setLoading(false);
    },
    [setUnknownError, iframe]
  );

  useEffect(() => {
    if (!pageProps && url !== undefined) {
      byteLengthFromUrl(url)
        .then((byteLength) => setAsyncBuffer(url, { url, byteLength }))
        .catch(setUnknownError);
    }
  }, [url, pageProps, setUnknownError, setAsyncBuffer]);

  const onUrlDrop = useCallback(
    (url: string) => {
      setLoading(true);
      // Add url=url to query string
      const params = new URLSearchParams(location.search);
      params.set("url", url);
      history.pushState({}, "", `${location.pathname}?${params}`);
      byteLengthFromUrl(url)
        .then((byteLength) => setAsyncBuffer(url, { url, byteLength }))
        .catch(setUnknownError);
    },
    [setUnknownError, setAsyncBuffer]
  );

  function onFileDrop(file: File) {
    setLoading(true);
    // Clear query string
    history.pushState({}, "", location.pathname);
    setAsyncBuffer(file.name, { file, byteLength: file.size }).catch(
      setUnknownError
    );
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
        {loading ? (
          <Loading />
        ) : pageProps ? (
          <Page {...pageProps} />
        ) : (
          <Welcome />
        )}
      </Dropzone>
    </Layout>
  );
}
