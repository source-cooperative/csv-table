import { type FormEvent, type ReactNode, useCallback, useRef } from 'react'

interface Props {
  setUrl: (url: string) => void
}

const exampleUrl = 'https://huggingface.co/datasets/Codatta/MM-Food-100K/resolve/main/MM-Food-100K.csv'
/**
 * Welcome page with quick links to example CSV files.
 * @param props Component props
 * @param props.setUrl Function to set the CSV URL
 * @returns Welcome page React node
 */
export default function Welcome({ setUrl }: Props): ReactNode {
  const urlRef = useRef<HTMLInputElement>(null)
  const onSubmit = useCallback((e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    const value = urlRef.current?.value ?? ''
    const url = value === '' ? exampleUrl : value
    setUrl(url)
  }, [setUrl])

  return (
    <div id="welcome">
      <div>
        <h1>CSV viewer</h1>
        <form onSubmit={onSubmit}>
          <label htmlFor="url">Drag and drop 🫳 a CSV file (or url) to see your data. 👀</label>
          <div className="inputGroup">
            <input id="url" type="url" ref={urlRef} required={false} placeholder={exampleUrl} />
            <button>Load</button>
          </div>
        </form>
        <p>
          Example files:
        </p>
        <ul className="quick-links">
          <li>
            <a
              className="source"
              href="?url=https://data.source.coop/severo/csv-papaparse-test-files/verylongsample.csv"
            >
              <h2>PapaParse test files</h2>
              <p>Five CSV files used to test the PapaParse library.</p>
            </a>
          </li>
          <li>
            <a
              className="huggingface"
              href="?url=https://huggingface.co/datasets/Codatta/MM-Food-100K/resolve/main/MM-Food-100K.csv"
            >
              <h2>MM-Food-100K</h2>
              <p>100K food images dataset for computer vision tasks (26MB).</p>
            </a>
          </li>
        </ul>
      </div>
    </div>
  )
}
