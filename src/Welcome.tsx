import { type ReactNode } from "react";

export default function Welcome(): ReactNode {
  return (
    <div id="welcome">
      <div>
        <h1>CSV viewer</h1>
        <p>
          Drag and drop 🫳 a CSV file (or url) to see your data, or
          click an example:
        </p>
        <ul className="quick-links">
          <li>
            <a
              className="source"
              href="?url=https://data.source.coop/severo/csv-papaparse-test-files/sample.csv"
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
          <li>
            <a
              className="github"
              href="?url=https://raw.githubusercontent.com/HPI-Information-Systems/Pollock/main/polluted_files/clean/file_double_trailing_newline.csv"
            >
              <h2>Pollock benchmark</h2>
              <p>A CSV from the Pollock data loading benchmark.</p>
            </a>
          </li>
        </ul>
      </div>
    </div>
  );
}
