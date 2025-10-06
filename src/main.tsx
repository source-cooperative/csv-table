import "hightable/src/HighTable.css";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./styles/index.css";
import App from "./App.tsx";

const app = document.getElementById("app");
if (!app) throw new Error("missing app element");

createRoot(app).render(
  <StrictMode>
    <App />
  </StrictMode>
);
