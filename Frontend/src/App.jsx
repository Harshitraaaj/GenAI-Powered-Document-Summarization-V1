// src/App.js
import { Toaster } from "react-hot-toast";
import { DocumentProvider } from "./context/DocumentContext";
import AppRouter from "./router/AppRouter";
import "./index.css";

const App = () => (
  <DocumentProvider>
    <AppRouter />
    <Toaster
      position="bottom-right"
      toastOptions={{
        style: {
          background: "#161616",
          color: "#e8e4dc",
          border: "1px solid #272727",
          fontFamily: "'DM Mono', monospace",
          fontSize: "12px",
          borderRadius: "8px",
        },
      }}
    />
  </DocumentProvider>
);

export default App;
