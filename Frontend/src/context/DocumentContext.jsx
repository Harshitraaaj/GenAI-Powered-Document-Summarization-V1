// src/context/DocumentContext.js
import { createContext, useContext, useState } from "react";

const DocumentContext = createContext(null);

export const DocumentProvider = ({ children }) => {
  const [docId, setDocId]         = useState(null);
  const [summary, setSummary]     = useState(null);
  const [entities, setEntities]   = useState(null);
  const [graphData, setGraphData] = useState(null);
  const [factData, setFactData]   = useState(null);
  const [fileName, setFileName]   = useState(null);
  const [steps, setSteps]         = useState({
    summarized: false, extracted: false, graphBuilt: false, verified: false,
  });

  const completeStep = (step) => setSteps((p) => ({ ...p, [step]: true }));

  const reset = () => {
    setDocId(null); setSummary(null); setEntities(null);
    setGraphData(null); setFactData(null); setFileName(null);
    setSteps({ summarized: false, extracted: false, graphBuilt: false, verified: false });
  };

  return (
    <DocumentContext.Provider value={{
      docId, setDocId, summary, setSummary, entities, setEntities,
      graphData, setGraphData, factData, setFactData,
      fileName, setFileName, steps, completeStep, reset,
    }}>
      {children}
    </DocumentContext.Provider>
  );
};

export const useDocument = () => {
  const ctx = useContext(DocumentContext);
  if (!ctx) throw new Error("useDocument must be inside DocumentProvider");
  return ctx;
};
