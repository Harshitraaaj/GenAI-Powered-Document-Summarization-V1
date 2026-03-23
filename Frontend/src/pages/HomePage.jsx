// src/pages/HomePage.js
import { useCallback, useState } from "react";
import { useDropzone } from "react-dropzone";
import { useNavigate } from "react-router-dom";
import { Upload, FileText, Zap, Brain, Network, ShieldCheck, Search } from "lucide-react";
import toast from "react-hot-toast";
import { useDocument } from "../context/DocumentContext";
import { summarizeDocument } from "../services/api";
import { Spinner } from "../components/Shared";

const FEATURES = [
  { icon: FileText,    label: "Hierarchical Summary",  desc: "Chunk → Section → Executive" },
  { icon: Brain,       label: "Entity Extraction",     desc: "People, orgs, tech, concepts" },
  { icon: Network,     label: "Knowledge Graph",       desc: "Neo4j relationship mapping" },
  { icon: ShieldCheck, label: "Fact Verification",     desc: "Claims grounded against source" },
  { icon: Search,      label: "Semantic Search",       desc: "FAISS vector similarity" },
  { icon: Zap,         label: "Accuracy Metrics",      desc: "Coverage & factual accuracy" },
];

const HomePage = () => {
  const navigate = useNavigate();
  const { setDocId, setSummary, setFileName, completeStep } = useDocument();
  const [loading, setLoading] = useState(false);
  const [file, setFile] = useState(null);

  const onDrop = useCallback((accepted) => { if (accepted[0]) setFile(accepted[0]); }, []);
  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop, accept: { "application/pdf": [".pdf"] }, maxFiles: 1, disabled: loading,
  });

  const handleUpload = async () => {
    if (!file) return;
    setLoading(true);
    const tid = toast.loading("Summarizing document — this takes 60s...");
    try {
      const data = await summarizeDocument(file);
      setDocId(data.doc_id);
      setSummary(data);
      setFileName(file.name);
      completeStep("summarized");
      toast.success("Document summarized!", { id: tid });
      navigate("/summary");
    } catch (err) {
      toast.error(err?.response?.data?.detail || "Upload failed", { id: tid });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="animate-fadeUp">
      {/* Hero */}
      <div className="mb-10">
        <div className="uppercase tracking-[3px] text-amber mb-4">
          GenAI Document Intelligence
        </div>
        <h1 className="font-display text-4xl font-extrabold tracking-tight leading-tight mb-4 text-ink">
          Extract knowledge from 
          <span className="text-amber"> any research document</span>
        </h1>
        <p className="text-sm text-dim leading-relaxed">
          Upload a PDF — get hierarchical summaries, entity graphs,
          fact verification, and semantic search powered by LLMs and vector search.
        </p>
      </div>

      {/* Upload zone */}
      <div className="mb-10">
        <div
          {...getRootProps()}
          className={`border-2 border-dashed rounded-xl p-10 text-center cursor-pointer transition-all duration-200 mb-4
            ${isDragActive ? "border-amber bg-amber-glow" : ""}
            ${file && !isDragActive ? "border-jade bg-jade-glow border-solid" : ""}
            ${!file && !isDragActive ? "border-border2 bg-surface hover:border-amber hover:bg-amber-glow" : ""}`}
        >
          <input {...getInputProps()} />
          {loading ? (
            <Spinner text="Summarizing your document  ..." />
          ) : (
            <>
              <div className="flex justify-center mb-3 text-dim">
                <Upload size={26} />
              </div>
              {file ? (
                <div className="flex items-center justify-center gap-2 text-jade text-sm">
                  <FileText size={15} />
                  <span>{file.name}</span>
                  <span className="text-dim text-xs">({(file.size / 1024 / 1024).toFixed(1)} MB)</span>
                </div>
              ) : (
                <>
                  <p className="text-sm text-ink mb-1">
                    {isDragActive ? "Drop your PDF here" : "Drag & drop a PDF"}
                  </p>
                  <p className="text-xs text-dim">or click to browse</p>
                </>
              )}
            </>
          )}
        </div>

        <button
          onClick={handleUpload}
          disabled={!file || loading}
          className="w-full py-3 bg-amber text-black rounded-md text-sm font-medium font-mono
            hover:bg-amber/90 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
        >
          {loading ? "Processing..." : "Analyze Document →"}
        </button>
      </div>

      {/* Feature grid */}
      <div className="grid grid-cols-2 gap-2.5">
        {FEATURES.map(({ icon: Icon, label, desc }) => (
          <div key={label} className="flex items-start gap-3 p-3.5 bg-surface border border-border rounded-lg">
            <div className="w-7 h-7 bg-surface2 rounded-md grid place-items-center text-amber flex-shrink-0">
              <Icon size={14} />
            </div>
            <div>
              <div className="text-xs font-medium text-ink">{label}</div>
              <div className="text-[11px] text-dim mt-0.5">{desc}</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default HomePage;
