// src/pages/QueryPage.js
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import toast from "react-hot-toast";
import { Search, ChevronRight } from "lucide-react";
import { useDocument } from "../context/DocumentContext";
import { semanticQuery } from "../services/api";
import { PageHeader, EmptyState, Btn, Spinner } from "../components/Shared";

const EXAMPLES = [
  "How does RAG combine parametric and non-parametric memory?",
  "What percentage of students delayed graduation due to COVID-19?",
  "What datasets were used to evaluate model performance?",
  "What are the main risks identified in this paper?",
  "How does DPR retrieve documents using dense passage retrieval?",
];

const QueryPage = () => {
  const { docId } = useDocument();
  const navigate = useNavigate();
  const [query, setQuery]     = useState("");
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleQuery = async () => {
    if (!query.trim() || !docId) return;
    setLoading(true);
    try {
      const data = await semanticQuery(docId, query);
      setResults(data);
    } catch { toast.error("Query failed"); }
    finally { setLoading(false); }
  };

  if (!docId) return (
    <EmptyState icon={Search} title="No document loaded" subtitle="Upload and summarize a document first"
      action={<Btn onClick={() => navigate("/")}>Go to Upload</Btn>} />
  );

  return (
    <div className="max-w-3xl animate-fadeUp">
      <PageHeader title="Semantic Query" subtitle="Search the document using natural language" />

      {/* Input */}
      <div className="mb-8">
        <div className="flex gap-2 items-start bg-surface border border-border rounded-xl p-4 mb-3
          focus-within:border-amber transition-colors">
          <Search size={15} className="text-dim mt-0.5 flex-shrink-0" />
          <textarea
            className="flex-1 bg-transparent text-ink text-sm outline-none resize-none placeholder-faint leading-relaxed"
            placeholder="Ask a specific question about the document..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleQuery(); } }}
            rows={2}
          />
        </div>
        <Btn onClick={handleQuery} loading={loading} disabled={!query.trim()}>
          Search Document
        </Btn>
      </div>

      {/* Examples */}
      {!results && (
        <div className="mb-8">
          <div className="text-[11px] uppercase tracking-widest text-dim mb-3">Try an example</div>
          <div className="space-y-1.5">
            {EXAMPLES.map((q) => (
              <button
                key={q}
                onClick={() => setQuery(q)}
                className="flex items-center gap-2 w-full px-3.5 py-2.5 bg-surface border border-border rounded-lg
                  text-xs text-dim text-left hover:border-amber hover:text-amber transition-all font-mono"
              >
                <ChevronRight size={11} className="flex-shrink-0" />
                {q}
              </button>
            ))}
          </div>
        </div>
      )}

      {loading && <Spinner text="Searching document..." />}

      {/* Results */}
      {results && !loading && (
        <div>
          <div className="flex items-center justify-between mb-4">
            <span className="text-xs text-dim">
              {results.results?.length} results for "{results.query}"
            </span>
            <button
              onClick={() => { setResults(null); setQuery(""); }}
              className="text-[11px] border border-border text-dim px-3 py-1 rounded-full
                hover:border-rose hover:text-rose transition-all font-mono"
            >
              Clear
            </button>
          </div>

          <div className="space-y-3">
            {results.results?.map((r, i) => (
              <div key={i} className="bg-surface border border-border rounded-xl overflow-hidden">
                {/* Result header */}
                <div className="flex items-center gap-3 px-4 py-2.5 bg-surface2 border-b border-border">
                  <span className="text-[10px] bg-amber-glow text-amber border border-amber/20 px-2 py-0.5 rounded-full">
                    #{i + 1}
                  </span>
                  <span className="text-[11px] text-dim">Chunk {r.chunk_id}</span>
                  <div className="ml-auto flex items-center gap-2 text-[11px] text-dim">
                    Score: {r.score?.toFixed(3)}
                    <div
                      className="h-1.5 bg-jade rounded-full"
                      style={{ width: `${Math.min(r.score * 150, 80)}px` }}
                    />
                  </div>
                </div>
                {/* Content */}
                <p className="px-4 py-3.5 text-xs text-ink leading-relaxed">{r.content}</p>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default QueryPage;
