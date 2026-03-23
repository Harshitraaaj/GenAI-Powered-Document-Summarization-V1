// src/pages/EntitiesPage.js
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import toast from "react-hot-toast";
import { Cpu, Search } from "lucide-react";
import { useDocument } from "../context/DocumentContext";
import { extractEntities } from "../services/api";
import { PageHeader, MetricCard, EmptyState, Btn, Spinner } from "../components/Shared";

const TYPE_STYLE = {
  PERSON:       "bg-sky-glow text-sky border-sky/20",
  ORGANIZATION: "bg-jade-glow text-jade border-jade/20",
  TECHNOLOGY:   "bg-amber-glow text-amber border-amber/20",
  CONCEPT:      "bg-purple-900/20 text-purple-400 border-purple-400/20",
  LOCATION:     "bg-pink-900/20 text-pink-400 border-pink-400/20",
  EVENT:        "bg-orange-900/20 text-orange-400 border-orange-400/20",
  PRODUCT:      "bg-teal-900/20 text-teal-400 border-teal-400/20",
  DATE:         "bg-emerald-900/20 text-emerald-400 border-emerald-400/20",
  OTHER:        "bg-surface2 text-dim border-border",
};

const TypeBadge = ({ type }) => (
  <span className={`text-[10px] px-2 py-0.5 rounded-full border font-medium whitespace-nowrap
    ${TYPE_STYLE[type?.toUpperCase()] || TYPE_STYLE.OTHER}`}>
    {type}
  </span>
);

const EntitiesPage = () => {
  const { docId, entities, setEntities, completeStep } = useDocument();
  const navigate = useNavigate();
  const [loading, setLoading]   = useState(false);
  const [search, setSearch]     = useState("");
  const [typeFilter, setType]   = useState("ALL");

  const handleExtract = async () => {
    if (!docId) return;
    setLoading(true);
    const tid = toast.loading("Extracting entities...");
    try {
      const data = await extractEntities(docId);
      setEntities(data);
      completeStep("extracted");
      toast.success(`Extracted ${data.entity_count} entities`, { id: tid });
    } catch (err) {
      toast.error(err?.response?.data?.detail || "Extraction failed", { id: tid });
    } finally { setLoading(false); }
  };

  if (!docId) return (
    <EmptyState icon={Cpu} title="No document loaded" subtitle="Upload and summarize a document first"
      action={<Btn onClick={() => navigate("/")}>Go to Upload</Btn>} />
  );

  const allTypes = entities
    ? ["ALL", ...new Set(entities.entities?.map((e) => e.type?.toUpperCase()).filter(Boolean))]
    : ["ALL"];

  const filtered = entities?.entities?.filter((e) => {
    const ms = e.name?.toLowerCase().includes(search.toLowerCase()) ||
               e.context?.toLowerCase().includes(search.toLowerCase());
    const mt = typeFilter === "ALL" || e.type?.toUpperCase() === typeFilter;
    return ms && mt;
  }) || [];

  return (
    <div className="max-w-4xl animate-fadeUp">
      <PageHeader title="Entity Extraction" subtitle="Named entities extracted from the document" />

      {/* Extract CTA */}
      {!entities && (
        <div className="bg-surface border border-border rounded-xl p-6 mb-6">
          <p className="text-xs text-dim leading-relaxed mb-4">
            Extract named entities — people, organizations, technologies, concepts and more.
            Results include accuracy metrics across 3 quality dimensions.
          </p>
          <Btn onClick={handleExtract} loading={loading}>
            {loading ? "Extracting..." : "Extract Entities"}
          </Btn>
        </div>
      )}

      {loading && <Spinner text="Extracting entities from all chunks in parallel..." />}

      {entities && !loading && (
        <>
          {/* Accuracy metrics */}
          <div className="grid grid-cols-4 gap-3 mb-6">
            <MetricCard label="Total Entities"   value={entities.entity_count}                            unit="" color="blue" />
            <MetricCard label="Type Accuracy"    value={entities.accuracy_metrics?.type_accuracy} />
            <MetricCard label="Context Accuracy" value={entities.accuracy_metrics?.context_accuracy} />
            <MetricCard label="Overall Accuracy" value={entities.accuracy_metrics?.overall_accuracy}     color="green" />
          </div>

          {/* Controls */}
          <div className="flex gap-3 items-center mb-4 flex-wrap">
            <div className="flex items-center gap-2 bg-surface border border-border rounded-md px-3 py-2 flex-1 min-w-[200px]">
              <Search size={13} className="text-dim flex-shrink-0" />
              <input
                className="bg-transparent text-ink text-xs flex-1 placeholder-faint outline-none"
                placeholder="Search entities..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />
            </div>
            <div className="flex gap-1.5 flex-wrap">
              {allTypes.map((t) => (
                <button
                  key={t}
                  onClick={() => setType(t)}
                  className={`px-2.5 py-1 rounded-full border text-[10px] font-mono transition-all
                    ${typeFilter === t
                      ? "bg-amber text-black border-amber"
                      : "border-border text-dim hover:border-amber hover:text-amber"}`}
                >
                  {t}
                </button>
              ))}
            </div>
          </div>

          {/* Table */}
          <div className="bg-surface border border-border rounded-xl overflow-hidden mb-8">
            {/* Header */}
            <div className="grid grid-cols-[110px_1fr_2fr_120px] gap-3 px-4 py-2.5 bg-surface2 border-b border-border
              text-[10px] uppercase tracking-wider text-dim">
              <span>Type</span><span>Name</span><span>Context</span><span>Chunks</span>
            </div>
            {/* Rows */}
            <div className="max-h-[460px] overflow-y-auto">
              {filtered.map((e, i) => (
                <div key={i}
                  className="grid grid-cols-[110px_1fr_2fr_120px] gap-3 px-4 py-2.5 border-b border-border
                    last:border-0 hover:bg-surface2 transition-colors items-center">
                  <TypeBadge type={e.type} />
                  <span className="text-xs text-ink truncate">{e.name}</span>
                  <span className="text-[11px] text-dim truncate">{e.context}</span>
                  <div className="flex gap-1 flex-wrap">
                    {e.source_chunk_ids?.slice(0, 4).map((c) => (
                      <span key={c} className="text-[10px] bg-surface2 border border-border rounded-full px-1.5 py-0.5 text-faint">
                        #{c}
                      </span>
                    ))}
                    {e.source_chunk_ids?.length > 4 && (
                      <span className="text-[10px] text-faint">+{e.source_chunk_ids.length - 4}</span>
                    )}
                  </div>
                </div>
              ))}
            </div>
            <div className="px-4 py-2.5 border-t border-border text-[11px] text-faint">
              Showing {filtered.length} of {entities.entity_count} entities
            </div>
          </div>

          <div className="pt-6 border-t border-border">
            <Btn onClick={() => navigate("/graph")}>Build Knowledge Graph →</Btn>
          </div>
        </>
      )}
    </div>
  );
};

export default EntitiesPage;
