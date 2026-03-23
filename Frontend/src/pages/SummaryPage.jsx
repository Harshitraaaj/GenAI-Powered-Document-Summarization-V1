// src/pages/SummaryPage.jsx
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { FileText, ChevronDown, ChevronUp } from "lucide-react";
import { useDocument } from "../context/DocumentContext";
import { PageHeader, MetricCard, StatusBadge, EmptyState, Btn } from "../components/Shared";

const SectionCard = ({ section }) => {
  const [open, setOpen] = useState(false);
  return (
    <div className="bg-surface border border-border rounded-lg overflow-hidden">
      <button
        className="flex items-center gap-3 w-full px-4 py-3 text-left hover:bg-surface2 transition-colors"
        onClick={() => setOpen(!open)}
      >
        <span className="text-[10px] bg-amber-glow text-amber border border-amber/20 w-6 h-5 rounded-md flex items-center justify-center font-mono flex-shrink-0 tabular-nums">
          {String(section.section_id).padStart(2, '0')}
        </span>
        <span className="flex-1 text-xs text-dim truncate">{section.tldr}</span>
        {open
          ? <ChevronUp size={13} className="text-faint flex-shrink-0" />
          : <ChevronDown size={13} className="text-faint flex-shrink-0" />}
      </button>

      {open && (
        <div className="px-4 pb-4 border-t border-border">
          <p className="text-xs text-ink leading-relaxed mt-3 mb-4">{section.summary}</p>
          <div className="grid grid-cols-3 gap-4">
            {section.key_points?.length > 0 && (
              <div>
                <div className="text-[10px] uppercase tracking-wider text-dim mb-2">Key Points</div>
                <ul className="space-y-1.5">
                  {section.key_points.map((kp, i) => (
                    <li key={i} className="text-[11px] text-ink border-b border-border pb-1.5 leading-relaxed">{kp}</li>
                  ))}
                </ul>
              </div>
            )}
            {section.risks?.length > 0 && (
              <div>
                <div className="text-[10px] uppercase tracking-wider text-rose mb-2">Risks</div>
                <ul className="space-y-1">
                  {section.risks.map((r, i) => <li key={i} className="text-[11px] text-rose/80">{r}</li>)}
                </ul>
              </div>
            )}
            {section.action_items?.length > 0 && (
              <div>
                <div className="text-[10px] uppercase tracking-wider text-sky mb-2">Actions</div>
                <ul className="space-y-1">
                  {section.action_items.map((a, i) => <li key={i} className="text-[11px] text-sky/80">{a}</li>)}
                </ul>
              </div>
            )}
          </div>
          {(section.source_chunks || section.source_chunk_ids)?.length > 0 && (
            <div className="flex gap-1.5 flex-wrap mt-3">
              {(section.source_chunks || section.source_chunk_ids).map((c) => (
                <span key={c} className="text-[10px] bg-surface2 border border-border rounded-full px-2 py-0.5 text-faint">
                  Chunk {c}
                </span>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

const SummaryPage = () => {
  const { summary } = useDocument();
  const navigate = useNavigate();

  // Guard: no summary at all
  if (!summary) return (
    <EmptyState
      icon={FileText}
      title="No summary yet"
      subtitle="Upload a document to get started"
      action={<Btn onClick={() => navigate("/")}>Upload Document</Btn>}
    />
  );

  // ── Normalize response shape ──────────────────────────────
  // Backend may return nested { metadata: {...} }
  // or flat { total_chunks, coverage_percent, ... }
  // Both are handled here.

  // Backend returns { doc_id, cached, filename, summary: { metadata, executive_summary, section_summaries } }
  const raw = summary.summary || summary;

  const metadata = raw.metadata || {
    total_chunks:     raw.total_chunks     || raw.chunk_count     || 0,
    valid_chunks:     raw.valid_chunks     || raw.chunk_count     || 0,
    coverage_percent: raw.coverage_percent || 100,
    missing_sections: raw.missing_sections || 0,
    status:           raw.status           || "ok",
  };

  const executive_summary = raw.executive_summary || {};
  const section_summaries =
    raw.section_summaries ||
    raw.sections          ||
    raw.chunk_summaries   ||
    [];


  // Debug: log the shape once (remove after confirming it works)

  return (
    <div className="max-w-4xl animate-fadeUp">
      <PageHeader
        title="Document Summary"
        subtitle={
          metadata.total_chunks
            ? `${metadata.total_chunks} chunks · ${metadata.valid_chunks} valid`
            : `doc_id: ${raw.doc_id || "—"}`
        }
        badge={<StatusBadge status={metadata.status || "ok"} />}
      />

      {/* Metrics row */}
      <div className="grid grid-cols-4 gap-3 mb-6">
        <MetricCard
          label="Coverage"
          value={(metadata.coverage_percent || 100) / 100}
        />
        <MetricCard
          label="Valid Chunks"
          value={metadata.valid_chunks || metadata.total_chunks || 0}
          unit=""
          color="blue"
        />
        <MetricCard
          label="Sections"
          value={section_summaries.length}
          unit=""
          color="green"
        />
        <MetricCard
          label="Missing Sections"
          value={metadata.missing_sections || 0}
          unit=""
          color={(metadata.missing_sections || 0) > 0 ? "red" : "green"}
        />
      </div>

      {/* Executive summary */}
      {(executive_summary.summary || executive_summary.tldr) ? (
        <div className="bg-surface border border-border rounded-xl p-6 mb-6">
          <div className="flex items-center gap-3 mb-4">
            <span className="text-[10px] uppercase tracking-widest text-amber bg-amber-glow px-2.5 py-1 rounded-full">
              Executive Summary
            </span>
            <span className="text-xs text-dim italic truncate">
              {executive_summary?.tldr}
            </span>
          </div>

          <p className="text-sm text-ink leading-relaxed mb-5">
            {executive_summary?.summary}
          </p>

          <div className="grid grid-cols-3 gap-5">
            <div>
              <div className="text-[10px] uppercase tracking-wider text-dim mb-2">Key Points</div>
              <ul className="space-y-2">
                {executive_summary?.key_points?.map((kp, i) => (
                  <li key={i} className="text-xs text-ink border-b border-border pb-2 leading-relaxed">{kp}</li>
                ))}
              </ul>
            </div>
            <div>
              <div className="text-[10px] uppercase tracking-wider text-rose mb-2">Risks</div>
              <ul className="space-y-1.5">
                {executive_summary?.risks?.slice(0, 8).map((r, i) => (
                  <li key={i} className="text-xs text-rose/80">{r}</li>
                ))}
              </ul>
            </div>
            <div>
              <div className="text-[10px] uppercase tracking-wider text-sky mb-2">Action Items</div>
              <ul className="space-y-1.5">
                {executive_summary?.action_items?.map((a, i) => (
                  <li key={i} className="text-xs text-sky/80">{a}</li>
                ))}
              </ul>
            </div>
          </div>
        </div>
      ) : (
        <div className="bg-surface border border-border rounded-xl p-6 mb-6 text-xs text-dim">
          Executive summary not available. Check backend response in console.
        </div>
      )}

      {/* Section summaries */}
      {section_summaries.length > 0 ? (
        <div>
          <div className="flex items-center justify-between mb-3">
            <span className="text-xs text-dim">Section Summaries</span>
            <span className="text-[11px] bg-surface2 px-2.5 py-1 rounded-full text-dim">
              {section_summaries.length} sections
            </span>
          </div>
          <div className="space-y-2 mb-8">
            {section_summaries.map((s) => (
              <SectionCard key={s.section_id} section={s} />
            ))}
          </div>
        </div>
      ) : (
        <div className="bg-surface border border-border rounded-xl p-6 mb-6 text-xs text-dim">
          No section summaries available.
        </div>
      )}

      <div className="pt-6 border-t border-border">
        <Btn onClick={() => navigate("/entities")}>Extract Entities →</Btn>
      </div>
    </div>
  );
};

export default SummaryPage;