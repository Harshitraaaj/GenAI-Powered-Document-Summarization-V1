// src/pages/FactCheckPage.js
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import toast from "react-hot-toast";
import { ShieldCheck, CheckCircle2, XCircle } from "lucide-react";
import { useDocument } from "../context/DocumentContext";
import { verifyFacts } from "../services/api";
import { PageHeader, MetricCard, StatusBadge, EmptyState, Btn, Spinner } from "../components/Shared";

const FactCheckPage = () => {
  const { docId, factData, setFactData, completeStep } = useDocument();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);

  const handleVerify = async () => {
    if (!docId) return;
    setLoading(true);
    const tid = toast.loading("Verifying facts against source...");
    try {
      const data = await verifyFacts(docId);
      setFactData(data);
      completeStep("verified");
      toast.success(`${Math.round(data.coverage_score * 100)}% factual accuracy`, { id: tid });
    } catch (err) {
      toast.error(err?.response?.data?.detail || "Fact verification failed", { id: tid });
    } finally { setLoading(false); }
  };

  if (!docId) return (
    <EmptyState icon={ShieldCheck} title="No document loaded" subtitle="Upload and summarize a document first"
      action={<Btn onClick={() => navigate("/")}>Go to Upload</Btn>} />
  );

  return (
    <div className="max-w-3xl animate-fadeUp">
      <PageHeader
        title="Fact Verification"
        subtitle="Verify summary claims against source document"
        badge={factData && <StatusBadge status={factData.status} />}
      />

      {!factData && (
        <div className="bg-surface border border-border rounded-xl p-6 mb-6">
          <p className="text-xs text-dim leading-relaxed mb-4">
            Each claim from the executive summary is verified against the source document
            using FAISS semantic search and LLM reasoning. Prose chunks are preferred over
            statistical tables for more accurate verification.
          </p>
          <Btn onClick={handleVerify} loading={loading}>
            {loading ? "Verifying..." : "Run Fact Verification"}
          </Btn>
        </div>
      )}

      {loading && <Spinner text="Verifying claims against source chunks..." />}

      {factData && !loading && (
        <>
          {/* Metrics */}
          <div className="grid grid-cols-4 gap-3 mb-6">
            <MetricCard label="Factual Accuracy" value={factData.coverage_score}          color="green" />
            <MetricCard label="Supported"         value={factData.supported_claims}        unit="" color="blue" />
            <MetricCard label="Total Claims"      value={factData.total_claims}            unit="" color="blue" />
            <MetricCard label="Flagged"           value={factData.flagged_claims?.length}  unit=""
              color={factData.flagged_claims?.length > 0 ? "red" : "green"} />
          </div>

          {/* Progress bar */}
          <div className="bg-surface border border-border rounded-xl p-5 mb-5">
            <div className="flex justify-between text-xs text-dim mb-3">
              <span>{factData.supported_claims} of {factData.total_claims} claims supported</span>
              <span className="text-jade">{Math.round(factData.coverage_score * 100)}%</span>
            </div>
            <div className="h-2 bg-surface2 rounded-full overflow-hidden">
              <div
                className="h-full bg-jade rounded-full transition-all duration-1000"
                style={{ width: `${factData.coverage_score * 100}%` }}
              />
            </div>
          </div>

          {/* Flagged claims */}
          {factData.flagged_claims?.length > 0 ? (
            <div className="mb-6">
              <div className="flex items-center gap-2 text-rose text-xs mb-3">
                <XCircle size={13} />
                Flagged Claims ({factData.flagged_claims.length})
              </div>
              <div className="space-y-2">
                {factData.flagged_claims.map((f, i) => (
                  <div key={i} className="bg-surface border border-rose/20 rounded-lg p-4">
                    <p className="text-sm text-ink mb-2">{f.claim}</p>
                    <p className="text-xs text-dim leading-relaxed mb-2">{f.reason}</p>
                    <span className="text-[10px] text-rose">
                      Confidence: {Math.round((f.confidence || 0) * 100)}%
                    </span>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div className="flex items-center gap-2 px-4 py-3.5 bg-jade-glow border border-jade/25 rounded-lg text-jade text-sm mb-6">
              <CheckCircle2 size={15} />
              All claims are supported by the source document
            </div>
          )}

          <div className="pt-6 border-t border-border">
            <Btn onClick={() => navigate("/query")}>Semantic Query →</Btn>
          </div>
        </>
      )}
    </div>
  );
};

export default FactCheckPage;
