// export default GraphPage;

// src/pages/GraphPage.jsx
import { useState, useEffect, useRef, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import toast from "react-hot-toast";
import { Share2, Search, X, ZoomIn, ZoomOut, Maximize2, Minimize2, Loader2, RefreshCw } from "lucide-react";
import ForceGraph2D from "react-force-graph-2d";
import { useDocument } from "../context/DocumentContext";
import { buildGraph, queryGraph, getFullGraph } from "../services/api";
import { PageHeader, MetricCard, EmptyState, Btn, Spinner } from "../components/Shared";

const TYPE_COLOR = {
  PERSON:       "#4a9eff",
  ORGANIZATION: "#4caf82",
  TECHNOLOGY:   "#f0a500",
  CONCEPT:      "#a78bfa",
  LOCATION:     "#f472b6",
  EVENT:        "#fb923c",
  PRODUCT:      "#2dd4bf",
  DATE:         "#a3e635",
  OTHER:        "#666666",
};

const getNodeColor = (node) => {
  const type = (node.type || "OTHER").toUpperCase();
  return TYPE_COLOR[type] || TYPE_COLOR.OTHER;
};

const buildGraphData = (relationships, entityMap) => {
  const nodeSet = new Map();
  const links   = [];

  relationships.forEach((rel) => {
    if (!nodeSet.has(rel.source)) {
      const info = entityMap.get(rel.source.toLowerCase()) || {};
      nodeSet.set(rel.source, {
        id:    rel.source,
        label: rel.source,
        type:  rel.source_type || info.type || "OTHER",
        val:   (info.source_chunk_ids?.length || 1) * 2,
      });
    }
    if (!nodeSet.has(rel.target)) {
      const info = entityMap.get(rel.target.toLowerCase()) || {};
      nodeSet.set(rel.target, {
        id:    rel.target,
        label: rel.target,
        type:  rel.target_type || info.type || "OTHER",
        val:   (info.source_chunk_ids?.length || 1) * 2,
      });
    }
    links.push({
      source: rel.source,
      target: rel.target,
      label:  rel.relationship || "",
    });
  });

  return { nodes: Array.from(nodeSet.values()), links };
};

// ─── Graph Modal ──────────────────────────────────────────────
const GraphModal = ({ docId, entityList, onClose }) => {
  const fgRef    = useRef();
  const dragRef  = useRef({ active: false, startX: 0, startY: 0, originX: 0, originY: 0 });

  const [rels, setRels]             = useState([]);
  const [loading, setLoading]       = useState(true);
  const [queryEnt, setQueryEnt]     = useState("");
  const [querying, setQuerying]     = useState(false);
  const [hovered, setHovered]       = useState(null);
  const [graphData, setGraphData]   = useState({ nodes: [], links: [] });
  const [isFullscreen, setFull]     = useState(false);
  const [pos, setPos]               = useState({ x: 0, y: 0 });

  const entityMap = useRef(new Map());

  // ── Build entity lookup map ──────────────────────────────
  useEffect(() => {
    entityList.forEach((e) => {
      entityMap.current.set(e.name.toLowerCase(), e);
    });
  }, [entityList]);

  // ── Rebuild graph data whenever rels change ──────────────
  useEffect(() => {
    setGraphData(buildGraphData(rels, entityMap.current));
  }, [rels]);

  // ── Auto-load ALL relationships via /graph-all ───────────
  useEffect(() => {
    const autoLoad = async () => {
      if (!docId) { setLoading(false); return; }
      try {
        const data = await getFullGraph(docId);
        setRels(data?.relationships || []);
      } catch {
        try {
          const sorted = [...entityList].sort(
            (a, b) => (b.source_chunk_ids?.length || 0) - (a.source_chunk_ids?.length || 0)
          );
          const anchor = sorted[0]?.name;
          if (anchor) {
            const fb = await queryGraph(docId, anchor);
            setRels(fb?.relationships || []);
          }
        } catch { }
      } finally {
        setLoading(false);
      }
    };
    autoLoad();
  }, [docId, entityList]);

  // ── Merge new relationships into existing ────────────────
  const mergeRels = useCallback((newRels) => {
    setRels((prev) => {
      const seen  = new Set(prev.map((r) => `${r.source}|${r.relationship}|${r.target}`));
      const fresh = newRels.filter((r) => !seen.has(`${r.source}|${r.relationship}|${r.target}`));
      return [...prev, ...fresh];
    });
  }, []);

  const handleQuery = async () => {
    if (!queryEnt.trim()) return;
    setQuerying(true);
    try {
      const data = await queryGraph(docId, queryEnt);
      mergeRels(data?.relationships || []);
      toast.success(`Added ${data?.relationships?.length || 0} relationships`);
    } catch { toast.error("Query failed"); }
    finally { setQuerying(false); setQueryEnt(""); }
  };

  // ── Auto fit after loading ────────────────────────────────
  useEffect(() => {
    if (!loading && fgRef.current) {
      setTimeout(() => fgRef.current?.zoomToFit(400, 60), 600);
    }
  }, [loading, graphData.nodes.length]);

  // ── Drag to reposition ────────────────────────────────────
  const onHeaderMouseDown = (e) => {
    if (isFullscreen) return;
    // Don't start drag if clicking a button inside header
    if (e.target.closest("button") || e.target.closest("input")) return;
    dragRef.current = {
      active:  true,
      startX:  e.clientX,
      startY:  e.clientY,
      originX: pos.x,
      originY: pos.y,
    };
    e.preventDefault();
  };

  useEffect(() => {
    const onMouseMove = (e) => {
      if (!dragRef.current.active) return;
      setPos({
        x: dragRef.current.originX + (e.clientX - dragRef.current.startX),
        y: dragRef.current.originY + (e.clientY - dragRef.current.startY),
      });
    };
    const onMouseUp = () => { dragRef.current.active = false; };

    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
    return () => {
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
    };
  }, []);

  // ── Fullscreen toggle ─────────────────────────────────────
  const toggleFullscreen = () => {
    if (!isFullscreen) setPos({ x: 0, y: 0 }); // reset position on enter
    setFull((p) => !p);
    setTimeout(() => fgRef.current?.zoomToFit(400, 40), 300);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm pointer-events-none">
      <div
        className={`
          bg-[#0f0f0f] border border-[#272727] shadow-2xl flex flex-col overflow-hidden
          pointer-events-auto transition-[width,height,border-radius] duration-200
          ${isFullscreen
            ? "fixed inset-2 rounded-xl"
            : "rounded-2xl w-full max-w-6xl h-[90vh]"}
        `}
        style={isFullscreen ? {} : { transform: `translate(${pos.x}px, ${pos.y}px)` }}
      >

        {/* ── Header (drag handle) ── */}
        <div
          className={`flex items-center gap-3 px-5 py-3 border-b border-[#272727] bg-[#161616] flex-shrink-0 select-none
            ${!isFullscreen ? "cursor-grab active:cursor-grabbing" : "cursor-default"}`}
          onMouseDown={onHeaderMouseDown}
        >
          <Share2 size={14} className="text-amber flex-shrink-0" />
          <span className="font-display font-bold text-sm text-ink">Knowledge Graph</span>
          <span className="text-[10px] text-dim bg-[#1e1e1e] px-2 py-0.5 rounded-full border border-[#333]">
            {graphData.nodes.length} nodes · {graphData.links.length} edges
          </span>
         

          <div className="flex items-center gap-2 ml-auto">
            {/* Expand input */}
            <div className="flex items-center gap-2 bg-[#1e1e1e] border border-[#333] rounded-lg px-3 py-1.5 w-52">
              <Search size={11} className="text-dim flex-shrink-0" />
              <input
                className="bg-transparent text-ink text-[11px] flex-1 placeholder:text-[#444] outline-none font-mono"
                placeholder="Expand: add entity..."
                value={queryEnt}
                onChange={(e) => setQueryEnt(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleQuery()}
              />
            </div>
            {/* <button
              onClick={handleQuery}
              disabled={!queryEnt.trim() || querying}
              className="px-3 py-1.5 bg-amber text-black rounded-lg text-[11px] font-mono font-medium
                hover:bg-amber/90 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-1.5"
            >
              {querying ? <Loader2 size={10} className="animate-spin" /> : <span>Expand</span>}
            </button> */}

            {/* Zoom in */}
            <button
              onClick={() => fgRef.current?.zoom(1.4, 300)}
              className="w-7 h-7 rounded-lg border border-[#333] text-dim hover:text-amber hover:border-amber grid place-items-center transition-all"
              title="Zoom in"
            >
              <ZoomIn size={12} />
            </button>

            {/* Zoom out */}
            <button
              onClick={() => fgRef.current?.zoom(0.7, 300)}
              className="w-7 h-7 rounded-lg border border-[#333] text-dim hover:text-amber hover:border-amber grid place-items-center transition-all"
              title="Zoom out"
            >
              <ZoomOut size={12} />
            </button>

            {/* Fit to screen
            <button
              onClick={() => fgRef.current?.zoomToFit(400, 40)}
              className="w-7 h-7 rounded-lg border border-[#333] text-dim hover:text-amber hover:border-amber grid place-items-center transition-all"
              title="Fit graph to screen"
            >
              <Maximize2 size={12} />
            </button>

            {/* Fullscreen toggle */}
            {/* <button
              onClick={toggleFullscreen}
              className={`w-7 h-7 rounded-lg border grid place-items-center transition-all
                ${isFullscreen
                  ? "border-amber/60 text-amber bg-amber/10 hover:bg-amber/20"
                  : "border-[#333] text-dim hover:text-amber hover:border-amber"}`}
              title={isFullscreen ? "Exit fullscreen" : "Enter fullscreen"}
            >
              {isFullscreen ? <Minimize2 size={12} /> : <Maximize2 size={12} />}
            </button> */} 

            {/* Close */}
            <button
              onClick={onClose}
              className="w-7 h-7 rounded-lg border border-[#333] text-dim hover:text-rose hover:border-rose grid place-items-center transition-all ml-1"
              title="Close"
            >
              <X size={12} />
            </button>
          </div>
        </div>

        {/* ── Graph area ── */}
        <div className="flex-1 relative overflow-hidden">
          {loading ? (
            <div className="flex flex-col items-center justify-center h-full gap-3">
              <Loader2 size={28} className="animate-spin text-amber" />
              <p className="text-xs text-dim">Loading graph relationships...</p>
            </div>
          ) : graphData.nodes.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-full gap-3">
              <Share2 size={36} className="text-[#333]" />
              <p className="text-sm text-dim">No relationships to display</p>
              <p className="text-xs text-[#444]">Try expanding an entity above</p>
            </div>
          ) : (
            <ForceGraph2D
              ref={fgRef}
              graphData={graphData}
              backgroundColor="#0f0f0f"
              nodeRelSize={3}
              nodeVal={1}
              nodeColor={getNodeColor}
              nodeLabel={(node) => `${node.label} (${node.type})`}
              nodeCanvasObjectMode={() => "after"}
              nodeCanvasObject={(node, ctx, globalScale) => {
                const NODE_R = 5;
                const color  = getNodeColor(node);
                const isHov  = hovered?.id === node.id;
                const label  = node.label.length > 14 ? node.label.slice(0, 13) + "…" : node.label;

                if (isHov) {
                  ctx.beginPath();
                  ctx.arc(node.x, node.y, NODE_R * 2.4, 0, 2 * Math.PI);
                  ctx.fillStyle = color + "30";
                  ctx.fill();
                  ctx.strokeStyle = color;
                  ctx.lineWidth = 1.5;
                  ctx.stroke();
                }

                const fs = Math.max(10 / globalScale, 2.5);
                ctx.font         = `${fs}px DM Mono, monospace`;
                ctx.textAlign    = "center";
                ctx.textBaseline = "top";
                ctx.fillStyle    = isHov ? "#fff" : "#cccccc";
                ctx.fillText(label, node.x, node.y + NODE_R + 2);
              }}
              linkColor={() => "#3a3a3a"}
              linkWidth={1.2}
              linkDirectionalArrowLength={5}
              linkDirectionalArrowRelPos={1}
              linkDirectionalArrowColor={() => "#666"}
              linkCurvature={0.15}
              linkCanvasObjectMode={() => "after"}
              linkCanvasObject={(link, ctx, globalScale) => {
                if (!link.label || globalScale < 1.5) return;
                const s = link.source;
                const t = link.target;
                if (typeof s !== "object" || typeof t !== "object") return;
                const fs = Math.max(7 / globalScale, 2);
                ctx.font         = `${fs}px DM Mono, monospace`;
                ctx.textAlign    = "center";
                ctx.textBaseline = "middle";
                ctx.fillStyle    = "#555";
                ctx.fillText(link.label, (s.x + t.x) / 2, (s.y + t.y) / 2);
              }}
              onNodeHover={(node) => setHovered(node)}
              onNodeClick={(node) => setQueryEnt(node.id)}
              d3AlphaDecay={0.015}
              d3VelocityDecay={0.25}
              warmupTicks={80}
              cooldownTicks={150}
              onEngineStop={() => fgRef.current?.zoomToFit(400, 40)}
            />
          )}

          {/* Node tooltip */}
          {hovered && (
            <div className="absolute bottom-16 left-4 bg-[#161616] border border-[#333] rounded-lg px-3 py-2 text-[11px] pointer-events-none">
              <div className="text-ink font-medium">{hovered.label}</div>
              <div className="text-dim mt-0.5">Type: {hovered.type}</div>
              <div className="text-[#555] mt-0.5 text-[10px]">Click to expand relationships</div>
            </div>
          )}
        </div>

        {/* ── Legend ── */}
        <div className="flex items-center gap-4 px-5 py-2.5 border-t border-[#272727] bg-[#161616] flex-wrap flex-shrink-0">
          {Object.entries(TYPE_COLOR).filter(([t]) => t !== "OTHER").map(([type, color]) => (
            <div key={type} className="flex items-center gap-1.5">
              <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ background: color }} />
              <span className="text-[10px] text-dim">{type}</span>
            </div>
          ))}
          <span className="ml-auto text-[10px] text-[#444]">
            {isFullscreen ? "Fullscreen mode" : "Drag header to move"} · Scroll to zoom · Click node to expand
          </span>
        </div>
      </div>
    </div>
  );
};

// ─── Main Page ────────────────────────────────────────────────
const GraphPage = () => {
  const { docId, graphData, setGraphData, entities, steps, completeStep } = useDocument();
  const navigate = useNavigate();
  const [building, setBuilding]    = useState(false);
  const [querying, setQuerying]    = useState(false);
  const [entity, setEntity]        = useState("");
  const [queryResults, setResults] = useState(null);
  const [showModal, setShowModal]  = useState(false);

  const nodes         = graphData?.nodes_created         ?? graphData?.nodes         ?? 0;
  const relationships = graphData?.relationships_created ?? graphData?.relationships ?? 0;

  const handleBuild = async () => {
    if (!docId) return;
    setBuilding(true);
    const tid = toast.loading("Building knowledge graph...");
    try {
      const data = await buildGraph(docId);
      setGraphData(data);
      completeStep("graphBuilt");
      const n = data.nodes_created ?? data.nodes ?? 0;
      const r = data.relationships_created ?? data.relationships ?? 0;
      toast.success(`${n} nodes · ${r} relationships`, { id: tid });
    } catch (err) {
      toast.error(err?.response?.data?.detail || "Graph build failed", { id: tid });
    } finally { setBuilding(false); }
  };

  const handleQuery = async () => {
    if (!entity.trim() || !docId) return;
    setQuerying(true);
    try {
      const data = await queryGraph(docId, entity);
      setResults(data);
    } catch { toast.error("Graph query failed"); }
    finally { setQuerying(false); }
  };

  if (!docId) return (
    <EmptyState icon={Share2} title="No document loaded" subtitle="Upload and summarize a document first"
      action={<Btn onClick={() => navigate("/")}>Go to Upload</Btn>} />
  );

  return (
    <div className="max-w-4xl animate-fadeUp">
      <PageHeader title="Knowledge Graph" subtitle="Entity relationships extracted from the document" />

      {!graphData && (
        <div className="bg-surface border border-border rounded-xl p-6 mb-6">
          <p className="text-xs text-dim leading-relaxed mb-4">
            Build a Neo4j knowledge graph from extracted entities using dynamic anchor detection.
          </p>
          {!steps.extracted && (
            <div className="flex items-center gap-2 px-3 py-2 bg-amber-glow border border-amber/25 rounded-md text-xs text-amber mb-4">
              ⚠ Run entity extraction first for best results
            </div>
          )}
          <Btn onClick={handleBuild} loading={building}>
            {building ? "Building..." : "Build Knowledge Graph"}
          </Btn>
        </div>
      )}

      {building && <Spinner text="Extracting relationships and building graph..." />}

      {graphData && !building && (
        <div className="mb-6">
          <div className="grid grid-cols-2 gap-3 mb-4">
            <MetricCard label="Nodes"         value={nodes}         unit="" color="blue" />
            <MetricCard label="Relationships" value={relationships} unit="" color="amber" />
          </div>
          <div className="flex gap-2">
            <button
              onClick={() => setShowModal(true)}
              className="flex items-center gap-2 px-4 py-2.5 bg-amber text-black rounded-md text-xs
                font-medium font-mono hover:bg-amber/90 transition-colors"
            >
              <Share2 size={13} />
              View Graph Visualization
            </button>
            <Btn variant="ghost" onClick={handleBuild} disabled={building}>
              <RefreshCw size={12} /> Rebuild
            </Btn>
          </div>
        </div>
      )}

      {graphData && (
        <div className="bg-surface border border-border rounded-xl p-6 mb-8">
          <h2 className="font-display font-semibold text-base text-ink mb-1">Query Relationships</h2>
          <p className="text-xs text-dim mb-4">Search for an entity to see all its relationships as a table</p>

          <div className="flex gap-2 mb-5">
            <div className="flex items-center gap-2 flex-1 bg-surface2 border border-border rounded-md px-3 py-2">
              <Search size={13} className="text-dim flex-shrink-0" />
              <input
                className="bg-transparent text-ink text-xs flex-1 placeholder-faint outline-none font-mono"
                placeholder="e.g. SmolVLA, ICLR, Bryce Grant..."
                value={entity}
                onChange={(e) => setEntity(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleQuery()}
              />
            </div>
            <Btn onClick={handleQuery} loading={querying} disabled={!entity.trim()}>Query</Btn>
          </div>

          {querying && <Spinner text="Querying graph..." />}

          {queryResults && !querying && (
            <div className="border border-border rounded-lg overflow-hidden">
              <div className="grid grid-cols-[1fr_140px_1fr_2fr] gap-3 px-4 py-2 bg-surface2 border-b border-border
                text-[10px] uppercase tracking-wider text-dim">
                <span>Source</span><span>Relationship</span><span>Target</span><span>Description</span>
              </div>
              <div className="max-h-72 overflow-y-auto">
                {queryResults.relationships?.length > 0 ? (
                  queryResults.relationships.map((r, i) => (
                    <div key={i} className="grid grid-cols-[1fr_140px_1fr_2fr] gap-3 px-4 py-2.5
                      border-b border-border last:border-0 text-xs items-center hover:bg-surface2 transition-colors">
                      <span className="text-amber truncate">{r.source}</span>
                      <span className="text-[10px] bg-surface border border-border rounded-full px-2 py-0.5 text-dim text-center">
                        {r.relationship}
                      </span>
                      <span className="text-sky truncate">{r.target}</span>
                      <span className="text-dim text-[11px] truncate">{r.description}</span>
                    </div>
                  ))
                ) : (
                  <div className="py-8 text-center text-xs text-dim">
                    No relationships found for "{entity}"
                  </div>
                )}
              </div>
              <div className="px-4 py-2 border-t border-border text-[11px] text-faint">
                {queryResults.relationships?.length || 0} relationships found
              </div>
            </div>
          )}
        </div>
      )}

      <div className="pt-6 border-t border-border">
        <Btn onClick={() => navigate("/factcheck")}>Verify Facts →</Btn>
      </div>

      {showModal && (
        <GraphModal
          docId={docId}
          entityList={entities?.entities || []}
          onClose={() => setShowModal(false)}
        />
      )}
    </div>
  );
};

export default GraphPage;