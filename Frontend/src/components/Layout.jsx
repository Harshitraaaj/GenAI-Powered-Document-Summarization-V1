// src/components/Layout.js
import { Outlet, NavLink, useNavigate } from "react-router-dom";
import { useDocument } from "../context/DocumentContext";
import { FileText, Cpu, Share2, ShieldCheck, Search, Home, RotateCcw, ChevronRight } from "lucide-react";

const NAV = [
  { path: "/",           icon: Home,         label: "Upload",          step: null },
  { path: "/summary",   icon: FileText,     label: "Summary",         step: "summarized" },
  { path: "/entities",  icon: Cpu,          label: "Entities",        step: "extracted" },
  { path: "/graph",     icon: Share2,       label: "Knowledge Graph", step: "graphBuilt" },
  { path: "/factcheck", icon: ShieldCheck,  label: "Fact Verify",     step: "verified" },
  { path: "/query",     icon: Search,       label: "Query",           step: "summarized" },
];

const Layout = () => {
  const { docId, fileName, steps, reset } = useDocument();
  const navigate = useNavigate();

  const doneCount = Object.values(steps).filter(Boolean).length;

  return (
    <div className="flex h-screen overflow-hidden bg-bg">
      {/* ── Sidebar ── */}
      <aside className="w-56 min-w-[224px] bg-surface border-r border-border flex flex-col py-5 overflow-y-auto">

        {/* Brand */}
        <div className="flex items-center gap-3 px-5 pb-5 border-b border-border mb-3">
          <div className="w-8 h-8 bg-amber rounded-md grid place-items-center text-black flex-shrink-0">
            <FileText size={15} />
          </div>
          <span className="font-display font-bold text-base text-ink tracking-tight">DocAI</span>
        </div>

        {/* Active file pill */}
        {fileName && (
          <div className="flex items-center gap-2 px-5 mb-3">
            <div className="w-1.5 h-1.5 rounded-full bg-jade animate-pulse2 flex-shrink-0" />
            <span className="text-[11px] text-dim truncate" title={fileName}>{fileName}</span>
          </div>
        )}

        {/* Nav items */}
        <nav className="flex flex-col gap-0.5 px-2.5">
          {NAV.map(({ path, icon: Icon, label, step }) => {
            const locked = step && !steps[step] && !docId;
            const done   = step && steps[step];
            return (
              <NavLink
                key={path}
                to={path}
                end={path === "/"}
                className={({ isActive }) =>
                  `flex items-center gap-2.5 px-2.5 py-2 rounded-md text-xs transition-all duration-150 group
                  ${isActive
                    ? "bg-amber-glow text-amber border border-amber/20"
                    : locked
                    ? "text-faint cursor-not-allowed pointer-events-none opacity-40"
                    : "text-dim hover:bg-surface2 hover:text-ink"}`
                }
              >
                <Icon size={14} />
                <span className="flex-1">{label}</span>
                {done && <div className="w-1.5 h-1.5 rounded-full bg-jade flex-shrink-0" />}
                {!locked && (
                  <ChevronRight size={11} className="opacity-0 group-hover:opacity-100 transition-opacity" />
                )}
              </NavLink>
            );
          })}
        </nav>

        {/* Pipeline progress */}
        {docId && (
          <div className="mx-5 mt-5 pt-4 border-t border-border">
            <div className="text-[10px] uppercase tracking-widest text-faint mb-2">Pipeline</div>
            <div className="flex gap-1 mb-1.5">
              {["summarized","extracted","graphBuilt","verified"].map((s) => (
                <div key={s} className={`flex-1 h-[3px] rounded-full transition-colors duration-300 ${steps[s] ? "bg-amber" : "bg-border2"}`} />
              ))}
            </div>
            <div className="text-[10px] text-dim">{doneCount} / 4 complete</div>
          </div>
        )}

        {/* Reset */}
        {docId && (
          <button
            onClick={() => { reset(); navigate("/"); }}
            className="flex items-center gap-2 mx-5 mt-3 px-2.5 py-2 rounded-md border border-border text-dim text-[11px] hover:border-rose hover:text-rose transition-all"
          >
            <RotateCcw size={12} /> New Document
          </button>
        )}

        <div className="mt-auto px-5 pt-4 border-t border-border text-[10px] text-faint">
          GenAI Document Summarizer
        </div>
      </aside>

      {/* ── Main ── */}
      <main className="flex-1 overflow-y-auto p-10">
        <Outlet />
      </main>
    </div>
  );
};

export default Layout;
