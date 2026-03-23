// src/components/Shared.js
import { Loader2, AlertTriangle, CheckCircle2, XCircle } from "lucide-react";

export const Spinner = ({ text = "Processing..." }) => (
  <div className="flex flex-col items-center gap-3 py-12 text-dim">
    <Loader2 size={26} className="animate-spin text-amber" />
    <p className="text-xs">{text}</p>
  </div>
);

export const EmptyState = ({ icon: Icon, title, subtitle, action }) => (
  <div className="flex flex-col items-center gap-3 py-20 text-center">
    {Icon && <Icon size={38} className="text-faint" />}
    <h3 className="font-display text-lg font-semibold text-ink">{title}</h3>
    {subtitle && <p className="text-xs text-dim">{subtitle}</p>}
    {action && <div className="mt-2">{action}</div>}
  </div>
);

export const MetricCard = ({ label, value, unit = "%", color = "amber" }) => {
  const colors = {
    amber: "border-amber/20",
    blue:  "border-sky/20",
    green: "border-jade/20",
    red:   "border-rose/20",
  };
  const valColors = { amber: "text-amber", blue: "text-sky", green: "text-jade", red: "text-rose" };
  const display = unit === "%" && typeof value === "number" && value <= 1
    ? Math.round(value * 100)
    : value;

  return (
    <div className={`bg-surface border ${colors[color] || colors.amber} rounded-xl p-4 text-center`}>
      <div className={`font-display text-2xl font-bold ${valColors[color] || "text-ink"}`}>
        {display}<span className="text-sm text-dim ml-0.5">{unit}</span>
      </div>
      <div className="text-[11px] text-dim mt-1">{label}</div>
    </div>
  );
};

export const StatusBadge = ({ status }) => {
  const map = {
    ok:                     { icon: CheckCircle2, cls: "bg-jade-glow text-jade",  label: "OK" },
    low_coverage_warning:   { icon: AlertTriangle, cls: "bg-amber-glow text-amber", label: "Low Coverage" },
    section_missing_warning:{ icon: AlertTriangle, cls: "bg-amber-glow text-amber", label: "Missing Sections" },
    error:                  { icon: XCircle,       cls: "bg-rose-glow text-rose",  label: "Error" },
  };
  const { icon: Icon, cls, label } = map[status] || map["error"];
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-medium ${cls}`}>
      <Icon size={11} />{label}
    </span>
  );
};

export const PageHeader = ({ title, subtitle, badge }) => (
  <div className="flex items-start justify-between mb-7">
    <div>
      <h1 className="font-display text-2xl font-bold tracking-tight text-ink mb-1">{title}</h1>
      {subtitle && <p className="text-xs text-dim">{subtitle}</p>}
    </div>
    {badge && <div>{badge}</div>}
  </div>
);

export const Btn = ({ onClick, loading, disabled, children, variant = "primary", className = "" }) => {
  const base = "inline-flex items-center gap-2 px-4 py-2 rounded-md text-xs font-medium font-mono transition-all duration-150 disabled:opacity-50 disabled:cursor-not-allowed";
  const variants = {
    primary: "bg-amber text-black hover:bg-amber/90",
    ghost:   "border border-border text-dim hover:border-amber hover:text-amber",
    danger:  "border border-border text-dim hover:border-rose hover:text-rose",
  };
  return (
    <button className={`${base} ${variants[variant]} ${className}`} onClick={onClick} disabled={disabled || loading}>
      {loading && <Loader2 size={12} className="animate-spin" />}
      {children}
    </button>
  );
};

export const Tag = ({ children, color = "default" }) => {
  const colors = {
    default: "bg-surface2 border-border text-dim",
    amber:   "bg-amber-glow border-amber/20 text-amber",
    jade:    "bg-jade-glow border-jade/20 text-jade",
  };
  return (
    <span className={`text-[10px] px-2 py-0.5 rounded-full border ${colors[color]}`}>
      {children}
    </span>
  );
};
