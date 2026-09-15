const { useState, useEffect, useCallback } = React;

const API = "api";

const DRAFT_DAG = "draft_support_replies";
const EVAL_DAG = "evaluate_support_model_responses";

async function fetchJSON(path) {
  const res = await fetch(`${API}/${path}`);
  if (!res.ok) {
    let detail = "";
    try {
      detail = (await res.json()).detail || "";
    } catch (e) {
      detail = "";
    }
    throw new Error(detail || `${res.status} ${res.statusText}`);
  }
  return res.json();
}

function IconGauge() {
  return (
    <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M9 11l3 3 8-8" /><path d="M20 12v7a2 2 0 01-2 2H6a2 2 0 01-2-2V6a2 2 0 012-2h9" />
    </svg>
  );
}

function IconRefresh() {
  return (
    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="23 4 23 10 17 10" /><path d="M20.49 15a9 9 0 11-2.12-9.36L23 10" />
    </svg>
  );
}

function IconSun() {
  return (
    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="5" /><line x1="12" y1="1" x2="12" y2="3" /><line x1="12" y1="21" x2="12" y2="23" />
      <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" /><line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
      <line x1="1" y1="12" x2="3" y2="12" /><line x1="21" y1="12" x2="23" y2="12" />
      <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" /><line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
    </svg>
  );
}

function IconMoon() {
  return (
    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z" />
    </svg>
  );
}

function humanize(key) {
  return String(key).replace(/_/g, " ");
}

function fmtWhen(s) {
  if (!s) return "";
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return String(s);
  return d.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}

const SCORE_STYLE = {
  good: "bg-bGreen-50/15 text-bGreen-50 border-bGreen-50/30",
  acceptable: "bg-gold-40/15 text-gold-40 border-gold-40/30",
  poor: "bg-bRed-50/15 text-bRed-50 border-bRed-50/30",
};

const EQUIVALENCE_STYLE = {
  equivalent: "bg-bGreen-50/15 text-bGreen-50 border-bGreen-50/30",
  close: "bg-gold-40/15 text-gold-40 border-gold-40/30",
  different: "bg-bRed-50/15 text-bRed-50 border-bRed-50/30",
};

function ScoreChip({ name, dimension }) {
  const cls = SCORE_STYLE[dimension.score] || "bg-th-border/20 text-th-secondary border-th-border/30";
  return (
    <span className={`px-2 py-0.5 rounded-full text-[10px] font-medium border ${cls}`}>
      {humanize(name)}: {dimension.score}
    </span>
  );
}

function FlagChip({ name, value }) {
  if (typeof value === "boolean") {
    const cls = value
      ? "bg-bGreen-50/15 text-bGreen-50 border-bGreen-50/30"
      : "bg-bRed-50/15 text-bRed-50 border-bRed-50/30";
    return (
      <span className={`px-2 py-0.5 rounded-full text-[10px] font-medium border ${cls}`}>
        {humanize(name)}: {value ? "yes" : "no"}
      </span>
    );
  }
  const cls = EQUIVALENCE_STYLE[value] || "bg-th-border/20 text-th-secondary border-th-border/30";
  return (
    <span className={`px-2 py-0.5 rounded-full text-[10px] font-medium border ${cls}`}>
      {humanize(name)}: {String(value)}
    </span>
  );
}

function SummaryTiles({ summary }) {
  if (!summary || !summary.scored) return null;
  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-5">
      <div className="glass-dark rounded-lg border border-th-border/40 px-4 py-3">
        <div className="text-[10px] uppercase tracking-wider text-th-muted font-mono">replies scored</div>
        <div className="text-2xl font-display text-th-heading">{summary.scored}</div>
      </div>
      <div className="glass-dark rounded-lg border border-th-border/40 px-4 py-3">
        <div className="text-[10px] uppercase tracking-wider text-th-muted font-mono">avg reasoning tokens</div>
        <div className="text-2xl font-display text-th-heading">{summary.avg_reasoning_tokens ?? "-"}</div>
      </div>
      <div className="glass-dark rounded-lg border border-th-border/40 px-4 py-3">
        <div className="text-[10px] uppercase tracking-wider text-th-muted font-mono">avg latency</div>
        <div className="text-2xl font-display text-th-heading">
          {summary.avg_duration_ms == null ? "-" : `${summary.avg_duration_ms} ms`}
        </div>
      </div>
      <div className="glass-dark rounded-lg border border-th-border/40 px-4 py-3">
        <div className="text-[10px] uppercase tracking-wider text-th-muted font-mono">avg tool calls</div>
        <div className="text-2xl font-display text-th-heading">{summary.avg_tool_calls ?? "-"}</div>
      </div>
    </div>
  );
}

function DimensionRates({ dimensions }) {
  if (!dimensions || !dimensions.length) return null;
  return (
    <div className="glass-dark rounded-lg border border-th-border/40 overflow-hidden mb-5">
      <div className="px-4 py-2 border-b border-th-border/30 text-xs font-mono uppercase tracking-wider text-th-secondary">
        rate per dimension
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-[10px] uppercase tracking-wider text-th-muted font-mono">
              <th className="text-left px-4 py-2 font-medium">dimension</th>
              <th className="text-right px-3 py-2 font-medium">good</th>
              <th className="text-right px-3 py-2 font-medium">acceptable</th>
              <th className="text-right px-3 py-2 font-medium">poor</th>
              <th className="text-right px-4 py-2 font-medium">low confidence</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-th-surface2">
            {dimensions.map((d) => {
              const pct = (n) => (d.counted ? `${Math.round((n / d.counted) * 100)}%` : "-");
              return (
                <tr key={d.name}>
                  <td className="px-4 py-2 text-th-body">{humanize(d.name)}</td>
                  <td className="px-3 py-2 text-right font-mono text-bGreen-50">{pct(d.good)}</td>
                  <td className="px-3 py-2 text-right font-mono text-gold-40">{pct(d.acceptable)}</td>
                  <td className="px-3 py-2 text-right font-mono text-bRed-50">{pct(d.poor)}</td>
                  <td className="px-4 py-2 text-right font-mono text-th-secondary">{pct(d.low_confidence)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function EvalDetail({ record }) {
  return (
    <div className="border-t border-th-border/30 bg-th-surface/40 px-4 py-4 fade-in">
      <div className="grid md:grid-cols-2 gap-5">
        <div>
          <div className="text-[10px] uppercase tracking-wider text-th-muted font-mono mb-2">
            scores and reasoning
          </div>
          <div className="flex flex-col gap-3">
            {Object.entries(record.dimensions).map(([name, dimension]) => (
              <div key={name} className="rounded border border-th-border/30 px-3 py-2">
                <div className="flex items-center justify-between gap-2 mb-1">
                  <span className="text-sm text-th-heading">
                    {humanize(name)}
                    <span className="ml-2 text-[10px] font-mono text-th-muted">
                      {dimension.confidence} confidence
                    </span>
                  </span>
                  <ScoreChip name={name} dimension={dimension} />
                </div>
                <p className="text-xs text-th-secondary leading-relaxed">{dimension.reasoning}</p>
              </div>
            ))}
          </div>
        </div>
        <div>
          <div className="text-[10px] uppercase tracking-wider text-th-muted font-mono mb-2">
            the run that produced it
          </div>
          <dl className="text-xs font-mono grid grid-cols-[auto_1fr] gap-x-3 gap-y-1.5">
            <dt className="text-th-muted">model</dt>
            <dd className="text-th-body break-all">{record.model || "-"}</dd>
            <dt className="text-th-muted">trace</dt>
            <dd className="text-th-body break-all">{record.trace_id || "-"}</dd>
            <dt className="text-th-muted">span</dt>
            <dd className="text-th-body break-all">{record.span_id || "-"}</dd>
            {record.input_tokens != null && <><dt className="text-th-muted">tokens in</dt>
            <dd className="text-th-body">{record.input_tokens}</dd></>}
            {record.output_tokens != null && <><dt className="text-th-muted">tokens out</dt>
            <dd className="text-th-body">{record.output_tokens}</dd></>}
            <dt className="text-th-muted">reasoning tokens</dt>
            <dd className="text-th-body">{record.reasoning_tokens ?? "-"}</dd>
            <dt className="text-th-muted">duration</dt>
            <dd className="text-th-body">{record.duration_ms == null ? "-" : `${record.duration_ms} ms`}</dd>
            <dt className="text-th-muted">tool calls</dt>
            <dd className="text-th-body">
              {record.tool_calls.length
                ? record.tool_calls.map((t, i) => (
                    <div key={i}>{t.name}{t.duration_ms == null ? "" : ` · ${t.duration_ms} ms`}</div>
                  ))
                : "none"}
            </dd>
            {!!record.tools_available.length && <><dt className="text-th-muted">tools offered</dt>
            <dd className="text-th-body break-all">{record.tools_available.join(", ")}</dd></>}
            <dt className="text-th-muted">scored</dt>
            <dd className="text-th-body">{fmtWhen(record.scored_at) || "-"}</dd>
          </dl>
          {record.reply && (
            <div className="mt-4">
              <div className="text-[10px] uppercase tracking-wider text-th-muted font-mono mb-2">
                scored reply
              </div>
              <pre className="text-xs text-th-body whitespace-pre-wrap font-body leading-relaxed rounded border border-th-border/30 px-3 py-2 max-h-56 overflow-y-auto">
                {record.reply}
              </pre>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function EvalRow({ record, expanded, onToggle }) {
  return (
    <div className={expanded ? "border-l-2 border-l-th-accent" : "border-l-2 border-l-transparent"}>
      <button
        onClick={onToggle}
        className="w-full text-left px-4 py-3 hover:bg-th-surface2/40 transition-colors flex flex-col gap-1.5"
      >
        <div className="flex items-center justify-between gap-3">
          <span className="font-medium text-th-heading truncate">
            {record.ticket_id ? `${record.ticket_id} · ` : ""}{record.subject || "(no subject)"}
          </span>
          <span className="text-[10px] font-mono text-th-muted flex-shrink-0">{record.model || ""}</span>
        </div>
        <span className="text-xs text-th-secondary font-mono truncate">
          {record.customer_name || ""}{record.product_name ? ` · ${record.product_name}` : ""}
        </span>
        <div className="flex items-center gap-1.5 flex-wrap pt-0.5">
          {Object.entries(record.dimensions).map(([name, dimension]) => (
            <ScoreChip key={name} name={name} dimension={dimension} />
          ))}
          {Object.entries(record.flags).map(([name, value]) => (
            <FlagChip key={name} name={name} value={value} />
          ))}
        </div>
      </button>
      {expanded && <EvalDetail record={record} />}
    </div>
  );
}

function App() {
  const [evals, setEvals] = useState([]);
  const [summary, setSummary] = useState(null);
  const [expandedId, setExpandedId] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [light, setLight] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [e, s] = await Promise.all([fetchJSON("evals"), fetchJSON("summary")]);
      setEvals(e);
      setSummary(s);
      setError(null);
    } catch (err) {
      setError(String(err.message || err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    document.body.classList.toggle("light", light);
  }, [light]);

  return (
    <div className="min-h-screen">
      <header className="banner-header border-b border-th-border/40 sticky top-0 z-10">
        <div className="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-lg bg-th-accent/15 text-th-accent flex items-center justify-center">
              <IconGauge />
            </div>
            <h1 className="text-xl font-display text-th-heading tracking-wide">CosMarket AI Evals</h1>
          </div>
          <div className="flex items-center gap-4">
            <button onClick={() => setLight((v) => !v)} className="text-th-secondary hover:text-th-heading transition-colors">
              {light ? <IconMoon /> : <IconSun />}
            </button>
            <button onClick={load} className={`text-th-secondary hover:text-th-heading transition-colors ${loading ? "spin" : ""}`}>
              <IconRefresh />
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-6 py-6">
        {error && (
          <div className="mb-5 rounded-lg border border-bRed-50/30 bg-bRed-50/10 px-4 py-3 text-sm text-bRed-50">
            {error}
          </div>
        )}

        <SummaryTiles summary={summary} />
        <DimensionRates dimensions={summary ? summary.dimensions : []} />

        {evals.length ? (
          <div className="glass-dark rounded-lg border border-th-border/40 overflow-hidden divide-y divide-th-surface2">
            {evals.map((record) => (
              <EvalRow
                key={record.eval_id}
                record={record}
                expanded={expandedId === record.eval_id}
                onToggle={() => setExpandedId(expandedId === record.eval_id ? null : record.eval_id)}
              />
            ))}
          </div>
        ) : (
          !loading && (
            <div className="glass-dark rounded-lg border border-th-border/40 px-6 py-16 text-center">
              <div className="w-12 h-12 mx-auto mb-4 rounded-lg bg-th-surface3 text-th-secondary flex items-center justify-center">
                <IconGauge />
              </div>
              <p className="text-lg text-th-heading mb-2">No evals yet</p>
              <p className="text-sm text-th-secondary leading-relaxed">
                Run <code className="font-mono text-th-accent">{DRAFT_DAG}</code> to draft replies,
                then <code className="font-mono text-th-accent">{EVAL_DAG}</code> to score them
                against the traces of the runs that produced them.
              </p>
            </div>
          )
        )}
      </main>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
