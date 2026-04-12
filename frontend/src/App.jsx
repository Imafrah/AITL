import { useState } from "react"
import axios from "axios"

/** Dev: empty string uses Vite proxy to backend. Prod: set VITE_API_BASE_URL or default host. */
const API_BASE =
  import.meta.env.VITE_API_BASE_URL ??
  (import.meta.env.PROD ? "https://aitl.onrender.com" : "")

export default function App() {
  const [file, setFile] = useState(null)
  const [result, setResult] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [responseFormat, setResponseFormat] = useState("json")

  const handleUpload = async () => {
    if (!file) return
    setLoading(true)
    setError(null)
    setResult(null)

    const formData = new FormData()
    formData.append("file", file)

    try {
      if (responseFormat === "csv") {
        const res = await axios.post(`${API_BASE}/translate`, formData, {
          params: { format: "csv" },
          responseType: "blob",
        })
        const url = URL.createObjectURL(res.data)
        const a = document.createElement("a")
        a.href = url
        a.download = `${(file.name || "export").replace(/\.[^.]+$/, "")}.csv`
        a.click()
        URL.revokeObjectURL(url)
        setResult(null)
        return
      }

      const res = await axios.post(`${API_BASE}/translate`, formData, {
        params: { format: responseFormat },
      })
      setResult(res.data)
    } catch (err) {
      setError(err.response?.data?.detail || "Something went wrong.")
    } finally {
      setLoading(false)
    }
  }

  const rows = result?.data
  const hasUniversalEnvelope = Boolean(result?.document_id && Array.isArray(rows))

  return (
    <div style={styles.container}>
      <h1 style={styles.title}>🧠 AITL</h1>
      <p style={styles.subtitle}>Universal File Intelligence Engine</p>

      <div style={styles.uploadBox}>
        <input
          type="file"
          accept=".txt,.csv,.pdf"
          onChange={(e) => setFile(e.target.files[0])}
          style={styles.fileInput}
        />
        {file && <p style={styles.fileName}>📄 {file.name}</p>}
        <div style={{ marginBottom: 16 }}>
          <label style={{ color: "#94a3b8", fontSize: 14, marginRight: 8 }}>Response</label>
          <select
            value={responseFormat}
            onChange={(e) => setResponseFormat(e.target.value)}
            style={styles.select}
          >
            <option value="json">JSON (full envelope)</option>
            <option value="table">JSON + flattened table</option>
            <option value="dashboard">JSON + dashboard (analytics)</option>
            <option value="csv">Download CSV</option>
          </select>
        </div>
        <button
          onClick={handleUpload}
          disabled={!file || loading}
          style={styles.button}
        >
          {loading ? "Processing..." : "Process file"}
        </button>
      </div>

      {error && (
        <div style={styles.errorBox}>
          ❌ {error}
        </div>
      )}

      {result && typeof result === "object" && !Array.isArray(result) && hasUniversalEnvelope && (
        <div style={styles.resultContainer}>
          <div style={styles.statusRow}>
            <span style={{
              ...styles.badge,
              background: result.status === "success" ? "#22c55e"
                : result.status === "partial" ? "#f59e0b" : "#ef4444"
            }}>
              {String(result.status ?? "unknown").toUpperCase()}
            </span>
            <span style={styles.docId}>ID: {result.document_id}</span>
            <span style={styles.docId}>Type: {result.document_type}</span>
          </div>

          {result.error && (
            <div style={{ ...styles.errorBox, marginTop: 12 }}>
              {String(result.error)}
            </div>
          )}

          <h2 style={styles.sectionTitle}>Data ({rows.length} rows)</h2>
          <DataPreview rows={rows} />

          {result.table && (
            <>
              <h2 style={styles.sectionTitle}>Flattened table</h2>
              <DataPreview rows={result.table} />
            </>
          )}

          {result.dashboard && (
            <>
              <h2 style={styles.sectionTitle}>Dashboard</h2>
              <div style={styles.metaBox}>
                <h3 style={{ ...styles.sectionTitle, marginTop: 0 }}>Summary</h3>
                <pre style={styles.jsonBox}>
                  {JSON.stringify(result.dashboard.summary || {}, null, 2)}
                </pre>
                <h3 style={styles.sectionTitle}>Salary stats</h3>
                <pre style={styles.jsonBox}>
                  {JSON.stringify(result.dashboard.charts?.salary_stats || {}, null, 2)}
                </pre>
                <h3 style={styles.sectionTitle}>City distribution (top)</h3>
                <pre style={styles.jsonBox}>
                  {JSON.stringify(result.dashboard.charts?.city_distribution || {}, null, 2)}
                </pre>
                <h3 style={styles.sectionTitle}>Sample records</h3>
                <DataPreview rows={result.dashboard.records || []} />
              </div>
            </>
          )}

          <h2 style={styles.sectionTitle}>Metadata</h2>
          <div style={styles.metaBox}>
            {Object.entries(result.metadata || {}).map(([k, v]) => (
              <div key={k} style={styles.metaRow}>
                <span style={styles.metaKey}>{k}</span>
                <span style={styles.metaVal}>{String(v)}</span>
              </div>
            ))}
          </div>

          <h2 style={styles.sectionTitle}>Raw JSON</h2>
          <div style={{ display: "flex", gap: 12, marginBottom: 12, flexWrap: "wrap" }}>
            <button
              onClick={() => {
                const blob = new Blob([JSON.stringify(result, null, 2)], { type: "application/json" })
                const url = URL.createObjectURL(blob)
                const a = document.createElement("a")
                a.href = url
                a.download = `${result.document_id}.json`
                a.click()
                URL.revokeObjectURL(url)
              }}
              style={styles.downloadBtn}
            >
              ⬇ Download JSON
            </button>
            <button
              onClick={async () => {
                try {
                  const res = await axios.post(`${API_BASE}/export/toml`, result, {
                    responseType: "blob",
                  })
                  const url = URL.createObjectURL(res.data)
                  const a = document.createElement("a")
                  a.href = url
                  a.download = `${result.document_id ?? "export"}.toml`
                  a.click()
                  URL.revokeObjectURL(url)
                } catch {
                  alert("Failed to download TOML")
                }
              }}
              style={{ ...styles.downloadBtn, background: "#7c3aed" }}
            >
              ⬇ Download TOML
            </button>
          </div>
          <pre style={styles.jsonBox}>
            {JSON.stringify(result, null, 2)}
          </pre>
        </div>
      )}
    </div>
  )
}

function DataPreview({ rows }) {
  if (!rows?.length) {
    return <p style={{ color: "#64748b" }}>No rows.</p>
  }
  const keys = [...new Set(rows.flatMap((r) => Object.keys(r)))]
  return (
    <div style={{ overflowX: "auto", borderRadius: 10, border: "1px solid #334155" }}>
      <table style={styles.table}>
        <thead>
          <tr>
            {keys.map((k) => (
              <th key={k} style={styles.th}>{k}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} style={i % 2 ? styles.trAlt : undefined}>
              {keys.map((k) => (
                <td key={k} style={styles.td}>{r[k] === null || r[k] === undefined ? "" : String(r[k])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

const styles = {
  container: { maxWidth: 960, margin: "0 auto", padding: "40px 20px", fontFamily: "system-ui, sans-serif", background: "#0f172a", minHeight: "100vh", color: "#e2e8f0" },
  title: { fontSize: 36, fontWeight: 800, margin: 0, color: "#f8fafc" },
  subtitle: { color: "#94a3b8", marginTop: 4, marginBottom: 32 },
  uploadBox: { background: "#1e293b", border: "2px dashed #334155", borderRadius: 12, padding: 32, textAlign: "center" },
  fileInput: { marginBottom: 16 },
  fileName: { color: "#94a3b8", margin: "8px 0 16px" },
  select: { background: "#0f172a", color: "#e2e8f0", border: "1px solid #334155", borderRadius: 8, padding: "8px 12px", fontSize: 14 },
  button: { background: "#3b82f6", color: "#fff", border: "none", borderRadius: 8, padding: "12px 28px", fontSize: 16, cursor: "pointer", fontWeight: 600 },
  errorBox: { background: "#450a0a", border: "1px solid #ef4444", borderRadius: 8, padding: 16, marginTop: 24, color: "#fca5a5" },
  resultContainer: { marginTop: 32 },
  statusRow: { display: "flex", alignItems: "center", gap: 16, marginBottom: 24, flexWrap: "wrap" },
  badge: { padding: "4px 14px", borderRadius: 999, fontWeight: 700, fontSize: 13, color: "#fff" },
  docId: { color: "#64748b", fontSize: 13 },
  sectionTitle: { fontSize: 18, fontWeight: 700, color: "#cbd5e1", margin: "24px 0 12px" },
  metaBox: { background: "#1e293b", borderRadius: 10, padding: 16 },
  metaRow: { display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #334155" },
  metaKey: { color: "#64748b", fontSize: 13 },
  metaVal: { color: "#e2e8f0", fontSize: 13 },
  jsonBox: { background: "#1e293b", borderRadius: 10, padding: 20, fontSize: 12, overflowX: "auto", color: "#94a3b8", lineHeight: 1.6 },
  downloadBtn: {
    background: "#1e293b",
    color: "#94a3b8",
    border: "1px solid #334155",
    borderRadius: 8,
    padding: "8px 20px",
    fontSize: 14,
    cursor: "pointer",
    marginBottom: 12,
    fontWeight: 600
  },
  table: { width: "100%", borderCollapse: "collapse", fontSize: 13 },
  th: { textAlign: "left", padding: "10px 12px", background: "#334155", color: "#cbd5e1", borderBottom: "1px solid #475569" },
  td: { padding: "8px 12px", borderBottom: "1px solid #334155", color: "#e2e8f0" },
  trAlt: { background: "#1e293b" },
}
