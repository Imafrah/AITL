import { useState } from "react"
import axios from "axios"

export default function App() {
  const [file, setFile] = useState(null)
  const [result, setResult] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const handleUpload = async () => {
    if (!file) return
    setLoading(true)
    setError(null)
    setResult(null)

    const formData = new FormData()
    formData.append("file", file)

    try {
      const res = await axios.post("/translate", formData)
      setResult(res.data)
    } catch (err) {
      setError(err.response?.data?.detail || "Something went wrong.")
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={styles.container}>
      <h1 style={styles.title}>🧠 AITL</h1>
      <p style={styles.subtitle}>AI Data Translation Layer</p>

      {/* Upload Box */}
      <div style={styles.uploadBox}>
        <input
          type="file"
          accept=".txt,.csv,.pdf"
          onChange={(e) => setFile(e.target.files[0])}
          style={styles.fileInput}
        />
        {file && <p style={styles.fileName}>📄 {file.name}</p>}
        <button
          onClick={handleUpload}
          disabled={!file || loading}
          style={styles.button}
        >
          {loading ? "Processing..." : "Translate Document"}
        </button>
      </div>

      {/* Error */}
      {error && (
        <div style={styles.errorBox}>
          ❌ {error}
        </div>
      )}

      {/* Results */}
      {result && (
        <div style={styles.resultContainer}>

          {/* Status Badge */}
          <div style={styles.statusRow}>
            <span style={{
              ...styles.badge,
              background: result.status === "success" ? "#22c55e"
                : result.status === "partial" ? "#f59e0b" : "#ef4444"
            }}>
              {result.status.toUpperCase()}
            </span>
            <span style={styles.docId}>ID: {result.document_id}</span>
          </div>

          {/* Entity Cards */}
          <h2 style={styles.sectionTitle}>Entities</h2>
          <div style={styles.cardGrid}>
            <EntityGroup label="👤 People" items={result.entities?.person_names} color="#3b82f6" />
            <EntityGroup label="🏢 Organizations" items={result.entities?.organizations} color="#8b5cf6" />
            <EntityGroup label="📅 Dates" items={result.entities?.dates} color="#06b6d4" />
            <EntityGroup label="💰 Amounts" items={result.entities?.amounts} color="#22c55e" />
          </div>

          {/* Relationships */}
          {result.relationships?.length > 0 && (
            <>
              <h2 style={styles.sectionTitle}>Relationships</h2>
              <div style={styles.relBox}>
                {result.relationships.map((rel, i) => (
                  <div key={i} style={styles.relRow}>
                    <span style={styles.relTag}>{rel.from}</span>
                    <span style={styles.relArrow}>→ {rel.type} →</span>
                    <span style={styles.relTag}>{rel.to}</span>
                    <span style={styles.confidence}>
                      {(rel.confidence * 100).toFixed(0)}%
                    </span>
                  </div>
                ))}
              </div>
            </>
          )}

          {/* Metadata */}
          <h2 style={styles.sectionTitle}>Metadata</h2>
          <div style={styles.metaBox}>
            {Object.entries(result.metadata || {}).map(([k, v]) => (
              <div key={k} style={styles.metaRow}>
                <span style={styles.metaKey}>{k}</span>
                <span style={styles.metaVal}>{String(v)}</span>
              </div>
            ))}
          </div>

          {/* Raw JSON */}
          <h2 style={styles.sectionTitle}>Raw JSON</h2>
          <pre style={styles.jsonBox}>
            {JSON.stringify(result, null, 2)}
          </pre>

        </div>
      )}
    </div>
  )
}

function EntityGroup({ label, items, color }) {
  if (!items || items.length === 0) return null
  return (
    <div style={{ ...styles.entityGroup, borderColor: color }}>
      <h3 style={{ ...styles.entityLabel, color }}>{label}</h3>
      {items.map((item, i) => (
        <div key={i} style={styles.entityItem}>
          <span style={styles.entityValue}>{item.value}</span>
          <div style={styles.confBarBg}>
            <div style={{
              ...styles.confBarFill,
              width: `${item.confidence * 100}%`,
              background: color
            }} />
          </div>
          <span style={styles.confText}>
            {(item.confidence * 100).toFixed(0)}%
          </span>
        </div>
      ))}
    </div>
  )
}

const styles = {
  container: { maxWidth: 800, margin: "0 auto", padding: "40px 20px", fontFamily: "system-ui, sans-serif", background: "#0f172a", minHeight: "100vh", color: "#e2e8f0" },
  title: { fontSize: 36, fontWeight: 800, margin: 0, color: "#f8fafc" },
  subtitle: { color: "#94a3b8", marginTop: 4, marginBottom: 32 },
  uploadBox: { background: "#1e293b", border: "2px dashed #334155", borderRadius: 12, padding: 32, textAlign: "center" },
  fileInput: { marginBottom: 16 },
  fileName: { color: "#94a3b8", margin: "8px 0 16px" },
  button: { background: "#3b82f6", color: "#fff", border: "none", borderRadius: 8, padding: "12px 28px", fontSize: 16, cursor: "pointer", fontWeight: 600 },
  errorBox: { background: "#450a0a", border: "1px solid #ef4444", borderRadius: 8, padding: 16, marginTop: 24, color: "#fca5a5" },
  resultContainer: { marginTop: 32 },
  statusRow: { display: "flex", alignItems: "center", gap: 16, marginBottom: 24 },
  badge: { padding: "4px 14px", borderRadius: 999, fontWeight: 700, fontSize: 13, color: "#fff" },
  docId: { color: "#64748b", fontSize: 13 },
  sectionTitle: { fontSize: 18, fontWeight: 700, color: "#cbd5e1", margin: "24px 0 12px" },
  cardGrid: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 },
  entityGroup: { background: "#1e293b", border: "1px solid", borderRadius: 10, padding: 16 },
  entityLabel: { margin: "0 0 12px", fontSize: 14, fontWeight: 700 },
  entityItem: { display: "flex", alignItems: "center", gap: 8, marginBottom: 8 },
  entityValue: { flex: 1, fontSize: 14 },
  confBarBg: { width: 60, height: 6, background: "#334155", borderRadius: 999 },
  confBarFill: { height: 6, borderRadius: 999 },
  confText: { fontSize: 12, color: "#64748b", width: 30 },
  relBox: { background: "#1e293b", borderRadius: 10, padding: 16 },
  relRow: { display: "flex", alignItems: "center", gap: 12, marginBottom: 8 },
  relTag: { background: "#334155", padding: "4px 10px", borderRadius: 6, fontSize: 13 },
  relArrow: { color: "#64748b", fontSize: 13 },
  confidence: { marginLeft: "auto", color: "#64748b", fontSize: 12 },
  metaBox: { background: "#1e293b", borderRadius: 10, padding: 16 },
  metaRow: { display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #334155" },
  metaKey: { color: "#64748b", fontSize: 13 },
  metaVal: { color: "#e2e8f0", fontSize: 13 },
  jsonBox: { background: "#1e293b", borderRadius: 10, padding: 20, fontSize: 12, overflowX: "auto", color: "#94a3b8", lineHeight: 1.6 },
}