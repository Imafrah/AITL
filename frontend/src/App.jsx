import { useState, useRef } from "react"
import axios from "axios"
import "./index.css"

const API_BASE =
  import.meta.env.VITE_API_BASE_URL ??
  (import.meta.env.PROD ? "https://aitl.onrender.com" : "")

export default function App() {
  const [file, setFile] = useState(null)
  const [result, setResult] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [responseFormat, setResponseFormat] = useState("json")
  // For drag and drop visuals
  const [dragActive, setDragActive] = useState(false)
  const fileInputRef = useRef(null)

  const handleDrag = (e) => {
    e.preventDefault()
    e.stopPropagation()
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true)
    } else if (e.type === "dragleave") {
      setDragActive(false)
    }
  }

  const handleDrop = (e) => {
    e.preventDefault()
    e.stopPropagation()
    setDragActive(false)
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      setFile(e.dataTransfer.files[0])
    }
  }

  const handleChange = (e) => {
    e.preventDefault()
    if (e.target.files && e.target.files[0]) {
      setFile(e.target.files[0])
    }
  }

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
    <div className="app-wrapper animate-fade-in">
      <header className="header">
        <h1 className="title">AITL Engine</h1>
        <p className="subtitle">Universal File Intelligence & Next-Gen Data Cleaning</p>
      </header>

      <section className="glass-panel">
        <div 
          className={`upload-zone ${dragActive ? 'drag-active' : ''}`}
          onDragEnter={handleDrag}
          onDragLeave={handleDrag}
          onDragOver={handleDrag}
          onDrop={handleDrop}
          onClick={() => fileInputRef.current?.click()}
        >
          <input
            ref={fileInputRef}
            type="file"
            accept=".txt,.csv,.pdf"
            onChange={handleChange}
            className="file-input-hidden"
          />
          <div className="upload-icon">✦</div>
          <h3 style={{ fontSize: '1.2rem', marginBottom: '0.5rem' }}>
            {file ? file.name : 'Drag & Drop Dataset'}
          </h3>
          <p style={{ color: 'var(--text-secondary)' }}>
            {file ? "File ready for extraction" : "Supports .CSV, .TXT, .PDF"}
          </p>
        </div>

        <div className="upload-controls">
          <div>
            <label style={{ display: 'none' }}>Response</label>
            <select
              value={responseFormat}
              onChange={(e) => setResponseFormat(e.target.value)}
              className="styled-select"
            >
              <option value="json">Format: JSON (Full Envelope)</option>
              <option value="table">Format: JSON + Flattened Table</option>
              <option value="dashboard">Format: Dashboard Analytics</option>
              <option value="csv">Action: Direct CSV Download</option>
            </select>
          </div>
          <button
            onClick={handleUpload}
            disabled={!file || loading}
            className="btn-primary"
          >
            {loading ? (
              <><span className="loader-ring"></span> Processing...</>
            ) : "Extract Intelligence ⚡"}
          </button>
        </div>

        {error && (
          <div className="error-msg animate-fade-in">
            <span>⚠️</span> {error}
          </div>
        )}
      </section>

      {result && typeof result === "object" && !Array.isArray(result) && hasUniversalEnvelope && (
        <section className="results-container animate-fade-in" style={{ marginTop: '3rem' }}>
          
          <div className="results-header">
            <div>
              <h2 className="title" style={{ fontSize: '2rem', marginBottom: '0.5rem' }}>Intelligence Report</h2>
              <span style={{ color: 'var(--text-secondary)' }}>Generated for ID: {result.document_id}</span>
            </div>
            <div className="status-badges">
              <span className={`badge ${
                result.status === "success" ? "success"
                : result.status === "partial" ? "warning" : "error"
              }`}>
                {String(result.status ?? "unknown").toUpperCase()}
              </span>
              <span className="badge info">TYPE: {result.document_type}</span>
            </div>
          </div>

          <h2 className="section-title">Cleaned Data ({rows.length} records)</h2>
          <DataPreview rows={rows} />

          {result.table && (
            <>
              <h2 className="section-title">Flattened Dimensions</h2>
              <DataPreview rows={result.table} />
            </>
          )}

          <h2 className="section-title">Execution Metadata</h2>
          <div className="glass-panel" style={{ padding: '0', overflow: 'hidden' }}>
            <table className="premium-table">
              <tbody>
                {Object.entries(result.metadata || {}).map(([k, v]) => (
                  <tr key={k}>
                    <td style={{ width: '30%', color: 'var(--accent-cyan)' }}>{k}</td>
                    <td>{typeof v === "object" && v !== null ? JSON.stringify(v) : String(v)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <h2 className="section-title">Raw JSON Schema Output</h2>
          <div style={{ display: "flex", gap: '1rem', marginBottom: '1.5rem' }}>
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
              className="btn-secondary"
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
              className="btn-secondary"
            >
              ⬇ Download TOML
            </button>
          </div>
          <pre className="code-block">
            {JSON.stringify(result, null, 2)}
          </pre>

        </section>
      )}
    </div>
  )
}

function DataPreview({ rows }) {
  if (!rows?.length) {
    return <p style={{ color: "var(--text-secondary)", fontStyle: 'italic' }}>No rows to display.</p>
  }
  const keys = [...new Set(rows.flatMap((r) => Object.keys(r)))]
  
  return (
    <div className="table-wrapper animate-fade-in">
      <table className="premium-table">
        <thead>
          <tr>
            {keys.map((k) => (
              <th key={k}>{k}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              {keys.map((k) => {
                let val = r[k]
                if (val === null || val === undefined) val = ""
                else val = String(val)
                return <td key={k}>{val}</td>
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
