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
      setError(err.response?.data?.detail || "System Error: Something went wrong.")
    } finally {
      setLoading(false)
    }
  }

  const rows = result?.data
  const hasUniversalEnvelope = Boolean(result?.document_id && Array.isArray(rows))

  return (
    <div className="app-wrapper">
      {/* Fake Mac OS Window Title Bar */}
      <div className="window-title-bar">
        <div className="window-close-btn"></div>
        <div className="window-title-bar-content">
          System Window
        </div>
      </div>
      
      <div className="window-body">
        <header>
          <h1 className="title">AITL Engine</h1>
          <p className="subtitle">System 7.0 // Next-Gen Data Processing</p>
        </header>

        <section>
          <div 
            className={`upload-zone ${dragActive ? 'drag-active' : ''}`}
            onDragEnter={handleDrag}
            onDragLeave={handleDrag}
            onDragOver={handleDrag}
            onDrop={handleDrop}
            onClick={() => fileInputRef.current?.click()}
            style={{ position: 'relative' }}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".txt,.csv,.pdf"
              onChange={handleChange}
              className="file-input-hidden"
            />
            <div className="upload-icon">💾</div>
            <h3 style={{ fontSize: '1.5rem', marginBottom: '0.5rem', fontWeight: 800 }}>
              {file ? file.name : 'INSERT DISK OR DROP FILE'}
            </h3>
            <p style={{ fontWeight: 600 }}>
              {file ? "Ready to compute" : "Supports .CSV, .TXT, .PDF formats"}
            </p>
          </div>

          <div className="upload-controls" style={{ marginTop: '2rem' }}>
            <div style={{ flex: 1 }}>
              <label style={{ display: 'none' }}>Response</label>
              <select
                value={responseFormat}
                onChange={(e) => setResponseFormat(e.target.value)}
                className="styled-select"
                style={{ width: '100%' }}
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
                <><span className="retro-loader">█</span> COMPUTING...</>
              ) : "EXECUTE"}
            </button>
          </div>

          {error && (
            <div className="error-msg" style={{ marginTop: '1.5rem' }}>
              <span>⚠️</span> {error}
            </div>
          )}
        </section>

        {result && typeof result === "object" && !Array.isArray(result) && hasUniversalEnvelope && (
          <section className="results-container" style={{ marginTop: '2rem', borderTop: '4px solid #000', paddingTop: '2rem' }}>
            
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '2rem', flexWrap: 'wrap', gap: '1rem' }}>
              <div>
                <h2 className="title" style={{ fontSize: '2rem', marginBottom: '0' }}>REPORT</h2>
                <strong style={{ display: 'block', fontSize: '0.9rem', marginTop: '0.5rem' }}>ID: {result.document_id}</strong>
              </div>
              <div className="status-badges">
                <div className="badge">
                  STATUS: {String(result.status ?? "UNKNOWN")}
                </div>
                <div className="badge">
                  TYPE: {result.document_type}
                </div>
              </div>
            </div>

            <h2 className="section-title">CLEANED DATA ({rows.length} records)</h2>
            <DataPreview rows={rows} />

            {result.table && (
              <>
                <h2 className="section-title">FLATTENED DIMENSIONS</h2>
                <DataPreview rows={result.table} />
              </>
            )}

            <h2 className="section-title">METADATA</h2>
            <div className="table-wrapper">
              <table className="retro-table">
                <tbody>
                  {Object.entries(result.metadata || {}).map(([k, v]) => (
                    <tr key={k}>
                      <th style={{ width: '30%', backgroundColor: '#fff' }}>{k}</th>
                      <td>{typeof v === "object" && v !== null ? JSON.stringify(v) : String(v)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <h2 className="section-title">RAW OUTPUT</h2>
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
                [ SAVE JSON ]
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
                    alert("System Error: Failed to download TOML")
                  }
                }}
                className="btn-secondary"
              >
                [ SAVE TOML ]
              </button>
            </div>
            <pre className="code-block">
              {JSON.stringify(result, null, 2)}
            </pre>

          </section>
        )}
      </div>
    </div>
  )
}

function DataPreview({ rows }) {
  if (!rows?.length) {
    return <p style={{ fontWeight: 'bold' }}>NO RECORDS FOUND.</p>
  }
  const keys = [...new Set(rows.flatMap((r) => Object.keys(r)))]
  
  return (
    <div className="table-wrapper">
      <table className="retro-table">
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
