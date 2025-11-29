import React, { useEffect, useState, useMemo } from 'react'
import ArticleCard from './components/ArticleCard.jsx'
import { fetchArticles, searchArticles } from './lib/api.js'

export default function App() {
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [q, setQ] = useState('')
  const [poll, setPoll] = useState(true)

  const load = async () => {
    try {
      setLoading(true)
      const data = q ? await searchArticles(q) : await fetchArticles()
      setItems(data || [])
      setError('')
    } catch (e) {
      setError(e?.message || 'Failed to fetch')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
  }, [q])

  // Auto-refresh every 30s
  useEffect(() => {
    if (!poll) return
    const id = setInterval(load, 30000)
    return () => clearInterval(id)
  }, [poll, q])

  const count = items?.length || 0

  return (
    <div className="container">
      <header className="topbar">
        <h1>Financial News</h1>
        <div className="controls">
          <input
            type="search"
            placeholder="Search title/summary/tags…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
          <label className="poll">
            <input type="checkbox" checked={poll} onChange={() => setPoll(p => !p)} />
            Auto-refresh
          </label>
        </div>
      </header>

      {loading && <div className="status">Loading…</div>}
      {error && <div className="status error">{error}</div>}

      {!loading && !error && (
        <>
          <div className="count">{count} articles</div>
          <div className="grid">
            {items.map((it, idx) => (
              <ArticleCard key={it.id || it.article_id || idx} item={it} />
            ))}
          </div>
        </>
      )}

      <footer className="foot">
        <span>Auto-updates every 30s · Data expires 36h after ingest</span>
      </footer>
    </div>
  )
}
