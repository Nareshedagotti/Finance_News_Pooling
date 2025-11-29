import React from 'react'

function timeAgo(iso) {
  if (!iso) return ''
  const t = new Date(iso)
  const diff = Math.floor((Date.now() - t.getTime()) / 1000)
  if (diff < 60) return `${diff}s ago`
  const m = Math.floor(diff / 60)
  if (m < 60) return `${m}m ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ago`
  const d = Math.floor(h / 24)
  return `${d}d ago`
}

export default function ArticleCard({ item }) {
  const {
    title,
    summary,
    ui_recommendation,
    sentiment,
    category,
    tags = [],
    source,
    original_url,
    published_at,
  } = item

  const sentimentClass = (sentiment?.label || 'neutral')
  return (
    <div className="card">
      <div className="card-header">
        <a className="title" href={original_url} target="_blank" rel="noreferrer">
          {title}
        </a>
        <div className="meta">
          <span className="chip source">{source || 'Unknown'}</span>
          {published_at ? <span className="muted">· {timeAgo(published_at)}</span> : null}
          {category ? <span className="chip">{category}</span> : null}
          {sentiment?.label ? (
            <span className={`chip sentiment ${sentimentClass}`}>
              {sentiment.label}{sentiment.score != null ? ` ${Math.round(sentiment.score * 100)}%` : ''}
            </span>
          ) : null}
        </div>
      </div>

      {summary ? <p className="summary">{summary}</p> : null}

      {ui_recommendation ? (
        <div className="recommendation">
          <strong>Why it matters:</strong> {ui_recommendation}
        </div>
      ) : null}

      {tags?.length ? (
        <div className="tags">
          {tags.slice(0, 8).map((t, i) => (
            <span key={i} className="tag">#{t}</span>
          ))}
        </div>
      ) : null}

      <div className="actions">
        <a className="btn" href={original_url} target="_blank" rel="noreferrer">
          Read at Source ↗
        </a>
      </div>
    </div>
  )
}
