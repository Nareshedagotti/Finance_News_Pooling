import axios from 'axios'

// Configure API base via env; fallback to local dev
const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000'

export async function fetchArticles({ limit = 50, skip = 0 } = {}) {
  const url = `${API_BASE}/articles?limit=${limit}&skip=${skip}`
  const res = await axios.get(url)
  return res.data
}

export async function searchArticles(q, { limit = 50, skip = 0 } = {}) {
  const url = `${API_BASE}/articles/search?q=${encodeURIComponent(q)}&limit=${limit}&skip=${skip}`
  const res = await axios.get(url)
  return res.data
}
