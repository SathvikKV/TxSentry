/**
 * TxSentry API Client
 * 
 * Fetches real data from the Python FastAPI backend.
 * Falls back to mock data if the backend is unavailable.
 */

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

async function fetchAPI<T>(path: string, fallback: T): Promise<T> {
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      cache: 'no-store',
      headers: { 'Content-Type': 'application/json' },
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return await res.json()
  } catch (err) {
    console.warn(`API call failed for ${path}, using fallback:`, err)
    return fallback
  }
}

// --- Alert Queue ---
export async function fetchAlerts(params?: {
  limit?: number
  riskBand?: string
  action?: string
  search?: string
}) {
  const query = new URLSearchParams()
  if (params?.limit) query.set('limit', String(params.limit))
  if (params?.riskBand && params.riskBand !== 'all') query.set('risk_band', params.riskBand)
  if (params?.action && params.action !== 'all') query.set('action', params.action)
  if (params?.search) query.set('search', params.search)

  const qs = query.toString()
  return fetchAPI<{ alerts: any[]; total: number }>(
    `/api/alerts${qs ? '?' + qs : ''}`,
    { alerts: [], total: 0 }
  )
}

export async function fetchActionDistribution() {
  return fetchAPI<any[]>('/api/alerts/distribution', [])
}

// --- Cases ---
export async function fetchCaseList() {
  return fetchAPI<string[]>('/api/cases', [])
}

export async function fetchCase(caseId: string) {
  return fetchAPI<any>(`/api/cases/${caseId}`, null)
}

// --- Model Performance ---
export async function fetchModelMetrics() {
  return fetchAPI<any>('/api/model/metrics', {})
}

export async function fetchShapImportance() {
  return fetchAPI<any[]>('/api/model/shap', [])
}

// --- Monitoring ---
export async function fetchMonitoring() {
  return fetchAPI<any>('/api/monitoring', {
    psiHeatmap: [],
    monthlyPrecision: [],
    monthlyFraudRate: [],
    retrainingRecommended: false,
    driftFeatures: [],
  })
}

// --- Graph Explorer ---
export async function fetchGraphData(accountId: string) {
  return fetchAPI<any>(`/api/graph/${accountId}`, null)
}

// --- Overview ---
export async function fetchOverview() {
  return fetchAPI<any>('/api/overview', {
    keyMetrics: [],
    actionDistribution: [],
  })
}