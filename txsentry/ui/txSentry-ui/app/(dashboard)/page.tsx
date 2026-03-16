'use client'

import { useState, useEffect } from 'react'
import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { fetchOverview } from '@/lib/api'
import { Shield, Brain, Search, Zap, BarChart3, Network } from 'lucide-react'

const techStack = [
  'Python', 'PySpark', 'LightGBM', 'SHAP', 'NetworkX',
  'LangGraph', 'FastMCP', 'FastAPI', 'Kafka', 'MLflow', 'Docker'
]

const metricIcons = [Zap, Shield, BarChart3, Brain, Network, Search]

export default function OverviewPage() {
  const [data, setData] = useState<any>({ keyMetrics: [], actionDistribution: [] })

  useEffect(() => {
    fetchOverview().then(setData)
  }, [])

  // Fallback if API isn't running
  const metrics = data.keyMetrics?.length > 0 ? data.keyMetrics : [
    { label: 'Transactions Processed', value: '10.1M' },
    { label: 'Fraud Detected', value: '597K' },
    { label: 'Precision @Budget', value: '99.5%' },
    { label: 'PR-AUC', value: '0.994' },
    { label: 'Fraud Scenarios', value: '6' },
    { label: 'Investigation Tools', value: '11' },
  ]

  return (
    <div className="space-y-8">
      {/* Hero */}
      <div className="text-center py-8">
        <h1 className="text-4xl font-bold text-slate-100 mb-2">
          🛡️ TxSentry
        </h1>
        <p className="text-xl text-slate-300 mb-4">
          Real-Time Payment Risk Detection & Autonomous Investigation Platform
        </p>
        <p className="text-slate-400 max-w-2xl mx-auto">
          TxSentry processes 10M+ transactions through a two-layer architecture: ML models score every
          transaction in real-time, then an AI agent autonomously investigates flagged alerts using 11 specialized tools.
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        {metrics.map((m: any, i: number) => {
          const Icon = metricIcons[i % metricIcons.length]
          return (
            <Card key={m.label} className="bg-slate-800/50 border-slate-700/50">
              <CardContent className="pt-6 text-center">
                <Icon className="h-6 w-6 text-blue-400 mx-auto mb-2" />
                <p className="text-2xl font-bold text-slate-100">{m.value}</p>
                <p className="text-xs text-slate-400 mt-1">{m.label}</p>
              </CardContent>
            </Card>
          )
        })}
      </div>

      {/* Architecture */}
      <Card className="bg-slate-800/50 border-slate-700/50">
        <CardContent className="pt-6">
          <h2 className="text-lg font-semibold text-slate-100 mb-6 text-center">Two-Layer Architecture</h2>

          {/* Detection Layer */}
          <div className="mb-8">
            <h3 className="text-sm font-medium text-blue-400 mb-3 uppercase tracking-wider">Layer 1 — Detection</h3>
            <div className="flex flex-wrap items-center gap-2 justify-center">
              {['Transaction Stream', 'PySpark Features', 'LightGBM + IsoForest + Graph', 'Fusion Engine', 'Action Decision'].map((step, i) => (
                <div key={step} className="flex items-center gap-2">
                  <div className="px-3 py-2 bg-blue-500/10 border border-blue-500/30 rounded-lg text-sm text-blue-300">
                    {step}
                  </div>
                  {i < 4 && <span className="text-slate-600">→</span>}
                </div>
              ))}
            </div>
          </div>

          {/* Investigation Layer */}
          <div>
            <h3 className="text-sm font-medium text-purple-400 mb-3 uppercase tracking-wider">Layer 2 — Investigation</h3>
            <div className="flex flex-wrap items-center gap-2 justify-center">
              {['Flagged Alert', 'Triage', 'Plan', 'Investigate (tool loop)', 'Synthesize', 'Case Memo'].map((step, i) => (
                <div key={step} className="flex items-center gap-2">
                  <div className="px-3 py-2 bg-purple-500/10 border border-purple-500/30 rounded-lg text-sm text-purple-300">
                    {step}
                  </div>
                  {i < 5 && <span className="text-slate-600">→</span>}
                </div>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Tech Stack */}
      <div className="text-center">
        <h2 className="text-sm font-medium text-slate-400 mb-3 uppercase tracking-wider">Tech Stack</h2>
        <div className="flex flex-wrap justify-center gap-2">
          {techStack.map((tech) => (
            <Badge key={tech} variant="outline" className="bg-slate-800/50 text-slate-300 border-slate-600 px-3 py-1">
              {tech}
            </Badge>
          ))}
        </div>
      </div>
    </div>
  )
}