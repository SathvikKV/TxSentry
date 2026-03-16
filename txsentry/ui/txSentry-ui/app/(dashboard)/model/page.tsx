'use client'

import { useState, useEffect } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { fetchModelMetrics, fetchShapImportance } from '@/lib/api'
import { BarChart, Bar, XAxis, YAxis, ResponsiveContainer, Tooltip } from 'recharts'
import { CheckCircle, AlertTriangle, TrendingUp } from 'lucide-react'

export default function ModelPerformancePage() {
  const [metrics, setMetrics] = useState<any>({})
  const [shapData, setShapData] = useState<any[]>([])

  useEffect(() => {
    fetchModelMetrics().then(setMetrics)
    fetchShapImportance().then(setShapData)
  }, [])

  const categoryColors: Record<string, string> = {
    velocity: '#3b82f6', graph: '#a855f7', behavioral: '#22c55e', metadata: '#f59e0b', other: '#6b7280',
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Model Performance</h1>
        <p className="text-slate-400 mt-1">Evaluation metrics and feature importance analysis</p>
      </div>

      {/* Split Comparison */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card className="bg-slate-800/50 border-slate-700/50 border-l-4 border-l-green-500">
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle className="text-slate-100 flex items-center gap-2">
                <CheckCircle className="h-5 w-5 text-green-400" />
                Temporal Split (Correct)
              </CardTitle>
              <Badge className="bg-green-500/20 text-green-400 border-green-500/50">Production Ready</Badge>
            </div>
            <CardDescription className="text-slate-400">Train: Jan-Sep 2024 | Test: Oct-Dec 2024</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-4">
              <div className="text-center p-4 bg-slate-900/50 rounded-lg">
                <p className="text-3xl font-bold text-green-400">{(metrics.prAucTemporal || 0).toFixed(3)}</p>
                <p className="text-sm text-slate-400">PR-AUC</p>
              </div>
              <div className="text-center p-4 bg-slate-900/50 rounded-lg">
                <p className="text-3xl font-bold text-green-400">{(metrics.rocAucTemporal || 0).toFixed(3)}</p>
                <p className="text-sm text-slate-400">ROC-AUC</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-slate-700/50 border-l-4 border-l-amber-500">
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle className="text-slate-100 flex items-center gap-2">
                <AlertTriangle className="h-5 w-5 text-amber-400" />
                Random Split (Naive)
              </CardTitle>
              <Badge className="bg-amber-500/20 text-amber-400 border-amber-500/50">Misleading</Badge>
            </div>
            <CardDescription className="text-slate-400">80/20 random stratified split</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-4">
              <div className="text-center p-4 bg-slate-900/50 rounded-lg">
                <p className="text-3xl font-bold text-amber-400">{(metrics.prAucRandom || 0).toFixed(3)}</p>
                <p className="text-sm text-slate-400">PR-AUC</p>
              </div>
              <div className="text-center p-4 bg-slate-900/50 rounded-lg">
                <p className="text-3xl font-bold text-amber-400">{(metrics.rocAucRandom || 0).toFixed(3)}</p>
                <p className="text-sm text-slate-400">ROC-AUC</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card className="bg-slate-900/30 border-slate-700/50">
        <CardContent className="pt-6">
          <p className="text-slate-300 text-sm leading-relaxed">
            <span className="font-semibold text-slate-100">Why temporal splits matter:</span>{' '}
            Random splits leak future fraud patterns into training data, causing inflated metrics.
            Temporal splits ensure evaluation on truly unseen future data.
          </p>
        </CardContent>
      </Card>

      {/* Precision & Recall */}
      <div className="grid grid-cols-2 gap-6">
        <Card className="bg-slate-800/50 border-slate-700/50">
          <CardContent className="pt-6 text-center">
            <p className="text-4xl font-bold text-green-400">{((metrics.precisionAtBudget || 0) * 100).toFixed(1)}%</p>
            <p className="text-sm text-slate-400 mt-1">Precision @ 500 alerts/day</p>
          </CardContent>
        </Card>
        <Card className="bg-slate-800/50 border-slate-700/50">
          <CardContent className="pt-6 text-center">
            <p className="text-4xl font-bold text-blue-400">{((metrics.recallAtBudget || 0) * 100).toFixed(1)}%</p>
            <p className="text-sm text-slate-400 mt-1">Recall @ 500 alerts/day</p>
          </CardContent>
        </Card>
      </div>

      {/* SHAP */}
      <Card className="bg-slate-800/50 border-slate-700/50">
        <CardHeader>
          <CardTitle className="text-slate-100 flex items-center gap-2">
            <TrendingUp className="h-5 w-5 text-blue-400" />
            SHAP Feature Importance
          </CardTitle>
          <CardDescription className="text-slate-400">Top features by mean |SHAP| value</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-4 mb-4">
            {Object.entries(categoryColors).filter(([k]) => k !== 'other').map(([cat, col]) => (
              <div key={cat} className="flex items-center gap-2">
                <div className="w-3 h-3 rounded-full" style={{ backgroundColor: col }} />
                <span className="text-xs text-slate-400 capitalize">{cat}</span>
              </div>
            ))}
          </div>
          <div className="h-[350px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={shapData} layout="vertical" margin={{ top: 0, right: 30, left: 120, bottom: 0 }}>
                <XAxis type="number" tick={{ fill: '#94a3b8', fontSize: 12 }} axisLine={{ stroke: '#334155' }} />
                <YAxis type="category" dataKey="feature" tick={{ fill: '#94a3b8', fontSize: 12 }} axisLine={{ stroke: '#334155' }} width={110} />
                <Tooltip contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px', color: '#f1f5f9' }} formatter={(v: number) => [v.toFixed(4), 'Mean |SHAP|']} />
                <Bar dataKey="importance" radius={[0, 4, 4, 0]}>
                  {shapData.map((entry, i) => (
                    <rect key={i} fill={categoryColors[entry.category] || '#6b7280'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}