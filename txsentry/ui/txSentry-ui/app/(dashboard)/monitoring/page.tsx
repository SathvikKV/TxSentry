'use client'

import { useState, useEffect } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { fetchMonitoring } from '@/lib/api'
import { LineChart, Line, XAxis, YAxis, ResponsiveContainer, Tooltip, ReferenceLine } from 'recharts'
import { AlertTriangle, Activity, TrendingUp, RefreshCw } from 'lucide-react'

export default function MonitoringPage() {
  const [data, setData] = useState<any>({
    psiHeatmap: [], monthlyPrecision: [], monthlyFraudRate: [],
    retrainingRecommended: false, driftFeatures: [],
  })

  useEffect(() => {
    fetchMonitoring().then(setData)
  }, [])

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Monitoring & Drift</h1>
        <p className="text-slate-400 mt-1">Track model stability and feature distribution shifts</p>
      </div>

      {data.retrainingRecommended && (
        <Card className="bg-red-500/10 border-red-500/30 border-2">
          <CardContent className="pt-6">
            <div className="flex items-start gap-4">
              <div className="w-10 h-10 rounded-full bg-red-500/20 flex items-center justify-center shrink-0">
                <AlertTriangle className="h-5 w-5 text-red-400" />
              </div>
              <div>
                <h3 className="text-lg font-semibold text-red-400">Retraining Recommended</h3>
                <p className="text-slate-300 mt-1">
                  PSI &gt; 0.2 on {data.driftFeatures?.length || 0}/{data.psiHeatmap?.length || 0} monitored features.
                  Significant drift detected across production months.
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* PSI Heatmap */}
      <Card className="bg-slate-800/50 border-slate-700/50">
        <CardHeader>
          <CardTitle className="text-slate-100 flex items-center gap-2">
            <Activity className="h-5 w-5 text-blue-400" />
            PSI Heatmap
          </CardTitle>
          <CardDescription className="text-slate-400">Feature distribution shift from training data</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex gap-4 mb-4">
            <div className="flex items-center gap-2"><div className="w-4 h-4 rounded bg-green-500" /><span className="text-xs text-slate-400">{'< 0.1 Stable'}</span></div>
            <div className="flex items-center gap-2"><div className="w-4 h-4 rounded bg-amber-500" /><span className="text-xs text-slate-400">0.1-0.2 Moderate</span></div>
            <div className="flex items-center gap-2"><div className="w-4 h-4 rounded bg-red-500" /><span className="text-xs text-slate-400">{'> 0.2 Significant'}</span></div>
          </div>
          <table className="w-full">
            <thead>
              <tr>
                <th className="text-left text-sm font-medium text-slate-400 pb-3 pr-4">Feature</th>
                <th className="text-center text-sm font-medium text-slate-400 pb-3 px-4">Oct 2024</th>
                <th className="text-center text-sm font-medium text-slate-400 pb-3 px-4">Nov 2024</th>
                <th className="text-center text-sm font-medium text-slate-400 pb-3 px-4">Dec 2024</th>
                <th className="text-center text-sm font-medium text-slate-400 pb-3 pl-4">Jan 2025</th>
              </tr>
            </thead>
            <tbody>
              {(data.psiHeatmap || []).map((row: any) => (
                <tr key={row.feature}>
                  <td className="py-2 pr-4"><span className="font-mono text-sm text-slate-300">{row.feature}</span></td>
                  {['oct', 'nov', 'dec', 'jan'].map((m) => (
                    <td key={m} className="py-2 px-4">
                      <PSICell value={row.values?.[m] || 0} />
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Monthly Precision */}
        <Card className="bg-slate-800/50 border-slate-700/50">
          <CardHeader>
            <CardTitle className="text-slate-100 flex items-center gap-2 text-base">
              <TrendingUp className="h-5 w-5 text-green-400" />
              Monthly Precision @500 Alerts
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-[280px]">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={data.monthlyPrecision || []} margin={{ top: 20, right: 20, left: 0, bottom: 20 }}>
                  <XAxis dataKey="month" tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={{ stroke: '#334155' }} />
                  <YAxis domain={[90, 100]} tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={{ stroke: '#334155' }} tickFormatter={(v) => `${v}%`} />
                  <Tooltip contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px', color: '#f1f5f9' }} formatter={(v: number) => [`${v.toFixed(1)}%`, 'Precision']} />
                  <ReferenceLine y={95} stroke="#f59e0b" strokeDasharray="3 3" />
                  <Line type="monotone" dataKey="precision" stroke="#22c55e" strokeWidth={2} dot={{ fill: '#22c55e', strokeWidth: 2, r: 4 }} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        {/* Fraud Rate */}
        <Card className="bg-slate-800/50 border-slate-700/50">
          <CardHeader>
            <CardTitle className="text-slate-100 flex items-center gap-2 text-base">
              <RefreshCw className="h-5 w-5 text-red-400" />
              Monthly Fraud Rate Trend
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-[280px]">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={data.monthlyFraudRate || []} margin={{ top: 20, right: 20, left: 0, bottom: 20 }}>
                  <XAxis dataKey="month" tick={{ fill: '#94a3b8', fontSize: 10 }} axisLine={{ stroke: '#334155' }} interval={1} />
                  <YAxis domain={[0, 'auto']} tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={{ stroke: '#334155' }} tickFormatter={(v) => `${v}%`} />
                  <Tooltip contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px', color: '#f1f5f9' }} formatter={(v: number) => [`${v.toFixed(1)}%`, 'Fraud Rate']} />
                  <Line type="monotone" dataKey="rate" stroke="#ef4444" strokeWidth={2} dot={{ fill: '#ef4444', strokeWidth: 2, r: 3 }} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

function PSICell({ value }: { value: number }) {
  const bg = value < 0.1 ? 'bg-green-500' : value < 0.2 ? 'bg-amber-500' : 'bg-red-500'
  const text = value < 0.1 ? 'text-green-400' : value < 0.2 ? 'text-amber-400' : 'text-red-400'
  return (
    <div className="flex items-center justify-center">
      <div className={`w-16 h-10 rounded-lg ${bg} bg-opacity-20 flex items-center justify-center`}>
        <span className={`font-mono text-sm font-medium ${text}`}>{value.toFixed(2)}</span>
      </div>
    </div>
  )
}