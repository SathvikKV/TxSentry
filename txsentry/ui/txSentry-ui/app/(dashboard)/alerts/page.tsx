'use client'

import { useState, useEffect, useMemo } from 'react'
import Link from 'next/link'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '@/components/ui/select'
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table'
import { fetchAlerts, fetchActionDistribution } from '@/lib/api'
import { Search, Filter, Check, X, AlertTriangle, Gauge } from 'lucide-react'
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts'

type RiskBand = 'CRITICAL' | 'HIGH' | 'ELEVATED' | 'MEDIUM' | 'LOW'
type Action = 'BLOCK' | 'QUEUE_FOR_REVIEW' | 'STEP_UP_AUTH' | 'ALLOW_WITH_MONITORING' | 'ALLOW'

interface Alert {
  alertId: string; timestamp: string; accountId: string; amount: number
  riskScore: number; riskBand: RiskBand; action: Action; isFraud: boolean
}

export default function AlertQueuePage() {
  const [alerts, setAlerts] = useState<Alert[]>([])
  const [actionDist, setActionDist] = useState<any[]>([])
  const [riskBandFilter, setRiskBandFilter] = useState<string>('all')
  const [actionFilter, setActionFilter] = useState<string>('all')
  const [searchQuery, setSearchQuery] = useState('')
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    Promise.all([
      fetchAlerts({ limit: 100, riskBand: riskBandFilter, action: actionFilter, search: searchQuery }),
      fetchActionDistribution(),
    ]).then(([alertData, distData]) => {
      setAlerts(alertData.alerts || [])
      setActionDist(distData || [])
      setLoading(false)
    })
  }, [riskBandFilter, actionFilter, searchQuery])

  const stats = useMemo(() => {
    const total = alerts.length
    const fraudCount = alerts.filter(a => a.isFraud).length
    const avgRisk = alerts.reduce((acc, a) => acc + a.riskScore, 0) / total || 0
    const precision = total > 0 ? (fraudCount / total) * 100 : 0
    return { total, fraudCount, avgRisk, precision }
  }, [alerts])

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Alert Queue</h1>
        <p className="text-slate-400 mt-1">Review and investigate flagged transactions</p>
      </div>

      <Card className="bg-slate-800/50 border-slate-700/50">
        <CardContent className="pt-6">
          <div className="flex flex-wrap gap-4 items-center">
            <div className="flex items-center gap-2">
              <Filter className="h-4 w-4 text-slate-400" />
              <span className="text-sm text-slate-400">Filters:</span>
            </div>
            <Select value={riskBandFilter} onValueChange={setRiskBandFilter}>
              <SelectTrigger className="w-[160px] bg-slate-900/50 border-slate-700 text-slate-200">
                <SelectValue placeholder="Risk Band" />
              </SelectTrigger>
              <SelectContent className="bg-slate-800 border-slate-700">
                <SelectItem value="all">All Risk Bands</SelectItem>
                <SelectItem value="CRITICAL">Critical</SelectItem>
                <SelectItem value="HIGH">High</SelectItem>
                <SelectItem value="ELEVATED">Elevated</SelectItem>
                <SelectItem value="MEDIUM">Medium</SelectItem>
                <SelectItem value="LOW">Low</SelectItem>
              </SelectContent>
            </Select>
            <Select value={actionFilter} onValueChange={setActionFilter}>
              <SelectTrigger className="w-[200px] bg-slate-900/50 border-slate-700 text-slate-200">
                <SelectValue placeholder="Action" />
              </SelectTrigger>
              <SelectContent className="bg-slate-800 border-slate-700">
                <SelectItem value="all">All Actions</SelectItem>
                <SelectItem value="BLOCK">Block</SelectItem>
                <SelectItem value="QUEUE_FOR_REVIEW">Queue for Review</SelectItem>
                <SelectItem value="STEP_UP_AUTH">Step-Up Auth</SelectItem>
                <SelectItem value="ALLOW_WITH_MONITORING">Allow w/ Monitoring</SelectItem>
                <SelectItem value="ALLOW">Allow</SelectItem>
              </SelectContent>
            </Select>
            <div className="relative flex-1 min-w-[200px]">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-slate-500" />
              <Input
                placeholder="Search by Account ID..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-10 bg-slate-900/50 border-slate-700 text-slate-200 placeholder:text-slate-500"
              />
            </div>
          </div>
        </CardContent>
      </Card>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card className="bg-slate-800/50 border-slate-700/50">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-slate-400">Total Alerts</p>
                <p className="text-2xl font-bold text-slate-100">{stats.total}</p>
              </div>
              <AlertTriangle className="h-8 w-8 text-amber-500/50" />
            </div>
          </CardContent>
        </Card>
        <Card className="bg-slate-800/50 border-slate-700/50">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-slate-400">Fraud in Selection</p>
                <p className="text-2xl font-bold text-red-400">{stats.fraudCount}</p>
              </div>
              <X className="h-8 w-8 text-red-500/50" />
            </div>
          </CardContent>
        </Card>
        <Card className="bg-slate-800/50 border-slate-700/50">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-slate-400">Avg Risk Score</p>
                <p className="text-2xl font-bold text-slate-100">{stats.avgRisk.toFixed(2)}</p>
              </div>
              <Gauge className="h-8 w-8 text-blue-500/50" />
            </div>
          </CardContent>
        </Card>
        <Card className="bg-slate-800/50 border-slate-700/50">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-slate-400">Precision</p>
                <p className="text-2xl font-bold text-green-400">{stats.precision.toFixed(1)}%</p>
              </div>
              <Check className="h-8 w-8 text-green-500/50" />
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        <div className="lg:col-span-3">
          <Card className="bg-slate-800/50 border-slate-700/50">
            <CardHeader>
              <CardTitle className="text-slate-100 text-base">
                {loading ? 'Loading...' : `Alerts (${alerts.length})`}
              </CardTitle>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow className="border-slate-700 hover:bg-transparent">
                    <TableHead className="text-slate-400">Alert ID</TableHead>
                    <TableHead className="text-slate-400">Timestamp</TableHead>
                    <TableHead className="text-slate-400">Account ID</TableHead>
                    <TableHead className="text-slate-400 text-right">Amount</TableHead>
                    <TableHead className="text-slate-400">Risk Score</TableHead>
                    <TableHead className="text-slate-400">Risk Band</TableHead>
                    <TableHead className="text-slate-400">Action</TableHead>
                    <TableHead className="text-slate-400 text-center">Fraud</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {alerts.map((alert) => (
                    <TableRow key={alert.alertId} className="border-slate-700/50 hover:bg-slate-700/30 cursor-pointer transition-colors">
                      <TableCell>
                        <Link href="/case" className="font-mono text-sm text-blue-400 hover:underline">
                          {alert.alertId}
                        </Link>
                      </TableCell>
                      <TableCell className="text-slate-300 text-sm">{alert.timestamp}</TableCell>
                      <TableCell className="font-mono text-sm text-slate-300">{alert.accountId}</TableCell>
                      <TableCell className="text-right text-slate-300">${alert.amount.toLocaleString()}</TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <div className="w-16 h-2 bg-slate-700 rounded-full overflow-hidden">
                            <div className={`h-full ${alert.riskScore >= 0.8 ? 'bg-red-500' : alert.riskScore >= 0.6 ? 'bg-amber-500' : alert.riskScore >= 0.4 ? 'bg-blue-500' : 'bg-green-500'} transition-all`} style={{ width: `${alert.riskScore * 100}%` }} />
                          </div>
                          <span className="text-xs font-mono text-slate-300">{alert.riskScore.toFixed(2)}</span>
                        </div>
                      </TableCell>
                      <TableCell>
                        <RiskBandBadge band={alert.riskBand} />
                      </TableCell>
                      <TableCell>
                        <ActionBadge action={alert.action} />
                      </TableCell>
                      <TableCell className="text-center">
                        {alert.isFraud ? <Check className="h-4 w-4 text-red-400 mx-auto" /> : <X className="h-4 w-4 text-slate-500 mx-auto" />}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </div>

        <div className="lg:col-span-1">
          <Card className="bg-slate-800/50 border-slate-700/50">
            <CardHeader>
              <CardTitle className="text-slate-100 text-base">Action Distribution</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="h-[200px]">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie data={actionDist} cx="50%" cy="50%" innerRadius={40} outerRadius={70} paddingAngle={2} dataKey="count">
                      {actionDist.map((entry, i) => <Cell key={i} fill={entry.color} />)}
                    </Pie>
                    <Tooltip contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px', color: '#f1f5f9' }} formatter={(v: number) => v.toLocaleString()} />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="space-y-2 mt-4">
                {actionDist.map((item) => (
                  <div key={item.action} className="flex items-center justify-between text-sm">
                    <div className="flex items-center gap-2">
                      <div className="w-3 h-3 rounded-full" style={{ backgroundColor: item.color }} />
                      <span className="text-slate-400 text-xs">{item.action}</span>
                    </div>
                    <span className="text-slate-300 text-xs font-mono">{(item.count / 1000000).toFixed(2)}M</span>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  )
}

function RiskBandBadge({ band }: { band: RiskBand }) {
  const c: Record<RiskBand, string> = {
    CRITICAL: 'bg-red-500/20 text-red-400 border-red-500/50',
    HIGH: 'bg-orange-500/20 text-orange-400 border-orange-500/50',
    ELEVATED: 'bg-blue-500/20 text-blue-400 border-blue-500/50',
    MEDIUM: 'bg-purple-500/20 text-purple-400 border-purple-500/50',
    LOW: 'bg-green-500/20 text-green-400 border-green-500/50',
  }
  return <span className={`text-xs font-medium px-2 py-1 rounded border ${c[band]}`}>{band}</span>
}

function ActionBadge({ action }: { action: Action }) {
  const c: Record<Action, string> = {
    BLOCK: 'bg-red-500/20 text-red-400 border-red-500/50',
    QUEUE_FOR_REVIEW: 'bg-amber-500/20 text-amber-400 border-amber-500/50',
    STEP_UP_AUTH: 'bg-blue-500/20 text-blue-400 border-blue-500/50',
    ALLOW_WITH_MONITORING: 'bg-purple-500/20 text-purple-400 border-purple-500/50',
    ALLOW: 'bg-green-500/20 text-green-400 border-green-500/50',
  }
  const labels: Record<Action, string> = { BLOCK: 'BLOCK', QUEUE_FOR_REVIEW: 'REVIEW', STEP_UP_AUTH: 'STEP-UP', ALLOW_WITH_MONITORING: 'MONITOR', ALLOW: 'ALLOW' }
  return <span className={`text-xs font-medium px-2 py-1 rounded border ${c[action]}`}>{labels[action]}</span>
}