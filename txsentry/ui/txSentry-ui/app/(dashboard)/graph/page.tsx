'use client'

import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { fetchGraphData } from '@/lib/api'
import { Search, User, Smartphone, Wallet, AlertTriangle, Network, Eye, Link2 } from 'lucide-react'

export default function GraphExplorerPage() {
  const [searchQuery, setSearchQuery] = useState('ACC_AML_152')
  const [graphData, setGraphData] = useState<any>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleSearch = async () => {
    if (!searchQuery.trim()) return
    setLoading(true)
    setError(null)
    const data = await fetchGraphData(searchQuery.trim())
    if (data?.error) {
      setError(data.error)
      setGraphData(null)
    } else {
      setGraphData(data)
    }
    setLoading(false)
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Graph Explorer</h1>
        <p className="text-slate-400 mt-1">Explore account neighborhoods and detect graph-based fraud patterns</p>
      </div>

      <Card className="bg-slate-800/50 border-slate-700/50">
        <CardContent className="pt-6">
          <div className="flex gap-4">
            <div className="relative flex-1">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-slate-500" />
              <Input
                placeholder="Enter Account ID (e.g., ACC_AML_152)"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
                className="pl-10 bg-slate-900/50 border-slate-700 text-slate-200 placeholder:text-slate-500"
              />
            </div>
            <Button onClick={handleSearch} disabled={loading} className="bg-blue-600 hover:bg-blue-700 text-white">
              <Eye className="h-4 w-4 mr-2" />
              {loading ? 'Loading...' : 'Explore'}
            </Button>
          </div>
        </CardContent>
      </Card>

      {error && (
        <Card className="bg-red-500/10 border-red-500/30">
          <CardContent className="pt-6">
            <p className="text-red-400">{error}</p>
          </CardContent>
        </Card>
      )}

      {graphData && (
        <>
          {/* Shared Device Alert */}
          {graphData.sharedDeviceAccounts?.length > 0 && (
            <Card className="bg-amber-500/10 border-amber-500/30 border-2">
              <CardContent className="pt-6">
                <div className="flex items-start gap-4">
                  <div className="w-10 h-10 rounded-full bg-amber-500/20 flex items-center justify-center shrink-0">
                    <AlertTriangle className="h-5 w-5 text-amber-400" />
                  </div>
                  <div>
                    <h3 className="text-lg font-semibold text-amber-400">Shared Device Detected</h3>
                    <p className="text-slate-300 mt-1">
                      Device shared with <span className="font-bold text-amber-300">{graphData.sharedDeviceAccounts.length} other accounts</span>.
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* Account Info */}
            <Card className="bg-slate-800/50 border-slate-700/50">
              <CardHeader>
                <CardTitle className="text-slate-100 flex items-center gap-2">
                  <User className="h-5 w-5 text-blue-400" />
                  Account Details
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="p-4 bg-slate-900/50 rounded-lg border border-slate-700/50">
                  <p className="font-mono text-lg text-blue-400 mb-3">{graphData.account?.id}</p>
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <p className="text-xs text-slate-500">Degree</p>
                      <p className="text-lg font-bold text-slate-200">{graphData.account?.degree || 0}</p>
                    </div>
                    <div>
                      <p className="text-xs text-slate-500">Risk Score</p>
                      <p className="text-lg font-bold text-red-400">{(graphData.account?.riskScore || 0).toFixed(3)}</p>
                    </div>
                    <div>
                      <p className="text-xs text-slate-500">Community</p>
                      <p className="text-sm font-mono text-slate-300">{graphData.account?.community}</p>
                    </div>
                    <div>
                      <p className="text-xs text-slate-500">Community Fraud</p>
                      <p className="text-lg font-bold text-amber-400">{((graphData.account?.communityFraudRate || 0) * 100).toFixed(0)}%</p>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Network placeholder */}
            <Card className="bg-slate-800/50 border-slate-700/50">
              <CardHeader>
                <CardTitle className="text-slate-100 flex items-center gap-2">
                  <Network className="h-5 w-5 text-purple-400" />
                  Network Summary
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="p-3 bg-slate-900/50 rounded-lg flex justify-between">
                  <span className="text-slate-400">Devices</span>
                  <span className="text-blue-400 font-bold">{graphData.connectedDevices?.length || 0}</span>
                </div>
                <div className="p-3 bg-slate-900/50 rounded-lg flex justify-between">
                  <span className="text-slate-400">Beneficiaries</span>
                  <span className="text-green-400 font-bold">{graphData.connectedBeneficiaries?.length || 0}</span>
                </div>
                <div className="p-3 bg-slate-900/50 rounded-lg flex justify-between">
                  <span className="text-slate-400">IPs</span>
                  <span className="text-slate-300 font-bold">{graphData.connectedIPs?.length || 0}</span>
                </div>
                <div className="p-3 bg-red-500/10 rounded-lg border border-red-500/30 flex justify-between">
                  <span className="text-slate-400">Shared Device Accounts</span>
                  <span className="text-red-400 font-bold">{graphData.sharedDeviceAccounts?.length || 0}</span>
                </div>
              </CardContent>
            </Card>

            {/* Pattern Detection */}
            <Card className="bg-slate-800/50 border-slate-700/50">
              <CardHeader>
                <CardTitle className="text-slate-100 flex items-center gap-2">
                  <Link2 className="h-5 w-5 text-amber-400" />
                  Pattern Detection
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {(graphData.patterns || []).length === 0 ? (
                  <p className="text-slate-500 text-sm">No patterns detected</p>
                ) : (
                  (graphData.patterns || []).map((p: any) => (
                    <div key={p.pattern} className="p-4 bg-slate-900/50 rounded-lg border border-slate-700/50">
                      <div className="flex items-center justify-between mb-2">
                        <span className="font-mono text-sm font-medium text-amber-400">{p.pattern}</span>
                        <Badge className={p.confidence >= 0.9 ? 'bg-red-500/20 text-red-400 border-red-500/50' : 'bg-amber-500/20 text-amber-400 border-amber-500/50'}>
                          {(p.confidence * 100).toFixed(0)}%
                        </Badge>
                      </div>
                      <p className="text-xs text-slate-400">{p.evidence}</p>
                      <div className="mt-2 h-1.5 bg-slate-700 rounded-full overflow-hidden">
                        <div className={`h-full ${p.confidence >= 0.9 ? 'bg-red-500' : 'bg-amber-500'}`} style={{ width: `${p.confidence * 100}%` }} />
                      </div>
                    </div>
                  ))
                )}
              </CardContent>
            </Card>
          </div>

          {/* Connected Entities */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <Card className="bg-slate-800/50 border-slate-700/50">
              <CardHeader className="pb-3">
                <CardTitle className="text-slate-100 text-base flex items-center gap-2">
                  <Smartphone className="h-4 w-4 text-blue-400" />
                  Devices ({graphData.connectedDevices?.length || 0})
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2">
                {(graphData.connectedDevices || []).map((d: any) => (
                  <div key={d.id} className={`p-3 rounded-lg border ${d.shared ? 'bg-red-500/10 border-red-500/30' : 'bg-slate-900/50 border-slate-700/50'}`}>
                    <div className="flex items-center justify-between">
                      <span className="font-mono text-sm text-slate-300">{d.id}</span>
                      {d.shared && <Badge className="bg-red-500/20 text-red-400 border-red-500/50 text-[10px]">Shared ({d.sharedWith})</Badge>}
                    </div>
                  </div>
                ))}
              </CardContent>
            </Card>

            <Card className="bg-slate-800/50 border-slate-700/50">
              <CardHeader className="pb-3">
                <CardTitle className="text-slate-100 text-base flex items-center gap-2">
                  <Wallet className="h-4 w-4 text-green-400" />
                  Beneficiaries ({graphData.connectedBeneficiaries?.length || 0})
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2">
                {(graphData.connectedBeneficiaries || []).slice(0, 8).map((b: any) => (
                  <div key={b.id} className="p-3 rounded-lg border bg-slate-900/50 border-slate-700/50">
                    <span className="font-mono text-sm text-slate-300">{b.id}</span>
                  </div>
                ))}
              </CardContent>
            </Card>

            <Card className="bg-slate-800/50 border-slate-700/50 border-l-4 border-l-red-500">
              <CardHeader className="pb-3">
                <CardTitle className="text-slate-100 text-base flex items-center gap-2">
                  <AlertTriangle className="h-4 w-4 text-red-400" />
                  Shared Device Accounts
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2">
                {(graphData.sharedDeviceAccounts || []).map((a: any) => (
                  <div key={a.id} className="p-3 rounded-lg border bg-red-500/10 border-red-500/30">
                    <div className="flex items-center justify-between">
                      <span className="font-mono text-sm text-slate-300">{a.id}</span>
                      <Badge className="bg-red-500 text-white text-[10px]">Flagged</Badge>
                    </div>
                  </div>
                ))}
                {(graphData.sharedDeviceAccounts || []).length === 0 && (
                  <p className="text-slate-500 text-sm">No shared devices detected</p>
                )}
              </CardContent>
            </Card>
          </div>
        </>
      )}
    </div>
  )
}