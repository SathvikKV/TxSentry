'use client'

import { useState, useEffect } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { fetchCaseList, fetchCase } from '@/lib/api'
import { Download, Shield, CheckCircle, User, Smartphone, Building, Globe, Wallet, ChevronRight, Search } from 'lucide-react'

type Action = 'BLOCK' | 'QUEUE_FOR_REVIEW' | 'STEP_UP_AUTH' | 'ALLOW_WITH_MONITORING' | 'ALLOW'

export default function CaseInvestigationPage() {
  const [caseList, setCaseList] = useState<string[]>([])
  const [selectedCase, setSelectedCase] = useState<string>('')
  const [caseData, setCaseData] = useState<any>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetchCaseList().then((list) => {
      setCaseList(list)
      if (list.length > 0) setSelectedCase(list[0])
    })
  }, [])

  useEffect(() => {
    if (!selectedCase) return
    setLoading(true)
    fetchCase(selectedCase).then((data) => {
      setCaseData(data)
      setLoading(false)
    })
  }, [selectedCase])

  if (loading || !caseData) {
    return (
      <div className="flex items-center justify-center h-64">
        <p className="text-slate-400">Loading case data...</p>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Case selector */}
      <div className="flex items-center gap-4">
        <Select value={selectedCase} onValueChange={setSelectedCase}>
          <SelectTrigger className="w-[250px] bg-slate-900/50 border-slate-700 text-slate-200">
            <SelectValue placeholder="Select case" />
          </SelectTrigger>
          <SelectContent className="bg-slate-800 border-slate-700">
            {caseList.map((c) => (
              <SelectItem key={c} value={c}>{c}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <div className="flex items-center gap-3 mb-2">
            <h1 className="text-2xl font-bold text-slate-100">{caseData.caseId}</h1>
            <ActionBadge action={caseData.action} />
            <PriorityBadge priority={caseData.priority} />
          </div>
          <p className="text-slate-400">Alert: {caseData.alertId} | Account: {caseData.accountId}</p>
        </div>
        <div className="flex items-center gap-4">
          <div className="text-center">
            <span className="text-lg font-bold text-green-400">{caseData.confidence}%</span>
            <p className="text-xs text-slate-500">Confidence</p>
          </div>
          <Button className="bg-blue-600 hover:bg-blue-700 text-white">
            <Download className="h-4 w-4 mr-2" />
            Download Report
          </Button>
        </div>
      </div>

      {/* Executive Summary */}
      <Card className="bg-slate-800/50 border-slate-700/50">
        <CardHeader>
          <CardTitle className="text-slate-100 flex items-center gap-2">
            <Shield className="h-5 w-5 text-red-400" />
            Executive Summary
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          <p className="text-lg text-slate-200">{caseData.summary}</p>
          <div className="space-y-4">
            <RiskScoreBar label="Transaction Risk Score" score={caseData.riskScores?.txnRisk || 0} />
            <RiskScoreBar label="Behavioral Anomaly Score" score={caseData.riskScores?.anomaly || 0} />
            <RiskScoreBar label="Graph Risk Score" score={caseData.riskScores?.graph || 0} />
            <RiskScoreBar label="Final Fused Score" score={caseData.riskScores?.final || 0} thick />
          </div>
        </CardContent>
      </Card>

      {/* Reason Codes */}
      <Card className="bg-slate-800/50 border-slate-700/50">
        <CardHeader>
          <CardTitle className="text-slate-100 text-base">Reason Codes</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-2">
            {(caseData.reasonCodes || []).map((code: string) => (
              <span key={code} className="text-xs font-medium px-3 py-1.5 rounded-r border-l-4 border-l-blue-500 bg-blue-500/10 text-blue-300">
                {code}
              </span>
            ))}
          </div>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Investigation Timeline */}
        <div className="lg:col-span-2">
          <Card className="bg-slate-800/50 border-slate-700/50">
            <CardHeader>
              <CardTitle className="text-slate-100 flex items-center gap-2">
                <Search className="h-5 w-5 text-blue-400" />
                Investigation Timeline
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="relative">
                <div className="absolute left-4 top-0 bottom-0 w-0.5 bg-slate-700" />
                <div className="space-y-6">
                  {(caseData.investigationSteps || []).map((step: any, index: number) => (
                    <div key={step.step} className="relative pl-12">
                      <div className={`absolute left-0 w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold
                        ${index === (caseData.investigationSteps?.length || 0) - 1
                          ? 'bg-green-500/20 text-green-400 border-2 border-green-500/50'
                          : 'bg-blue-500/20 text-blue-400 border-2 border-blue-500/50'
                        }`}>
                        {step.step}
                      </div>
                      <div className="bg-slate-900/50 rounded-lg p-4 border border-slate-700/50">
                        <div className="flex items-center gap-2 mb-2">
                          <span className="font-mono text-sm bg-slate-700/50 px-2 py-0.5 rounded text-blue-300">{step.tool}</span>
                        </div>
                        <p className="text-sm text-slate-300 mb-2">{step.output}</p>
                        <p className="text-xs text-slate-500 italic">{step.reasoning}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        <div className="space-y-6">
          {/* Entities */}
          <Card className="bg-slate-800/50 border-slate-700/50">
            <CardHeader>
              <CardTitle className="text-slate-100 text-base">Entities Involved</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {(caseData.entities || []).map((entity: any) => {
                const icons: Record<string, any> = { Account: User, Device: Smartphone, Beneficiary: Wallet, Merchant: Building, IP: Globe }
                const Icon = icons[entity.type] || User
                return (
                  <div key={entity.id} className="flex items-center gap-3 p-3 bg-slate-900/50 rounded-lg border border-slate-700/50">
                    <div className="w-8 h-8 rounded-full bg-slate-700/50 flex items-center justify-center">
                      <Icon className="h-4 w-4 text-slate-400" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium text-slate-200 truncate">{entity.label}</p>
                      <p className="text-xs font-mono text-slate-500 truncate">{entity.id}</p>
                    </div>
                    <Badge variant="outline" className="text-[10px] bg-slate-800 text-slate-400 border-slate-600">{entity.type}</Badge>
                  </div>
                )
              })}
            </CardContent>
          </Card>

          {/* Evidence */}
          <Card className="bg-slate-800/50 border-slate-700/50">
            <CardHeader>
              <CardTitle className="text-slate-100 text-base">Supporting Evidence</CardTitle>
            </CardHeader>
            <CardContent>
              <ul className="space-y-2">
                {(caseData.evidence || []).map((item: string, i: number) => (
                  <li key={i} className="flex items-start gap-2 text-sm">
                    <CheckCircle className="h-4 w-4 text-green-400 mt-0.5 shrink-0" />
                    <span className="text-slate-300">{item}</span>
                  </li>
                ))}
              </ul>
            </CardContent>
          </Card>

          {/* Next Steps */}
          <Card className="bg-slate-800/50 border-slate-700/50">
            <CardHeader>
              <CardTitle className="text-slate-100 text-base">Next Steps</CardTitle>
            </CardHeader>
            <CardContent>
              <ol className="space-y-2">
                {(caseData.nextSteps || []).map((step: string, i: number) => (
                  <li key={i} className="flex items-start gap-3 text-sm">
                    <span className="flex items-center justify-center w-5 h-5 rounded-full bg-blue-500/20 text-blue-400 text-xs font-bold shrink-0">{i + 1}</span>
                    <span className="text-slate-300">{step}</span>
                  </li>
                ))}
              </ol>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  )
}

function ActionBadge({ action }: { action: string }) {
  const colors: Record<string, string> = { BLOCK: 'bg-red-500 text-white', QUEUE_FOR_REVIEW: 'bg-amber-500 text-white', STEP_UP_AUTH: 'bg-blue-500 text-white', ALLOW_WITH_MONITORING: 'bg-purple-500 text-white', ALLOW: 'bg-green-500 text-white' }
  return <Badge className={`${colors[action] || 'bg-slate-500 text-white'} text-sm px-3 py-1`}>{action}</Badge>
}

function PriorityBadge({ priority }: { priority: string }) {
  const colors: Record<string, string> = { HIGH: 'bg-red-500/20 text-red-400 border-red-500/50', MEDIUM: 'bg-amber-500/20 text-amber-400 border-amber-500/50', LOW: 'bg-green-500/20 text-green-400 border-green-500/50' }
  return <span className={`text-xs font-medium px-2 py-1 rounded border ${colors[priority] || ''}`}>{priority} PRIORITY</span>
}

function RiskScoreBar({ label, score, thick = false }: { label: string; score: number; thick?: boolean }) {
  const color = score >= 0.8 ? 'bg-red-500' : score >= 0.6 ? 'bg-amber-500' : score >= 0.4 ? 'bg-blue-500' : 'bg-green-500'
  return (
    <div className="space-y-1">
      <div className="flex justify-between text-sm">
        <span className="text-slate-400">{label}</span>
        <span className="font-mono text-slate-200">{score.toFixed(3)}</span>
      </div>
      <div className={`w-full ${thick ? 'h-3' : 'h-2'} bg-slate-700 rounded-full overflow-hidden`}>
        <div className={`h-full ${color} transition-all duration-500`} style={{ width: `${score * 100}%` }} />
      </div>
    </div>
  )
}