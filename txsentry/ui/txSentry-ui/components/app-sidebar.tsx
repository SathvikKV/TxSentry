'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import {
  LayoutDashboard,
  AlertTriangle,
  FileSearch,
  BarChart3,
  Activity,
  Network,
  Shield
} from 'lucide-react'

import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarFooter,
} from '@/components/ui/sidebar'

const navItems = [
  { title: 'Overview', href: '/', icon: LayoutDashboard },
  { title: 'Alert Queue', href: '/alerts', icon: AlertTriangle },
  { title: 'Case Investigation', href: '/case', icon: FileSearch },
  { title: 'Model Performance', href: '/model', icon: BarChart3 },
  { title: 'Monitoring & Drift', href: '/monitoring', icon: Activity },
  { title: 'Graph Explorer', href: '/graph', icon: Network },
]

export function AppSidebar() {
  const pathname = usePathname()

  return (
    <Sidebar className="border-r border-slate-700/50">
      <SidebarHeader className="border-b border-slate-700/50 px-4 py-4">
        <Link href="/" className="flex items-center gap-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-blue-600">
            <Shield className="h-5 w-5 text-white" />
          </div>
          <div className="flex flex-col">
            <span className="text-lg font-bold text-slate-100">TxSentry</span>
            <span className="text-xs text-slate-400">Fraud Detection</span>
          </div>
        </Link>
      </SidebarHeader>
      <SidebarContent className="px-2 py-4">
        <SidebarGroup>
          <SidebarGroupLabel className="text-xs font-medium text-slate-500 uppercase tracking-wider px-3 mb-2">
            Navigation
          </SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {navItems.map((item) => {
                const isActive = pathname === item.href
                return (
                  <SidebarMenuItem key={item.href}>
                    <SidebarMenuButton
                      asChild
                      isActive={isActive}
                      className={`
                        transition-all duration-200
                        ${isActive 
                          ? 'bg-blue-600/20 text-blue-400 border-l-2 border-blue-500' 
                          : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
                        }
                      `}
                    >
                      <Link href={item.href} className="flex items-center gap-3 px-3 py-2">
                        <item.icon className="h-4 w-4" />
                        <span>{item.title}</span>
                      </Link>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                )
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter className="border-t border-slate-700/50 px-4 py-4">
        <div className="text-xs text-slate-500">
          <p>Portfolio Demo</p>
          <p className="text-slate-600">v1.0.0</p>
        </div>
      </SidebarFooter>
    </Sidebar>
  )
}
