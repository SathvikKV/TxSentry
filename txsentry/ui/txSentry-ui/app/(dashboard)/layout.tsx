import { SidebarProvider, SidebarInset, SidebarTrigger } from '@/components/ui/sidebar'
import { AppSidebar } from '@/components/app-sidebar'

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <SidebarProvider>
      <AppSidebar />
      <SidebarInset className="bg-[#0f172a]">
        <header className="sticky top-0 z-10 flex h-14 items-center gap-4 border-b border-slate-700/50 bg-[#0f172a]/95 backdrop-blur px-6">
          <SidebarTrigger className="text-slate-400 hover:text-slate-200" />
        </header>
        <main className="flex-1 overflow-auto p-6">
          {children}
        </main>
      </SidebarInset>
    </SidebarProvider>
  )
}
