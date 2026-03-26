import { useState, useEffect, useRef } from 'react';
// @ts-ignore
import Plotly from 'plotly.js-dist-min';
import {
  LayoutDashboard,
  MessageSquare,
  Database,
  Settings,
  TrendingUp,
  Users,
  Package,
  DollarSign,
  Send,
  Loader2,
  ChevronRight,
  DatabaseZap,
  Quote,
  CheckCircle2,
  AlertTriangle,
  Clock,
  ArrowUpRight,
  BarChart3,
  Sparkles,
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

// ── Types ────────────────────────────────────────────────────────────
interface KPI {
  totalRevenue: number;
  totalOrders: number;
  avgTicket: number;
  activeCustomers: number;
  topProducts: Array<{ name: string; sold: number; revenue: number }>;
  monthlyRevenue: Array<{ month: string; revenue: number }>;
}

interface Message {
  role: 'user' | 'assistant';
  content: string;
  sources?: any[];
  chart?: any;
}

interface PipelineStep {
  name: string;
  desc: string;
  status: string;
  records: number | null;
  lastRun: string;
}

// ── PlotlyChart (DOM-native, Vite-safe) ──────────────────────────────
const PlotlyChart = ({ data, layout, config, style }: any) => {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!ref.current) return;
    const mergedLayout = {
      ...layout,
      autosize: true,
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: { color: '#94a3b8', family: 'Inter, sans-serif', size: 11 },
      xaxis: { ...layout?.xaxis, gridcolor: 'rgba(255,255,255,0.04)', linecolor: 'rgba(255,255,255,0.06)' },
      yaxis: { ...layout?.yaxis, gridcolor: 'rgba(255,255,255,0.04)', linecolor: 'rgba(255,255,255,0.06)' },
    };
    Plotly.newPlot(ref.current, data, mergedLayout, { ...config, responsive: true, displayModeBar: false });
    return () => { if (ref.current) Plotly.purge(ref.current); };
  }, [data, layout, config]);
  return <div ref={ref} style={style || { width: '100%', height: '280px' }} />;
};

// ── Sidebar Item ─────────────────────────────────────────────────────
const SidebarItem = ({ icon: Icon, label, active, onClick }: any) => (
  <button
    onClick={onClick}
    className={`w-full flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-200 group ${
      active
        ? 'bg-primary/15 text-primary border border-primary/20 glow-primary'
        : 'text-muted-foreground hover:bg-secondary hover:text-foreground border border-transparent'
    }`}
  >
    <Icon size={18} className={active ? 'text-primary' : 'text-muted-foreground group-hover:text-foreground'} />
    <span className="font-medium text-sm">{label}</span>
    {active && <ChevronRight size={14} className="ml-auto opacity-60" />}
  </button>
);

// ── KPI Card ─────────────────────────────────────────────────────────
const KPICard = ({ title, value, icon: Icon, trend, delay }: any) => (
  <motion.div
    initial={{ opacity: 0, y: 16 }}
    animate={{ opacity: 1, y: 0 }}
    transition={{ delay: delay * 0.1, duration: 0.4 }}
    className="p-5 glass-card hover:border-primary/30 transition-all duration-300 group"
  >
    <div className="flex items-center justify-between mb-3">
      <div className="p-2.5 rounded-lg bg-primary/10 text-primary group-hover:bg-primary/20 transition-colors">
        {Icon && <Icon size={20} />}
      </div>
      <div className="flex items-center gap-1 text-success text-xs font-semibold">
        <ArrowUpRight size={12} />
        {trend}
      </div>
    </div>
    <p className="text-xs text-muted-foreground mb-1">{title}</p>
    <p className="text-xl font-bold text-foreground">{value}</p>
  </motion.div>
);

// ── Pipeline Step Card ───────────────────────────────────────────────
const PipelineCard = ({ step, index }: { step: PipelineStep; index: number }) => {
  const statusColors: Record<string, string> = {
    success: 'text-success',
    warning: 'text-warning',
    error: 'text-danger',
  };
  const StatusIcon = step.status === 'success' ? CheckCircle2 : step.status === 'warning' ? AlertTriangle : AlertTriangle;

  return (
    <motion.div
      initial={{ opacity: 0, x: -20 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ delay: index * 0.08 }}
      className="glass-card p-5 flex items-center gap-4 hover:border-primary/30 transition-all"
    >
      <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-primary/10 text-primary font-bold text-sm shrink-0">
        {String(index + 1).padStart(2, '0')}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1">
          <span className="font-semibold text-sm text-foreground">{step.name}</span>
          <StatusIcon size={14} className={statusColors[step.status] || 'text-muted-foreground'} />
        </div>
        <p className="text-xs text-muted-foreground truncate">{step.desc}</p>
      </div>
      <div className="text-right shrink-0">
        {step.records !== null && (
          <div className="text-sm font-semibold text-foreground">{step.records.toLocaleString('pt-BR')} registros</div>
        )}
        <div className="flex items-center gap-1 text-[10px] text-muted-foreground mt-1">
          <Clock size={10} />
          {step.lastRun}
        </div>
      </div>
    </motion.div>
  );
};

// ══════════════════════════════════════════════════════════════════════
// MAIN APP
// ══════════════════════════════════════════════════════════════════════
export default function App() {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [kpis, setKpis] = useState<KPI | null>(null);
  const [pipeline, setPipeline] = useState<PipelineStep[]>([]);
  const [messages, setMessages] = useState<Message[]>([
    { role: 'assistant', content: '👋 Olá! Sou seu assistente ERP inteligente.\n\nPosso analisar dados de receita, produtos, clientes e muito mais. Tente perguntar:\n• "Top 5 produtos mais vendidos"\n• "Qual a receita total?"\n• "Perfil dos clientes"' },
  ]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    fetch('http://localhost:8000/api/kpis')
      .then((r) => r.json())
      .then(setKpis)
      .catch((e) => console.error('KPI error:', e));

    fetch('http://localhost:8000/api/pipeline')
      .then((r) => r.json())
      .then(setPipeline)
      .catch((e) => console.error('Pipeline error:', e));
  }, []);

  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [messages, isLoading]);

  const sendMessage = async () => {
    if (!input.trim() || isLoading) return;
    const userMsg: Message = { role: 'user', content: input };
    setMessages((p) => [...p, userMsg]);
    setIsLoading(true);
    setInput('');

    try {
      const res = await fetch('http://localhost:8000/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: input, history: messages.map((m) => ({ role: m.role, content: m.content })) }),
      });
      const data = await res.json();
      setMessages((p) => [...p, { role: 'assistant', content: data.answer, sources: data.sources, chart: data.chart }]);
    } catch {
      setMessages((p) => [...p, { role: 'assistant', content: '❌ Erro ao conectar ao servidor. Verifique se o backend está rodando na porta 8000.' }]);
    } finally {
      setIsLoading(false);
    }
  };

  const fmt = (v: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(v);

  return (
    <div className="flex h-screen bg-background text-foreground overflow-hidden">
      {/* ── Sidebar ──────────────────────────────── */}
      <aside className="w-60 border-r border-border/50 bg-card/50 backdrop-blur-xl p-5 flex flex-col gap-6">
        <div className="flex items-center gap-2.5 px-2">
          <div className="p-1.5 rounded-lg bg-primary/15">
            <DatabaseZap size={22} className="text-primary" />
          </div>
          <div>
            <h1 className="text-sm font-bold gradient-text">Lakehouse ERP</h1>
            <p className="text-[10px] text-muted-foreground">Databricks Platform</p>
          </div>
        </div>

        <nav className="flex-1 space-y-1.5">
          <SidebarItem icon={LayoutDashboard} label="Dashboard" active={activeTab === 'dashboard'} onClick={() => setActiveTab('dashboard')} />
          <SidebarItem icon={MessageSquare} label="Assistente IA" active={activeTab === 'chat'} onClick={() => setActiveTab('chat')} />
          <SidebarItem icon={Database} label="Data Pipeline" active={activeTab === 'pipeline'} onClick={() => setActiveTab('pipeline')} />
          <div className="pt-3 mt-3 border-t border-border/30">
            <SidebarItem icon={Settings} label="Configurações" active={activeTab === 'settings'} onClick={() => setActiveTab('settings')} />
          </div>
        </nav>

        <div className="p-3 rounded-xl bg-success/5 border border-success/20">
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-success pulse-glow" />
            <span className="text-[10px] font-semibold uppercase tracking-widest text-success/80">Online</span>
          </div>
          <p className="text-[10px] text-muted-foreground mt-1">Unity Catalog • workspace</p>
        </div>
      </aside>

      {/* ── Main ─────────────────────────────────── */}
      <main className="flex-1 flex flex-col overflow-hidden">
        <header className="h-14 border-b border-border/30 flex items-center justify-between px-6 bg-card/30 backdrop-blur-xl shrink-0">
          <div className="flex items-center gap-2">
            <BarChart3 size={16} className="text-primary" />
            <h2 className="text-sm font-semibold capitalize text-foreground">{activeTab === 'pipeline' ? 'Data Pipeline' : activeTab}</h2>
          </div>
          <div className="flex items-center gap-3">
            <div className="w-7 h-7 rounded-full bg-gradient-to-br from-primary to-accent flex items-center justify-center text-white font-bold text-[10px]">V</div>
          </div>
        </header>

        <div className="flex-1 overflow-auto p-6">
          {/* ── DASHBOARD ───────────────────────── */}
          {activeTab === 'dashboard' && (
            <div className="space-y-6 fade-in max-w-7xl mx-auto">
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
                <KPICard title="Receita Total" value={kpis ? fmt(kpis.totalRevenue) : '—'} icon={DollarSign} trend="+12.5%" delay={0} />
                <KPICard title="Pedidos" value={kpis?.totalOrders?.toLocaleString('pt-BR') ?? '—'} icon={Package} trend="+8.2%" delay={1} />
                <KPICard title="Ticket Médio" value={kpis ? fmt(kpis.avgTicket) : '—'} icon={TrendingUp} trend="+3.1%" delay={2} />
                <KPICard title="Clientes Ativos" value={kpis?.activeCustomers?.toLocaleString('pt-BR') ?? '—'} icon={Users} trend="+5.4%" delay={3} />
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
                {/* Revenue Chart */}
                <div className="lg:col-span-3 glass-card p-5">
                  <h3 className="text-sm font-semibold mb-4 text-foreground">Crescimento de Receita</h3>
                  {kpis?.monthlyRevenue ? (
                    <PlotlyChart
                      data={[{
                        x: kpis.monthlyRevenue.map((m) => m.month),
                        y: kpis.monthlyRevenue.map((m) => m.revenue),
                        type: 'scatter',
                        mode: 'lines+markers',
                        marker: { color: '#3b82f6', size: 8 },
                        line: { shape: 'spline', width: 3, color: '#3b82f6' },
                        fill: 'tozeroy',
                        fillcolor: 'rgba(59, 130, 246, 0.08)',
                        hovertemplate: '<b>%{x|%b %Y}</b><br>R$ %{y:,.2f}<extra></extra>',
                      }]}
                      layout={{ 
                        margin: { t: 5, r: 15, b: 30, l: 50 },
                        xaxis: { tickformat: '%b %Y', hoverformat: '%B %Y' },
                        yaxis: { tickformat: '.2s' }
                      }}
                      style={{ width: '100%', height: '260px' }}
                    />
                  ) : (
                    <div className="h-[260px] flex items-center justify-center text-muted-foreground text-sm">Carregando...</div>
                  )}
                </div>

                {/* Top Products */}
                <div className="lg:col-span-2 glass-card p-5">
                  <h3 className="text-sm font-semibold mb-4 text-foreground">Produtos Mais Vendidos</h3>
                  <div className="space-y-2">
                    {kpis?.topProducts?.map((p, i) => (
                      <motion.div
                        key={i}
                        initial={{ opacity: 0, x: 10 }}
                        animate={{ opacity: 1, x: 0 }}
                        transition={{ delay: i * 0.08 }}
                        className="flex items-center justify-between p-2.5 rounded-lg hover:bg-muted/50 transition-colors group"
                      >
                        <div className="flex items-center gap-3">
                          <span className="w-6 h-6 rounded-md bg-primary/10 text-primary flex items-center justify-center text-[10px] font-bold">
                            {i + 1}
                          </span>
                          <div>
                            <p className="text-xs font-medium text-foreground">{p.name}</p>
                            <p className="text-[10px] text-muted-foreground">{p.sold} vendas</p>
                          </div>
                        </div>
                        <span className="text-xs font-semibold text-foreground">{fmt(p.revenue)}</span>
                      </motion.div>
                    )) ?? <div className="text-muted-foreground text-sm text-center py-6">Carregando...</div>}
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* ── CHAT ─────────────────────────────── */}
          {activeTab === 'chat' && (
            <div className="max-w-3xl mx-auto h-[calc(100vh-6rem)] flex flex-col fade-in">
              <div ref={scrollRef} className="flex-1 overflow-y-auto space-y-4 pb-4 pr-1">
                <AnimatePresence>
                  {messages.map((msg, i) => (
                    <motion.div
                      key={i}
                      initial={{ opacity: 0, y: 10 }}
                      animate={{ opacity: 1, y: 0 }}
                      className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
                    >
                      <div className={`max-w-[85%] ${
                        msg.role === 'user'
                          ? 'bg-primary text-white rounded-2xl rounded-br-md'
                          : 'glass-card rounded-2xl rounded-bl-md'
                      } p-4 space-y-3`}>
                        <div className="text-sm leading-relaxed whitespace-pre-wrap">{msg.content}</div>

                        {msg.chart && (
                          <div className="mt-3 p-3 bg-background/50 rounded-xl border border-border/30 relative group">
                            <button
                              onClick={() => {
                                const chart = msg.chart;
                                if (!chart || !chart.x || !chart.y) return;
                                const header = `${chart.xaxis_title || 'Categoria'};${chart.yaxis_title || 'Valor'}`;
                                const rows = chart.x.map((xVal: any, idx: number) => {
                                  // Assegurando que textos sigam o padrão com aspas no CSV
                                  const yVal = String(chart.y[idx]).replace('.', ',');
                                  return `"${xVal}";"${yVal}"`;
                                });
                                const csvContent = "data:text/csv;charset=utf-8,\uFEFF" + [header, ...rows].join("\n");
                                const link = document.createElement("a");
                                link.href = encodeURI(csvContent);
                                link.download = `${chart.title?.replace(/\s+/g, '_').toLowerCase() || 'dados'}.csv`;
                                link.click();
                              }}
                              className="absolute top-2 right-2 p-1.5 bg-background border border-white/10 rounded-md text-muted-foreground hover:text-primary hover:border-primary/50 opacity-0 group-hover:opacity-100 transition-all z-10"
                              title="Exportar para CSV"
                            >
                              <span className="text-[10px] uppercase font-bold tracking-wider flex items-center gap-1">⬇️ CSV</span>
                            </button>
                            <PlotlyChart
                              data={[{
                                x: msg.chart.x,
                                y: msg.chart.y,
                                type: msg.chart.type || 'bar',
                                marker: { color: ['#3b82f6', '#6366f1', '#8b5cf6', '#a855f7', '#d946ef'].slice(0, msg.chart.x?.length) },
                                hovertemplate: (msg.chart.title?.toLowerCase().includes("volume") || msg.chart.title?.toLowerCase().includes("unidade") || msg.chart.title?.toLowerCase().includes("quantidade")) 
                                  ? '<b>%{x}</b><br>%{y} unidades<extra></extra>' 
                                  : '<b>%{x}</b><br>R$ %{y:,.2f}<extra></extra>',
                              }]}
                              layout={{
                                title: { text: msg.chart.title, font: { size: 12, color: '#94a3b8' } },
                                margin: { t: 35, r: 10, b: 80, l: 40 },
                                xaxis: { title: msg.chart.xaxis_title },
                                yaxis: { title: msg.chart.yaxis_title },
                              }}
                              style={{ width: '100%', height: '260px' }}
                            />
                          </div>
                        )}

                        {msg.sources && msg.sources.length > 0 && (
                          <div className="mt-4 pt-3 border-t border-white/5 space-y-2">
                            <div className="flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-wider text-primary/70">
                              <DatabaseZap size={10} /> Fontes Pesquisadas no Unity Catalog
                            </div>
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mt-2">
                              {msg.sources.slice(0, 4).map((s: any, idx: number) => (
                                <div key={idx} className="flex flex-col gap-1 p-2.5 rounded-lg bg-background/40 border border-white/10 hover:border-primary/30 hover:bg-white/5 transition-all cursor-default">
                                  <div className="flex items-center gap-1.5 opacity-70">
                                    <Database size={10} className="text-primary" />
                                    <span className="text-[10px] font-mono tracking-wide text-primary/90">
                                      {s.id ? String(s.id).replace('workspace.gold_ecommerce.', '') : 'tabela_databricks'}
                                    </span>
                                  </div>
                                  <span className="text-xs text-muted-foreground line-clamp-2 leading-relaxed" title={s.text}>
                                    {s.text}
                                  </span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    </motion.div>
                  ))}

                  {isLoading && (
                    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="flex justify-start">
                      <div className="glass-card p-4 rounded-2xl rounded-bl-md flex items-center gap-2">
                        <Loader2 size={14} className="animate-spin text-primary" />
                        <span className="text-xs text-muted-foreground">Analisando Databricks...</span>
                      </div>
                    </motion.div>
                  )}
                </AnimatePresence>
              </div>

              <div className="pt-3 border-t border-border/30">
                <div className="flex items-center gap-2 glass-card p-2">
                  <Sparkles size={16} className="text-primary/50 ml-2 shrink-0" />
                  <input
                    type="text"
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && sendMessage()}
                    placeholder="Pergunte sobre receita, produtos, clientes..."
                    className="flex-1 bg-transparent text-sm outline-none text-foreground placeholder:text-muted-foreground py-2"
                  />
                  <button
                    onClick={sendMessage}
                    disabled={isLoading || !input.trim()}
                    className="p-2.5 bg-primary hover:bg-primary/90 text-white rounded-lg transition-all disabled:opacity-30 shrink-0"
                  >
                    <Send size={14} />
                  </button>
                </div>
              </div>
            </div>
          )}

          {/* ── PIPELINE ─────────────────────────── */}
          {activeTab === 'pipeline' && (
            <div className="max-w-4xl mx-auto space-y-4 fade-in">
              <div className="glass-card p-5 flex items-center justify-between mb-2">
                <div>
                  <h3 className="text-sm font-semibold text-foreground">Medallion Architecture</h3>
                  <p className="text-xs text-muted-foreground mt-0.5">Unity Catalog: workspace.ecommerce_rag</p>
                </div>
                <div className="flex items-center gap-6 text-xs">
                  <div className="flex items-center gap-1.5"><div className="w-3 h-1.5 rounded-full bg-amber-600" /> Bronze</div>
                  <div className="flex items-center gap-1.5"><div className="w-3 h-1.5 rounded-full bg-slate-400" /> Silver</div>
                  <div className="flex items-center gap-1.5"><div className="w-3 h-1.5 rounded-full bg-yellow-500" /> Gold</div>
                </div>
              </div>

              {pipeline.length > 0 ? (
                pipeline.map((step, i) => <PipelineCard key={i} step={step} index={i} />)
              ) : (
                <div className="text-center text-muted-foreground py-12 text-sm">Carregando status do pipeline...</div>
              )}
            </div>
          )}

          {/* ── SETTINGS ─────────────────────────── */}
          {activeTab === 'settings' && (
            <div className="max-w-2xl mx-auto space-y-4 fade-in">
              <div className="glass-card p-6 space-y-4">
                <h3 className="text-sm font-semibold text-foreground">Conexão Databricks</h3>
                <div className="space-y-3 text-xs">
                  <div className="flex justify-between p-3 rounded-lg bg-muted/30">
                    <span className="text-muted-foreground">Catalog</span>
                    <span className="font-mono text-foreground">workspace</span>
                  </div>
                  <div className="flex justify-between p-3 rounded-lg bg-muted/30">
                    <span className="text-muted-foreground">Schema</span>
                    <span className="font-mono text-foreground">ecommerce_rag</span>
                  </div>
                  <div className="flex justify-between p-3 rounded-lg bg-muted/30">
                    <span className="text-muted-foreground">Vector Search</span>
                    <span className="font-mono text-foreground">ecommerce_rag_vs_endpoint</span>
                  </div>
                  <div className="flex justify-between p-3 rounded-lg bg-muted/30">
                    <span className="text-muted-foreground">LLM</span>
                    <span className="font-mono text-foreground text-primary">Google Gemini 3 Flash</span>
                  </div>
                  <div className="flex justify-between p-3 rounded-lg bg-muted/30">
                    <span className="text-muted-foreground">Status</span>
                    <span className="flex items-center gap-1.5 text-success font-semibold"><div className="w-1.5 h-1.5 rounded-full bg-success" /> Conectado</span>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
