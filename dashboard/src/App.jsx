import React, { useState, useEffect } from 'react';
import { supabase } from './lib/supabase';
import { 
  TrendingUp, 
  TrendingDown, 
  Activity, 
  Clock, 
  ShieldCheck, 
  LayoutDashboard,
  Menu,
  ChevronRight
} from 'lucide-react';
import { motion } from 'framer-motion';
import { format } from 'date-fns';
import './index.css';

// Components
import StatsCard from './components/StatsCard';
import EquityChart from './components/EquityChart';
import TradeTable from './components/TradeTable';

function App() {
  const [trades, setTrades] = useState([]);
  const [stats, setStats] = useState({
    winRate: 0,
    totalProfit: 0,
    dailyProfit: 0,
    activeTrades: 0
  });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    setLoading(true);
    try {
      // Latest 50 trades
      const { data: tradeData, error: tradeError } = await supabase
        .from('trade_logs')
        .select('*')
        .order('entry_time', { ascending: false })
        .limit(50);

      if (tradeError) throw tradeError;
      setTrades(tradeData || []);

      // Calculate simple stats (this would normally be handled by a more complex query)
      if (tradeData && tradeData.length > 0) {
        const wins = tradeData.filter(t => (t.realized_pnl || 0) > 0).length;
        const totalPnl = tradeData.reduce((acc, t) => acc + (t.realized_pnl || 0), 0);
        
        setStats({
          winRate: (wins / tradeData.length) * 100,
          totalProfit: totalPnl,
          dailyProfit: tradeData.filter(t => {
            const date = new Date(t.exit_time);
            return date.toDateString() === new Date().toDateString();
          }).reduce((acc, t) => acc + (t.realized_pnl || 0), 0),
          activeTrades: tradeData.filter(t => !t.exit_time).length
        });
      }
    } catch (error) {
      console.error('Error fetching dashboard data:', error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="dashboard-container">
      <header style={{ marginBottom: '2rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <h1 style={{ fontSize: '1.5rem', fontWeight: 700 }}>V18 Quantum Dashboard</h1>
          <p style={{ color: 'var(--text-secondary)', fontSize: '0.875rem' }}>Real-time algorithmic trading monitor</p>
        </div>
        <div style={{ display: 'flex', gap: '1rem' }}>
          <button onClick={fetchData} className="glass-card" style={{ padding: '0.5rem 1rem', fontSize: '0.75rem', cursor: 'pointer' }}>
            <Activity size={14} style={{ marginRight: '0.5rem' }} /> Refresh
          </button>
        </div>
      </header>

      <div className="stats-grid">
        <StatsCard 
          label="Win Rate" 
          value={`${stats.winRate.toFixed(1)}%`} 
          subValue="Last 50 trades" 
          icon={<ShieldCheck size={24} color="var(--accent-primary)" />} 
        />
        <StatsCard 
          label="Total Net Profit" 
          value={`${stats.totalProfit.toFixed(2)} USDT`} 
          subValue="Cumulative" 
          icon={<TrendingUp size={24} color="var(--accent-long)" />}
          trend={stats.totalProfit >= 0 ? 'up' : 'down'}
        />
        <StatsCard 
          label="Daily Gain" 
          value={`${stats.dailyProfit.toFixed(2)} USDT`} 
          subValue="Today's performance" 
          icon={<Clock size={24} color="var(--text-secondary)" />} 
        />
        <StatsCard 
          label="Active Engine" 
          value={stats.activeTrades} 
          subValue="Open positions" 
          icon={<LayoutDashboard size={24} color="var(--accent-primary)" />} 
        />
      </div>

      <div className="chart-section">
        <div className="glass-card">
          <h3 style={{ marginBottom: '1.5rem', fontSize: '1rem' }}>Equity Curve</h3>
          <EquityChart trades={trades} />
        </div>
        <div className="glass-card">
          <h3 style={{ marginBottom: '1.5rem', fontSize: '1rem' }}>Engine Status</h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
            {/* Simple status list for now */}
            <StatusItem label="HFT Pipeline" status="Active" />
            <StatusItem label="Cortex Loop" status="Syncing" />
            <StatusItem label="Risk Manager" status="Healthy" />
            <StatusItem label="Supabase DB" status="Connected" />
          </div>
        </div>
      </div>

      <div className="glass-card">
        <h3 style={{ marginBottom: '1.5rem', fontSize: '1rem' }}>Recent Trade Execution</h3>
        <TradeTable trades={trades} />
      </div>
    </div>
  );
}

function StatusItem({ label, status }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', padding: '0.75rem', borderRadius: '0.5rem', background: 'rgba(255,255,255,0.03)' }}>
      <span style={{ fontSize: '0.875rem', color: 'var(--text-secondary)' }}>{label}</span>
      <span style={{ fontSize: '0.875rem', fontWeight: 600, color: 'var(--accent-long)' }}>{status}</span>
    </div>
  );
}

export default App;
