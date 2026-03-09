import React from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Filler,
  Legend,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import { format } from 'date-fns';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Filler,
  Legend
);

const EquityChart = ({ trades }) => {
  // Process trades for cumulative profit
  const sortedTrades = [...trades]
    .filter(t => t.exit_time) // Only finished trades
    .sort((a, b) => new Date(a.exit_time) - new Date(b.exit_time));

  let cumulative = 0;
  const chartData = sortedTrades.map(t => {
    cumulative += (t.realized_pnl || 0);
    return {
      x: format(new Date(t.exit_time), 'MM/dd HH:mm'),
      y: cumulative
    };
  });

  const data = {
    labels: chartData.map(d => d.x),
    datasets: [
      {
        label: 'Cumulative PNL (USDT)',
        data: chartData.map(d => d.y),
        fill: true,
        borderColor: '#3b82f6',
        backgroundColor: 'rgba(59, 130, 246, 0.1)',
        tension: 0.4,
        pointRadius: 2,
        pointHoverRadius: 6,
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
      tooltip: {
        mode: 'index',
        intersect: false,
        backgroundColor: 'rgba(13, 14, 18, 0.95)',
        titleColor: '#94a3b8',
        bodyColor: '#f8fafc',
        borderColor: 'rgba(255,255,255,0.1)',
        borderWidth: 1,
        padding: 12,
        displayColors: false,
      },
    },
    scales: {
      y: {
        grid: {
          color: 'rgba(255, 255, 255, 0.05)',
        },
        ticks: {
          color: '#64748b',
          font: { size: 10 },
        },
      },
      x: {
        grid: {
          display: false,
        },
        ticks: {
          color: '#64748b',
          font: { size: 10 },
          maxRotation: 45,
          minRotation: 45,
        },
      },
    },
  };

  return (
    <div style={{ height: '300px' }}>
      {chartData.length > 0 ? (
        <Line data={data} options={options} />
      ) : (
        <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-secondary)' }}>
          No historical data available
        </div>
      )}
    </div>
  );
};

export default EquityChart;
