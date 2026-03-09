import React from 'react';
import { format } from 'date-fns';

const TradeTable = ({ trades }) => {
  return (
    <div className="trade-table-container">
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Symbol</th>
            <th>Type</th>
            <th>Result</th>
            <th>Reason</th>
            <th>Slippage</th>
          </tr>
        </thead>
        <tbody>
          {trades.map((trade, i) => (
            <tr key={trade.trade_id || i}>
              <td style={{ color: 'var(--text-secondary)' }}>
                {format(new Date(trade.entry_time), 'MM/dd HH:mm')}
              </td>
              <td style={{ fontWeight: 600 }}>{trade.symbol}</td>
              <td>
                <span className={`badge ${trade.direction === 'LONG' ? 'badge-long' : 'badge-short'}`}>
                  {trade.direction}
                </span>
              </td>
              <td className={(trade.realized_pnl || 0) >= 0 ? 'positive' : 'negative'} style={{ fontWeight: 700 }}>
                {trade.realized_pnl ? `${trade.realized_pnl >= 0 ? '+' : ''}${trade.realized_pnl.toFixed(2)}` : '--'}
              </td>
              <td style={{ fontSize: '0.75rem', maxWidth: '300px', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                {trade.exit_reason || trade.entry_reason || '--'}
              </td>
              <td>
                <span style={{ fontSize: '0.75rem', opacity: 0.6 }}>
                  {trade.slippage ? `${(trade.slippage * 100).toFixed(3)}%` : '0.0%'}
                </span>
              </td>
            </tr>
          ))}
          {trades.length === 0 && (
            <tr>
              <td colSpan="6" style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-secondary)' }}>
                No recent trades found in history
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
};

export default TradeTable;
