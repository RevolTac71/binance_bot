import React from 'react';
import { motion } from 'framer-motion';

const StatsCard = ({ label, value, subValue, icon, trend }) => {
  return (
    <motion.div 
      className="glass-card metric-card"
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4 }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <span className="metric-label">{label}</span>
        {icon}
      </div>
      <div className={`metric-value ${trend === 'up' ? 'positive' : trend === 'down' ? 'negative' : ''}`}>
        {value}
      </div>
      <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', marginTop: '0.5rem' }}>
        {subValue}
      </div>
    </motion.div>
  );
};

export default StatsCard;
