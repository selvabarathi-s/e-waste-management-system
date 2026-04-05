import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Area } from 'recharts';

const colors = ["#10b981", "#6366f1", "#f59e0b"];

const CustomTooltip = ({ active, payload, label }) => {
  if (active && payload && payload.length) {
    return (
      <div style={{
        background: '#fff',
        padding: '12px 16px',
        borderRadius: '12px',
        boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
        border: '1px solid #e2e8f0',
        fontSize: '0.875rem'
      }}>
        <p style={{ fontWeight: 600, marginBottom: '8px', color: '#1e293b' }}>Year: {label}</p>
        {payload.map((entry, index) => (
          <p key={index} style={{ color: entry.color, margin: '4px 0', display: 'flex', alignItems: 'center', gap: '6px' }}>
            <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: entry.color, display: 'inline-block' }}></span>
            {entry.name}: <strong>{typeof entry.value === 'number' ? entry.value.toLocaleString() : entry.value}</strong>
          </p>
        ))}
      </div>
    );
  }
  return null;
};

const Chart = ({ data, xKey, yKeys, colors: customColors }) => {
  const colors = customColors || ["#10b981", "#6366f1", "#f59e0b"];
  return (
    <ResponsiveContainer width="100%" height={300}>
      <LineChart data={data} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
        <defs>
          {yKeys.map((key, index) => (
            <linearGradient key={key} id={`gradient-${key}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={colors[index % colors.length]} stopOpacity={0.3} />
              <stop offset="95%" stopColor={colors[index % colors.length]} stopOpacity={0} />
            </linearGradient>
          ))}
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
        <XAxis dataKey={xKey} tick={{ fill: '#64748b', fontSize: 12 }} axisLine={{ stroke: '#e2e8f0' }} />
        <YAxis tick={{ fill: '#64748b', fontSize: 12 }} axisLine={{ stroke: '#e2e8f0' }} />
        <Tooltip content={<CustomTooltip />} />
        <Legend wrapperStyle={{ fontSize: '0.85rem' }} />
        {yKeys.map((key, index) => (
          <React.Fragment key={key}>
            <Area
              type="monotone"
              dataKey={key}
              fill={`url(#gradient-${key})`}
              stroke="none"
              legendType="none"
            />
            <Line
              type="monotone"
              dataKey={key}
              name={key === 'predicted_tonnes' ? 'Predicted (tonnes)' : key}
              stroke={colors[index % colors.length]}
              strokeWidth={3}
              dot={{ fill: colors[index % colors.length], strokeWidth: 2, r: 4, stroke: '#fff' }}
              activeDot={{ r: 6, stroke: '#fff', strokeWidth: 2 }}
              animationDuration={1000}
            />
          </React.Fragment>
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
};

export default Chart;
