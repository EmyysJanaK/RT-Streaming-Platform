import React from "react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";

// Group metrics by eventType for charting
function groupByEventType(data) {
  const grouped = {};
  data.forEach((item) => {
    const { event_type, window_start, count } = item;
    if (!grouped[event_type]) grouped[event_type] = [];
    grouped[event_type].push({
      time: window_start || item.windowStart || item.window_start,
      count,
    });
  });
  return grouped;
}

function MetricsChart({ data }) {
  const grouped = groupByEventType(data);
  return (
    <div>
      {Object.keys(grouped).length === 0 && <div>No data yet.</div>}
      {Object.entries(grouped).map(([eventType, points]) => (
        <div key={eventType} style={{ marginBottom: 32 }}>
          <h4>{eventType}</h4>
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={points} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="time" tickFormatter={t => t && t.substring(11, 19)} />
              <YAxis allowDecimals={false} />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="count" stroke="#8884d8" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      ))}
    </div>
  );
}

export default MetricsChart;
