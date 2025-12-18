import React, { useEffect, useRef, useState } from "react";
import MetricsChart from "./MetricsChart";

const WS_URL = process.env.REACT_APP_WS_URL || "ws://localhost:8080/ws/analytics";

function App() {
  const [metrics, setMetrics] = useState([]);
  const ws = useRef(null);
  const reconnectTimeout = useRef(null);

  // Handle WebSocket connection and reconnection
  useEffect(() => {
    function connect() {
      ws.current = new window.WebSocket(WS_URL);
      ws.current.onopen = () => {
        // Connection established
      };
      ws.current.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          setMetrics((prev) => [...prev.slice(-99), data]); // Keep last 100
        } catch (e) {}
      };
      ws.current.onclose = () => {
        // Try to reconnect after 2s
        reconnectTimeout.current = setTimeout(connect, 2000);
      };
      ws.current.onerror = () => {
        ws.current.close();
      };
    }
    connect();
    return () => {
      if (ws.current) ws.current.close();
      if (reconnectTimeout.current) clearTimeout(reconnectTimeout.current);
    };
  }, []);

  return (
    <div style={{ padding: 24 }}>
      <h2>Real-Time Analytics Dashboard</h2>
      <MetricsChart data={metrics} />
    </div>
  );
}

export default App;
