import React, { useEffect, useState, useCallback } from 'react';
import { MapContainer, TileLayer, Marker, Popup, useMap } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';
if (typeof window !== 'undefined') {
  window.L = L;
}
import 'leaflet.heat/dist/leaflet-heat.js';
import { getMapData, getHeatmap, getClusters } from '../services/api';
import { MapPin, AlertCircle, Loader2, Layers } from 'lucide-react';

delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png',
  iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
});

const getSeverityColor = (severity) => {
  const s = String(severity).toLowerCase();
  if (s.includes('high') || s.includes('critical')) return '#ef4444';
  if (s.includes('medium') || s.includes('moderate')) return '#f59e0b';
  return '#10b981';
};

const createCustomIcon = (severity) => {
  const color = getSeverityColor(severity);
  return L.divIcon({
    className: 'custom-marker',
    html: `<div style="
      background: ${color};
      width: 28px;
      height: 28px;
      border-radius: 50% 50% 50% 0;
      transform: rotate(-45deg);
      border: 3px solid white;
      box-shadow: 0 2px 8px rgba(0,0,0,0.3);
      display: flex;
      align-items: center;
      justify-content: center;
    ">
      <div style="
        width: 8px;
        height: 8px;
        background: white;
        border-radius: 50%;
        transform: rotate(45deg);
      "></div>
    </div>`,
    iconSize: [28, 28],
    iconAnchor: [14, 28],
    popupAnchor: [0, -28],
  });
};

const defaultTile = {
  url: import.meta.env.VITE_MAP_TILE_URL || 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
  attribution:
    import.meta.env.VITE_MAP_ATTRIBUTION ||
    '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
};

function HeatmapLayer({ points }) {
  const map = useMap();
  useEffect(() => {
    if (!points?.length || typeof L.heatLayer !== 'function') return undefined;
    let heat;
    let timeoutId;
    let cancelled = false;

    const attach = () => {
      if (cancelled) return;
      map.invalidateSize();
      const { x, y } = map.getSize();
      if (x < 2 || y < 2) {
        timeoutId = window.setTimeout(attach, 100);
        return;
      }
      heat = L.heatLayer(
        points.map((p) => [p.lat, p.lng, p.weight]),
        { radius: 28, blur: 22, maxZoom: 14, max: 1.5 }
      );
      heat.addTo(map);
    };

    const onReady = () => {
      timeoutId = window.setTimeout(attach, 0);
    };
    if (map.whenReady) {
      map.whenReady(onReady);
    } else {
      onReady();
    }

    return () => {
      cancelled = true;
      window.clearTimeout(timeoutId);
      if (heat && map.hasLayer(heat)) map.removeLayer(heat);
    };
  }, [map, points]);
  return null;
}

function ClusterGeoJsonLayer({ data }) {
  const map = useMap();
  useEffect(() => {
    if (!data?.features?.length) return undefined;
    const layer = L.geoJSON(data, {
      pointToLayer(feature, latlng) {
        const c = feature.properties?.cluster ?? 0;
        const hue = (Math.abs(c) * 47) % 360;
        return L.circleMarker(latlng, {
          radius: 10,
          fillColor: `hsl(${hue}, 70%, 45%)`,
          color: '#fff',
          weight: 2,
          opacity: 1,
          fillOpacity: 0.85,
        });
      },
      onEachFeature(feature, lyr) {
        const p = feature.properties || {};
        lyr.bindPopup(
          `<div style="text-align:center"><strong>${p.region || 'Point'}</strong><br/>Cluster: ${p.cluster}<br/>Weight: ${p.weight}</div>`
        );
      },
    });
    layer.addTo(map);
    return () => {
      map.removeLayer(layer);
    };
  }, [map, data]);
  return null;
}

const MapComponent = () => {
  const [locations, setLocations] = useState([]);
  const [heatmapPoints, setHeatmapPoints] = useState([]);
  const [clusters, setClusters] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showMarkers, setShowMarkers] = useState(true);
  const [showHeat, setShowHeat] = useState(true);
  const [showClusters, setShowClusters] = useState(true);
  const [clusterMethod, setClusterMethod] = useState('dbscan');

  const loadAll = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const [locRes, heatRes, clRes] = await Promise.all([
        getMapData(),
        getHeatmap().catch(() => ({ data: { points: [] } })),
        getClusters(clusterMethod).catch(() => ({
          data: { type: 'FeatureCollection', features: [] },
        })),
      ]);
      setLocations(locRes.data);
      setHeatmapPoints(heatRes.data.points || []);
      setClusters(clRes.data?.type === 'FeatureCollection' ? clRes.data : null);
    } catch (e) {
      console.error('Map load error', e);
      setError('Failed to load map data');
    } finally {
      setLoading(false);
    }
  }, [clusterMethod]);

  useEffect(() => {
    loadAll();
  }, [loadAll]);

  if (loading) {
    return (
      <div className="spinner-container">
        <Loader2 className="spinner" size={40} />
        <p className="mt-3 mb-0">Loading map data...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="empty-state">
        <AlertCircle className="empty-state-icon" style={{ color: '#ef4444' }} />
        <p>{error}</p>
      </div>
    );
  }

  return (
    <div className="map-wrapper d-flex flex-column" style={{ height: '100%', minHeight: '350px' }}>
      <div
        className="d-flex flex-wrap align-items-center gap-2 mb-2 pb-2"
        style={{ borderBottom: '1px solid #e2e8f0' }}
      >
        <span className="d-flex align-items-center gap-1 text-muted small">
          <Layers size={14} /> Layers
        </span>
        <label className="d-flex align-items-center gap-1 small mb-0">
          <input type="checkbox" checked={showMarkers} onChange={(e) => setShowMarkers(e.target.checked)} />
          Markers
        </label>
        <label className="d-flex align-items-center gap-1 small mb-0">
          <input type="checkbox" checked={showHeat} onChange={(e) => setShowHeat(e.target.checked)} />
          Heatmap
        </label>
        <label className="d-flex align-items-center gap-1 small mb-0">
          <input type="checkbox" checked={showClusters} onChange={(e) => setShowClusters(e.target.checked)} />
          Clusters
        </label>
        <select
          className="form-select form-select-sm"
          style={{ width: 'auto', maxWidth: '140px' }}
          value={clusterMethod}
          onChange={(e) => setClusterMethod(e.target.value)}
        >
          <option value="dbscan">DBSCAN</option>
          <option value="kmeans">K-Means</option>
        </select>
      </div>
      <div style={{ flex: 1, minHeight: '400px', height: '400px' }}>
        <MapContainer center={[10.9000, 76.9600]} zoom={10} style={{ height: '100%', minHeight: '400px', width: '100%', borderRadius: '8px', zIndex: 1 }}>
          <TileLayer url={defaultTile.url} attribution={defaultTile.attribution} />
          {showHeat && heatmapPoints.length > 0 && <HeatmapLayer points={heatmapPoints} />}
          {showClusters && clusters?.features?.length > 0 && <ClusterGeoJsonLayer data={clusters} />}
          {showMarkers &&
            locations.length > 0 &&
            locations.map((loc) => (
              <Marker
                key={loc.id}
                position={[parseFloat(loc.latitude), parseFloat(loc.longitude)]}
                icon={createCustomIcon(loc.severity)}
              >
                <Popup>
                  <div style={{ textAlign: 'center', minWidth: '120px' }}>
                    <MapPin size={16} style={{ color: getSeverityColor(loc.severity), marginBottom: '4px' }} />
                    <strong style={{ display: 'block', marginBottom: '4px' }}>{loc.region}</strong>
                    <span
                      className="badge-custom"
                      style={{
                        background: getSeverityColor(loc.severity) + '20',
                        color: getSeverityColor(loc.severity),
                      }}
                    >
                      {loc.severity}
                    </span>
                  </div>
                </Popup>
              </Marker>
            ))}
        </MapContainer>
      </div>
      {locations.length === 0 && (
        <p className="text-muted small mt-2 mb-0 text-center">Upload data to add locations and weights.</p>
      )}
    </div>
  );
};

export default MapComponent;
