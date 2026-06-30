const API = import.meta.env.VITE_API_URL || 'http://127.0.0.1:8000';
const WS = API.replace(/^https:/, 'wss:').replace(/^http:/, 'ws:');

export const API_URL = API;
export const WS_URL = WS;
