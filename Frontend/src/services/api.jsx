// // src/services/api.js
// import axios from "axios";

// const BASE_URL = process.env.REACT_APP_API_URL || "http://localhost:8000";

// const api = axios.create({ baseURL: BASE_URL, timeout: 300000 });

// export const summarizeDocument = async (file) => {
//   const fd = new FormData();
//   fd.append("file", file);
//   const res = await api.post("/summarize", fd, {
//     headers: { "Content-Type": "multipart/form-data" },
//   });
//   return res.data;
// };

// export const extractEntities = async (docId) => {
//   const res = await api.post(`/extract-entities?doc_id=${docId}`);
//   return res.data;
// };

// export const buildGraph = async (docId) => {
//   const res = await api.post(`/build-graph?doc_id=${docId}`);
//   return res.data;
// };

// export const queryGraph = async (docId, entity) => {
//   const res = await api.get(`/graph-query?doc_id=${docId}&entity=${encodeURIComponent(entity)}`);
//   return res.data;
// };

// export const verifyFacts = async (docId) => {
//   const res = await api.post(`/verify-facts?doc_id=${docId}`);
//   return res.data;
// };

// export const semanticQuery = async (docId, query) => {
//   const res = await api.post(`/query?doc_id=${docId}&query=${encodeURIComponent(query)}`);
//   return res.data;
// };

// export default api;

// src/services/api.jsx
import axios from "axios";

const BASE_URL = process.env.REACT_APP_API_URL || "http://localhost:8000";

const api = axios.create({ baseURL: BASE_URL, timeout: 300000 });

export const summarizeDocument = async (file) => {
  const fd = new FormData();
  fd.append("file", file);
  const res = await api.post("/summarize", fd, {
    headers: { "Content-Type": "multipart/form-data" },
  });
  return res.data;
};

export const extractEntities = async (docId) => {
  const res = await api.post(`/extract-entities?doc_id=${docId}`);
  return res.data;
};

export const buildGraph = async (docId) => {
  const res = await api.post(`/build-graph?doc_id=${docId}`);
  return res.data;
};

export const queryGraph = async (docId, entity) => {
  const res = await api.get(`/graph-query?doc_id=${docId}&entity=${encodeURIComponent(entity)}`);
  return res.data;
};

// Returns ALL relationships for a document
// Equivalent to: MATCH (a:Entity {doc_id})-[r]->(b) RETURN a, r, b
export const getFullGraph = async (docId) => {
  const res = await api.get(`/graph-all?doc_id=${docId}`);
  return res.data;
};

export const verifyFacts = async (docId) => {
  const res = await api.post(`/verify-facts?doc_id=${docId}`);
  return res.data;
};

export const semanticQuery = async (docId, query) => {
  const res = await api.post(`/query?doc_id=${docId}&query=${encodeURIComponent(query)}`);
  return res.data;
};

export default api;