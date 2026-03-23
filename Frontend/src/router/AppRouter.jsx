// src/router/AppRouter.js
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import Layout       from "../components/Layout";
import HomePage     from "../pages/HomePage";
import SummaryPage  from "../pages/SummaryPage";
import EntitiesPage from "../pages/EntitiesPage";
import GraphPage    from "../pages/GraphPage";
import FactCheckPage from "../pages/FactCheckPage";
import QueryPage    from "../pages/QueryPage";

const AppRouter = () => (
  <BrowserRouter>
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index              element={<HomePage />} />
        <Route path="summary"    element={<SummaryPage />} />
        <Route path="entities"   element={<EntitiesPage />} />
        <Route path="graph"      element={<GraphPage />} />
        <Route path="factcheck"  element={<FactCheckPage />} />
        <Route path="query"      element={<QueryPage />} />
        <Route path="*"          element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  </BrowserRouter>
);

export default AppRouter;
