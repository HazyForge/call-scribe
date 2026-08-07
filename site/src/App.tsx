import SiteFooter from "./components/SiteFooter";
import SiteHeader from "./components/SiteHeader";
import HomePage from "./pages/HomePage";

export default function App() {
  return (
    <div className="shell">
      <SiteHeader />
      <HomePage />
      <SiteFooter />
    </div>
  );
}
