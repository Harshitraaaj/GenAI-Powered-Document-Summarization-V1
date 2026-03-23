/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{js,jsx}"],
  theme: {
    extend: {
      colors: {
        bg:       "#0f0f0f",
        surface:  "#161616",
        surface2: "#1e1e1e",
        border:   "#272727",
        border2:  "#333333",
        dim:      "#888888",
        faint:    "#444444",
        amber:    "#f0a500",
        "amber-glow": "#f0a50020",
        jade:     "#4caf82",
        "jade-glow": "#4caf8220",
        sky:      "#4a9eff",
        "sky-glow": "#4a9eff18",
        rose:     "#e05c5c",
        "rose-glow": "#e05c5c18",
        ink:      "#e8e4dc",
      },
      fontFamily: {
        mono:    ["'DM Mono'", "monospace"],
        display: ["'Syne'", "sans-serif"],
      },
      keyframes: {
        spin:    { to: { transform: "rotate(360deg)" } },
        fadeUp:  { from: { opacity: 0, transform: "translateY(12px)" }, to: { opacity: 1, transform: "translateY(0)" } },
        pulse2:  { "0%,100%": { opacity: 1 }, "50%": { opacity: 0.3 } },
      },
      animation: {
        spin:    "spin 1s linear infinite",
        fadeUp:  "fadeUp 0.25s ease forwards",
        pulse2:  "pulse2 2s ease infinite",
      },
    },
  },
  plugins: [],
};
