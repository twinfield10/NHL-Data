// NHL team names and colors (primary, secondary). Logos come from the NHL's CDN.

export interface Team {
  name: string;
  primary: string;
  secondary: string;
}

export const TEAMS: Record<string, Team> = {
  ANA: { name: "Anaheim Ducks", primary: "#F47A38", secondary: "#B9975B" },
  BOS: { name: "Boston Bruins", primary: "#FFB81C", secondary: "#000000" },
  BUF: { name: "Buffalo Sabres", primary: "#003087", secondary: "#FFB81C" },
  CAR: { name: "Carolina Hurricanes", primary: "#CE1126", secondary: "#A4A9AD" },
  CBJ: { name: "Columbus Blue Jackets", primary: "#002654", secondary: "#CE1126" },
  CGY: { name: "Calgary Flames", primary: "#C8102E", secondary: "#F1BE48" },
  CHI: { name: "Chicago Blackhawks", primary: "#CF0A2C", secondary: "#FF671B" },
  COL: { name: "Colorado Avalanche", primary: "#6F263D", secondary: "#236192" },
  DAL: { name: "Dallas Stars", primary: "#006847", secondary: "#8F8F8C" },
  DET: { name: "Detroit Red Wings", primary: "#CE1126", secondary: "#FFFFFF" },
  EDM: { name: "Edmonton Oilers", primary: "#041E42", secondary: "#FF4C00" },
  FLA: { name: "Florida Panthers", primary: "#C8102E", secondary: "#B9975B" },
  LAK: { name: "Los Angeles Kings", primary: "#111111", secondary: "#A2AAAD" },
  MIN: { name: "Minnesota Wild", primary: "#154734", secondary: "#A6192E" },
  MTL: { name: "Montréal Canadiens", primary: "#AF1E2D", secondary: "#192168" },
  NJD: { name: "New Jersey Devils", primary: "#CE1126", secondary: "#000000" },
  NSH: { name: "Nashville Predators", primary: "#FFB81C", secondary: "#041E42" },
  NYI: { name: "New York Islanders", primary: "#00539B", secondary: "#F47D30" },
  NYR: { name: "New York Rangers", primary: "#0038A8", secondary: "#CE1126" },
  OTT: { name: "Ottawa Senators", primary: "#C52032", secondary: "#C2912C" },
  PHI: { name: "Philadelphia Flyers", primary: "#F74902", secondary: "#000000" },
  PIT: { name: "Pittsburgh Penguins", primary: "#000000", secondary: "#FCB514" },
  SEA: { name: "Seattle Kraken", primary: "#001628", secondary: "#99D9D9" },
  SJS: { name: "San Jose Sharks", primary: "#006D75", secondary: "#EA7200" },
  STL: { name: "St. Louis Blues", primary: "#002F87", secondary: "#FCB514" },
  TBL: { name: "Tampa Bay Lightning", primary: "#002868", secondary: "#FFFFFF" },
  TOR: { name: "Toronto Maple Leafs", primary: "#00205B", secondary: "#FFFFFF" },
  UTA: { name: "Utah Mammoth", primary: "#6CACE4", secondary: "#010101" },
  VAN: { name: "Vancouver Canucks", primary: "#00205B", secondary: "#00843D" },
  VGK: { name: "Vegas Golden Knights", primary: "#B4975A", secondary: "#333F42" },
  WPG: { name: "Winnipeg Jets", primary: "#041E42", secondary: "#AC162C" },
  WSH: { name: "Washington Capitals", primary: "#C8102E", secondary: "#041E42" },
};

const FALLBACK: Team = { name: "", primary: "#64748b", secondary: "#94a3b8" };

export const team = (abbr: string | null | undefined): Team => (abbr && TEAMS[abbr]) || { ...FALLBACK, name: abbr ?? "" };

export const logoUrl = (abbr: string, variant: "light" | "dark") =>
  `https://assets.nhle.com/logos/nhl/svg/${abbr}_${variant}.svg`;

function rgb(hex: string): [number, number, number] {
  const n = parseInt(hex.slice(1), 16);
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
}

/** WCAG relative luminance, 0 (black) to 1 (white). */
function luminance(hex: string): number {
  const [r, g, b] = rgb(hex).map((c) => {
    const s = c / 255;
    return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function distance(a: string, b: string): number {
  const [x, y] = [rgb(a), rgb(b)];
  return Math.hypot(x[0] - y[0], x[1] - y[1], x[2] - y[2]);
}

/** Blend a color toward white (amount 0..1). */
function lighten(hex: string, amount: number): string {
  const mixed = rgb(hex).map((c) => Math.round(c + (255 - c) * amount));
  return `#${mixed.map((c) => c.toString(16).padStart(2, "0")).join("")}`;
}

/**
 * A color that reads on the current background: the primary unless it vanishes there, then
 * the secondary unless that is near-white (a white bar reads as empty), else a lightened primary.
 */
function visible(t: Team, dark: boolean): string {
  const ok = (c: string) => (dark ? luminance(c) > 0.04 && luminance(c) < 0.8 : luminance(c) < 0.8);
  if (ok(t.primary)) return t.primary;
  if (ok(t.secondary)) return t.secondary;
  return dark ? lighten(t.primary, 0.35) : "#475569";
}

/** One team's color, readable on the current background. */
export const teamColor = (abbr: string, dark: boolean) => visible(team(abbr), dark);

/** Hex color with an alpha channel, for gradients. */
export const alpha = (hex: string, a: number) => `${hex}${Math.round(a * 255).toString(16).padStart(2, "0")}`;

/** Bar colors for a matchup; the away team falls back to its secondary when the two clash. */
export function matchupColors(away: string, home: string, dark: boolean): { away: string; home: string } {
  const a = team(away);
  const h = team(home);
  const homeColor = visible(h, dark);
  let awayColor = visible(a, dark);
  if (distance(awayColor, homeColor) < 90) {
    const alt = visible({ ...a, primary: a.secondary, secondary: a.primary }, dark);
    awayColor = distance(alt, homeColor) >= 90 ? alt : dark ? "#94a3b8" : "#475569";
  }
  return { away: awayColor, home: homeColor };
}
