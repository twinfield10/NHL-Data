import SubNav from "@/components/SubNav";

const TABS = [
  { href: "/results/markets", label: "Game Markets" },
  { href: "/results/props", label: "Props" },
] as const;

/** The ledgers: bets placed, pending and graded. */
export default function ResultsLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <>
      <SubNav items={TABS} />
      {children}
    </>
  );
}
