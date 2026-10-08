import SubNav from "@/components/SubNav";

const TABS = [
  { href: "/results/edges", label: "Edges" },
  { href: "/results/bets", label: "Bets" },
  { href: "/results/props", label: "Props" },
] as const;

export default function ResultsLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <>
      <SubNav items={TABS} />
      {children}
    </>
  );
}
