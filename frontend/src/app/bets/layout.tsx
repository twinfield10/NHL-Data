import SubNav from "@/components/SubNav";

const TABS = [
  { href: "/bets/markets", label: "Game Markets" },
  { href: "/bets/props", label: "Prop Markets" },
] as const;

/** Today's board: every edge at the current best price, placed or not. */
export default function BetsLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <>
      <SubNav items={TABS} />
      {children}
    </>
  );
}
