import SubNav from "@/components/SubNav";

const TABS = [
  { href: "/ratings/teams", label: "Teams" },
  { href: "/ratings/lines", label: "Lines" },
  { href: "/ratings/players", label: "Players" },
] as const;

export default function RatingsLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <>
      <SubNav items={TABS} />
      {children}
    </>
  );
}
