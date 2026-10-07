import { pct } from "@/lib/format";

interface ProbBarProps {
  pHome: number;
  pMarket?: number | null;
  awayColor?: string;
  homeColor?: string;
  label?: string;
}

/** Away | home split of the model's win probability, with a tick at the market's split. */
export default function ProbBar({ pHome, pMarket, awayColor, homeColor, label }: ProbBarProps) {
  return (
    <div>
      <div className="mb-1 flex justify-between text-xs text-muted-foreground tabular">
        <span>{pct(1 - pHome)}</span>
        {label && <span className="text-[11px]">{label}</span>}
        <span>{pct(pHome)}</span>
      </div>
      <div className="relative flex h-2 overflow-hidden rounded-full bg-muted">
        <div className="bg-away" style={{ width: `${(1 - pHome) * 100}%`, backgroundColor: awayColor }} />
        <div className="bg-accent" style={{ width: `${pHome * 100}%`, backgroundColor: homeColor }} />
        {pMarket != null && (
          <div
            className="absolute top-0 h-full w-0.5 bg-foreground"
            style={{ left: `calc(${(1 - pMarket) * 100}% - 1px)` }}
            title={`Market: away ${pct(1 - pMarket)} / home ${pct(pMarket)}`}
          />
        )}
      </div>
    </div>
  );
}
