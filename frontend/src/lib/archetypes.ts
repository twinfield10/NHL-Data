/** Forward archetypes (archetypes plan phase B): short codes and badge colors. */

export const ARCHETYPES: Record<string, { code: string; className: string; blurb: string }> = {
  "skill winger": {
    code: "SW", className: "bg-sky-500/15 text-sky-700 dark:text-sky-300",
    blurb: "Winger who shoots a lot, from range, with a wrist/snap release; light defensive role",
  },
  "balanced winger": {
    code: "BW", className: "bg-slate-500/15 text-slate-700 dark:text-slate-300",
    blurb: "Winger near average on every axis",
  },
  "power forward": {
    code: "PF", className: "bg-amber-500/15 text-amber-700 dark:text-amber-300",
    blurb: "Big, physical winger: hits, gets hit, takes and draws penalties",
  },
  "offensive centre": {
    code: "OC", className: "bg-violet-500/15 text-violet-700 dark:text-violet-300",
    blurb: "Takes faceoffs; little PK or defensive-zone work",
  },
  "two-way centre": {
    code: "2C", className: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300",
    blurb: "Takes faceoffs; blocks shots, starts in his own zone and kills penalties",
  },
};

export const archetypeInfo = (name: string | null | undefined) => (name ? ARCHETYPES[name] : undefined);
