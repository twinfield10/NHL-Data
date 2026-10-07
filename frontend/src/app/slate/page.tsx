import { redirect } from "next/navigation";

/** Old URL: the slate is the Games page now. */
export default async function SlateRedirect({ searchParams }: { searchParams: Promise<{ date?: string }> }) {
  const { date } = await searchParams;
  redirect(date ? `/games?date=${date}` : "/games");
}
