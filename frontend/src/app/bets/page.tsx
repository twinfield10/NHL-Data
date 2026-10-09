import { redirect } from "next/navigation";

export default function BetsIndex() {
  redirect("/bets/markets");
}
