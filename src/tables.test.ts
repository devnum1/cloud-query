import test from "ava";

import { getTables } from "./tables.js";

test("getTables returns a single table with two columns", async (t) => {
  const [table] = await getTables();
  t.is(table.columns[0].name, "First Name");
  t.is(table.columns[1].name, "Last Name");
});

test("getTables returns a single table named 'Names'", async (t) => {
  const [table] = await getTables();
  t.is(table.name, "Names");
});
