import axios from "axios";
import { Utf8 } from "@cloudquery/plugin-sdk-javascript/arrow";
import type { Column, ColumnResolver } from "@cloudquery/plugin-sdk-javascript/schema/column";
import type { Table, TableResolver } from "@cloudquery/plugin-sdk-javascript/schema/table";
import { createTable } from "@cloudquery/plugin-sdk-javascript/schema/table";
import dayjs from "dayjs";
import customParseFormat from "dayjs/plugin/customParseFormat.js";
import localizedFormat from "dayjs/plugin/localizedFormat.js";
import timezone from "dayjs/plugin/timezone.js";
import utc from "dayjs/plugin/utc.js";
import { open } from 'sqlite';
import sqlite3 from 'sqlite3';

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(customParseFormat);
dayjs.extend(localizedFormat);

const getColumnResolver = (columnName: string): ColumnResolver => {
  return (meta, resource) => {
    const dataItem = resource.getItem();
    resource.setColumData(columnName, (dataItem as Record<string, unknown>)[columnName]);
    return Promise.resolve();
  };
};

async function initDB() {
  const db = await open({ filename: './cve_database.db', driver: sqlite3.Database });
  await db.exec(`CREATE TABLE IF NOT EXISTS cves (
      cve_id TEXT PRIMARY KEY,
      description TEXT,
      last_modified TEXT,
      last_updated_at TEXT,
      last_touched TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
  )`);
  return db;
}

async function downloadCVEData() {
  try {
    const response = await axios.get("https://services.nvd.nist.gov/rest/json/cves/2.0");
    return response.data.result.CVE_Items || [];
  } catch (error) {
    console.error("Failed to download CVE data:", error);
    throw error;
  }
}

const getTable = async (): Promise<Table> => {
  const cves = await downloadCVEData();
  const columnNames = ["cve_id", "description", "last_modified", "last_updated_at"];
  const columnDefinitions: Column[] = columnNames.map(column => ({
    name: column,
    type: new Utf8(),
    description: "",
    primaryKey: column === "cve_id",
    notNull: column === "cve_id",
    incrementalKey: false,
    unique: column === "cve_id",
    ignoreInTests: false,
    resolver: getColumnResolver(column),
  }));

  const tableResolver: TableResolver = (clientMeta, parent, stream) => {
    for (const r of cves) stream.write(r);
    return Promise.resolve();
  };
  return createTable({ name: "CVE", columns: columnDefinitions, resolver: tableResolver });
};

export const getTables = async (
  ): Promise<Table[]> => {
   
    const table = await getTable();
    return [table];
};

async function upsertCves(cveData: any) {
  const db = await initDB();
  const stmt = await db.prepare(`INSERT INTO cves (cve_id, description, last_modified, last_updated_at) 
                                 VALUES (?, ?, ?, ?) 
                                 ON CONFLICT(cve_id) DO UPDATE SET 
                                 description = excluded.description, 
                                 last_modified = excluded.last_modified, 
                                 last_updated_at = excluded.last_updated_at,
                                 last_touched = CURRENT_TIMESTAMP`);
  for (const cve of cveData) {
      await stmt.run(cve.cve_id, cve.description, cve.last_modified, cve.last_updated_at);
  }
  await stmt.finalize();
  await db.close();
}
