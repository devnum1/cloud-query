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
import mysql from 'mysql2/promise';

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
  const connection = await mysql.createConnection({
    host: 'sql5.freemysqlhosting.net',
    user: 'sql5700808',
    password: 'Gjw3Kc8QK2',
    database: 'sql5700808'
  });

  await connection.execute(`
    CREATE TABLE IF NOT EXISTS cves (
      cve_id VARCHAR(255) PRIMARY KEY,
      description TEXT,
      last_modified DATETIME,
      last_updated_at DATETIME,
      last_touched TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  return connection;
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

async function upsertCves(cveData) {
  const db = await initDB();
  const query = `
    INSERT INTO cves (cve_id, description, last_modified, last_updated_at)
    VALUES (?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
    description = VALUES(description),
    last_modified = VALUES(last_modified),
    last_updated_at = VALUES(last_updated_at),
    last_touched = CURRENT_TIMESTAMP
  `;

  for (const cve of cveData) {
    await db.execute(query, [cve.cve_id, cve.description, cve.last_modified, cve.last_updated_at]);
  }

  await db.end();
}
