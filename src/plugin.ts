import type {
  NewClientFunction,
  TableOptions,
  SyncOptions,
  Plugin,
} from "@cloudquery/plugin-sdk-javascript/plugin/plugin";
import {
  newPlugin,
  newUnimplementedDestination,
} from "@cloudquery/plugin-sdk-javascript/plugin/plugin";
import { sync } from "@cloudquery/plugin-sdk-javascript/scheduler";
import type { Table } from "@cloudquery/plugin-sdk-javascript/schema/table";
import { filterTables } from "@cloudquery/plugin-sdk-javascript/schema/table";
import { readPackageUp } from "read-pkg-up";

import { parseSpec } from "./spec.js";
import type { Spec } from "./spec.js";
import { getTables } from "./tables.js";

const {
  packageJson: { version },
} = (await readPackageUp()) || { packageJson: { version: "development" } };

type FileClient = {
  id: () => string;
};

export const newSamplePlugin = () => {
  const pluginClient = {
    ...newUnimplementedDestination(),
    plugin: null as unknown as Plugin,
    spec: null as unknown as Spec,
    client: null as unknown as FileClient | null,
    allTables: null as unknown as Table[],
    close: () => Promise.resolve(),
    tables: ({ tables, skipTables, skipDependentTables }: TableOptions) => {
      const { allTables } = pluginClient;
      const filtered = filterTables(
        allTables,
        tables,
        skipTables,
        skipDependentTables,
      );
      return Promise.resolve(filtered);
    },
    sync: (options: SyncOptions) => {
      const { client, allTables, plugin } = pluginClient;

      if (client === null) {
        return Promise.reject(new Error("Client not initialized"));
      }

      const logger = plugin.getLogger();
      const {
        spec: { concurrency },
      } = pluginClient;

      const {
        stream,
        tables,
        skipTables,
        skipDependentTables,
        deterministicCQId,
      } = options;
      const filtered = filterTables(
        allTables,
        tables,
        skipTables,
        skipDependentTables,
      );

      return sync({
        logger,
        client,
        stream,
        tables: filtered,
        deterministicCQId,
        concurrency,
      });
    },
  };

  const newClient: NewClientFunction = async (
    logger,
    spec,
    { noConnection },
  ) => {
    if (noConnection) {
      pluginClient.allTables = [];
      return pluginClient;
    }
    pluginClient.spec = parseSpec(spec);
    pluginClient.client = { id: () => "cq-js-sample" };
    pluginClient.allTables = await getTables();

    return pluginClient;
  };

  pluginClient.plugin = newPlugin("cq-js-sample", version, newClient, {
    kind: "source",
    team: "cloudquery",
  });

  return pluginClient.plugin;
};
