import { createServeCommand } from "@cloudquery/plugin-sdk-javascript/plugin/serve";

import { newSamplePlugin } from "./plugin.js";

const main = () => {
  createServeCommand(newSamplePlugin()).parse();
};

main();
