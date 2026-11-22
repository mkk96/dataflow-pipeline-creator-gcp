/** Print src/config/runtime.ts as JSON so CI (jq) uses the same versions as the app. */
import { RUNTIME } from "../src/config/runtime";

process.stdout.write(JSON.stringify(RUNTIME) + "\n");
