/** "Suggest names": fill naming fields from org / env / domain / source / target (docs/standards.md). */
import { toToken } from "../config/naming";
import { str } from "../scenarios/common";
import type { NamingContext, Params, Scenario } from "../scenarios/types";

export function namingContext(s: Scenario, p: Params): NamingContext {
  return {
    org: toToken(str(p, "org")),
    env: str(p, "env"),
    domain: toToken(str(p, "domain")),
    source: s.naming.source,
    target: s.naming.target,
  };
}

export function suggestNames(s: Scenario, p: Params): Partial<Params> {
  const c = namingContext(s, p);
  if (!c.org || !c.env || !c.domain) return {};
  return {
    pipeline_name: `${c.domain}_${c.source}_to_${c.target}`,
    job_name: `${c.env}-${c.domain}-${c.source}-to-${c.target}`,
    temp_bucket: `${c.org}-${c.env}-dataflow-temp`,
    templates_bucket: `${c.org}-${c.env}-${c.domain}-templates`,
    ...(s.suggest?.(c, p) ?? {}),
  };
}
