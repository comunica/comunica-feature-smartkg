import {
  ActorQueryOperation,
  IActorQueryOperationOutput,
  IActorQueryOperationOutputBindings,
} from "@comunica/bus-query-operation";
import {KEY_CONTEXT_SOURCES} from "@comunica/bus-rdf-resolve-quad-pattern";
import {ActionContext} from "@comunica/core";
import {Algebra, Factory} from "sparqlalgebrajs";
import {
  ActorQueryOperationBgpSmartkgAdapter,
  ISmartKgData,
  KEY_CONTEXT_SMARTKG_FLAG,
} from "./ActorQueryOperationBgpSmartkgAdapter";

/**
 * A SmartKG BGP actor that resolves each SmartKG star pattern and the singular TPF BGP separately,
 * and joins the results of all of them afterwards.
 *
 * This actor should only be used if the number of intermediary results
 * for all star patterns is low to avoid expensive joins.
 */
export class ActorQueryOperationBgpSmartkgStarJoin extends ActorQueryOperationBgpSmartkgAdapter {

  protected async executePatterns(starPatternsSmartKg: Algebra.Pattern[][], patternsTpf: Algebra.Pattern[],
                                  smartKgData: ISmartKgData, sourceUriSmartKg: string, context: ActionContext,
                                  patternOriginal: Algebra.Bgp): Promise<IActorQueryOperationOutput> {
    const algebraFactory = new Factory();

    // Execute each SmartKG star over the appropriate HDT files
    const starResults: Promise<IActorQueryOperationOutputBindings>[] = [];
    for (const starPattern of starPatternsSmartKg) {
      // Determine the HDT files
      const sources = await this.getStarPatternSmartKgSources(starPattern, smartKgData, sourceUriSmartKg, context);
      if (!sources) {
        // If we received null, then this means that we are better off with delegating to TPF.
        patternsTpf = patternsTpf.concat(starPattern);
      }

      // Create an execute the star as a BGP over the given sources
      const contextSmartKg = context.set(KEY_CONTEXT_SOURCES, sources);
      const bgp = algebraFactory.createBgp(starPattern);
      starResults.push(this.mediatorQueryOperation.mediate({ operation: bgp, context: contextSmartKg })
        .then(ActorQueryOperation.getSafeBindings));
    }

    // Execute all remaining patterns using TPF
    if (patternsTpf.length > 0) {
      const contextTpf = context.set(KEY_CONTEXT_SMARTKG_FLAG, true);
      const bgpTpf = algebraFactory.createBgp(patternsTpf);
      starResults.push(this.mediatorQueryOperation.mediate({ operation: bgpTpf, context: contextTpf })
        .then(ActorQueryOperation.getSafeBindings));
    }

    // Join the results of both
    return this.mediatorJoin.mediate({ entries: await Promise.all(starResults) });
  }

}
