import {IActorQueryOperationOutput} from "@comunica/bus-query-operation";
import {KEY_CONTEXT_SOURCES} from "@comunica/bus-rdf-resolve-quad-pattern";
import {ActionContext} from "@comunica/core";
import {Algebra} from "sparqlalgebrajs";
import {
  ActorQueryOperationBgpSmartkgAdapter,
  ISmartKgData,
  KEY_CONTEXT_SMARTKG_FLAG,
} from "./ActorQueryOperationBgpSmartkgAdapter";

/**
 * A SmartKG BGP actor that attaches a source (HDT file or TPF interface) to each quad pattern,
 * and throws the modified operation back on the query operation bus,
 * so that it can be further executed by the usual BGP actors (possibly in a left-deep manner).
 */
export class ActorQueryOperationBgpSmartkgDeepJoin extends ActorQueryOperationBgpSmartkgAdapter {

  protected async executePatterns(starPatternsSmartKg: Algebra.Pattern[][], patternsTpf: Algebra.Pattern[],
                                  smartKgData: ISmartKgData, sourceUriSmartKg: string, context: ActionContext,
                                  patternOriginal: Algebra.Bgp): Promise<IActorQueryOperationOutput> {
    // Apply quad-pattern-specific contexts with dedicated sources
    for (const starPattern of starPatternsSmartKg) {
      // Determine the HDT files
      const sources = await this.getStarPatternSmartKgSources(starPattern, smartKgData, sourceUriSmartKg, context);
      if (!sources) {
        // If we received null, then this means that we are better off with delegating to TPF.
        patternsTpf = patternsTpf.concat(starPattern);
      }

      const contextSmartKg = context.set(KEY_CONTEXT_SOURCES, sources);
      for (const quadPattern of starPattern) {
        quadPattern.context = contextSmartKg;
      }
    }
    const contextTpf = context.set(KEY_CONTEXT_SMARTKG_FLAG, true);
    for (const quadPattern of patternsTpf) {
      quadPattern.context = contextTpf;
    }

    // Forward this (modified) BGP action to the next BGP actor
    return this.mediatorQueryOperation.mediate({
      context: context.set(KEY_CONTEXT_SMARTKG_FLAG, true),
      operation: patternOriginal,
    });
  }

}
