import {ActorQueryOperationBgpLeftDeepSmallest} from "@comunica/actor-query-operation-bgp-left-deep-smallest";
import {ActorHttp, IActionHttp, IActorHttpOutput} from "@comunica/bus-http";
import {
  ActorQueryOperation,
  ActorQueryOperationTypedMediated,
  IActorQueryOperationOutput,
  IActorQueryOperationOutputBindings,
  IActorQueryOperationTypedMediatedArgs,
} from "@comunica/bus-query-operation";
import {ActorRdfJoin, IActionRdfJoin} from "@comunica/bus-rdf-join";
import {
  DataSources,
  getDataSourceValue,
  IDataSource,
  KEY_CONTEXT_SOURCES,
} from "@comunica/bus-rdf-resolve-quad-pattern";
import {ActionContext, Actor, IActorTest, Mediator} from "@comunica/core";
import {IMediatorTypeIterations} from "@comunica/mediatortype-iterations";
import {EmptyIterator} from "asynciterator";
import {AsyncReiterableArray} from "asyncreiterable";
import {createReadStream, createWriteStream, existsSync, mkdirSync} from "fs";
import {join} from "path";
import {termToString} from "rdf-string";
import {Algebra, Factory} from "sparqlalgebrajs";
import {PassThrough} from "stream";
import stringifyStream = require("stream-to-string");

/**
 * A comunica BGP SmartKG Query Operation Actor.
 */
export class ActorQueryOperationBgpSmartkg extends ActorQueryOperationTypedMediated<Algebra.Bgp> {

  public readonly mediatorHttp: Mediator<Actor<IActionHttp, IActorTest, IActorHttpOutput>,
    IActionHttp, IActorTest, IActorHttpOutput>;
  public readonly mediatorJoin: Mediator<ActorRdfJoin,
    IActionRdfJoin, IMediatorTypeIterations, IActorQueryOperationOutput>;
  public readonly testEmptyPatterns: boolean;
  public readonly maxFamilies: number;

  private readonly cacheFolder: string;

  constructor(args: IActorQueryOperationBgpSmartkgArgs) {
    super(args, 'bgp');

    // Initialize SmartKG cache for HDT files
    this.cacheFolder = join(process.cwd(), '.smartkg-cache');
    if (!existsSync(this.cacheFolder)) {
      mkdirSync(this.cacheFolder);
    }
  }

  public static getStarPatterns(pattern: Algebra.Bgp): Algebra.Pattern[][] {
    const stars: {[subject: string]: Algebra.Pattern[]} = {};
    for (const quadPattern of pattern.patterns) {
      const subjectKey = termToString(quadPattern.subject);
      if (!stars[subjectKey]) {
        stars[subjectKey] = [];
      }
      stars[subjectKey].push(quadPattern);
    }
    return Object.keys(stars).map((key) => stars[key]);
  }

  public static isStarPatternSmartKg(patterns: Algebra.Pattern[], smartKgData: ISmartKgData): boolean {
    if (patterns.length === 1) {
      return false;
    }
    for (const pattern of patterns) {
      if (pattern.predicate.termType === 'Variable' || smartKgData.infrequentPredicates[pattern.predicate.value]) {
        return false;
      }
    }
    return true;
  }

  public static getSingleSmartKgSourceUri(context: ActionContext): string {
    // Determine all current sources
    let sourceUri: string = null;
    if (context.has(KEY_CONTEXT_SOURCES)) {
      const dataSources: DataSources = context.get(KEY_CONTEXT_SOURCES);
      let source: IDataSource;
      const it = dataSources.iterator();
      if (source = it.read()) { // tslint:disable-line:no-conditional-assignment
        const sourceValue = getDataSourceValue(source);
        if (typeof sourceValue === 'string') {
          sourceUri = sourceValue;
        } else {
          // Require a source by URI
          return null;
        }
      } else {
        // Require at least one source
        return null;
      }
      if (it.read()) {
        // We can only handle a single source
        return null;
      }
    } else {
      return null;
    }

    // Check if the source is a SmartKG source
    // TODO: do this with hypermedia in the future
    if (sourceUri.indexOf('quantum') < 0) {
      return null;
    }

    return sourceUri;
  }

  public async fetchCached(uri: string, context: ActionContext): Promise<NodeJS.ReadableStream> {
    // Determine the file location in the local file system.
    const localPath: string = join(this.cacheFolder, encodeURIComponent(uri));

    // If the file already exists, don't fetch it
    if (existsSync(localPath)) {
      return createReadStream(localPath);
    }

    // Fetch the URL
    const httpResponse: IActorHttpOutput = await this.mediatorHttp.mediate({ context, input: uri });
    const out = ActorHttp.toNodeReadable(httpResponse.body);

    // Cache the stream
    const body1 = out.pipe(new PassThrough());
    const body2 = out.pipe(new PassThrough());
    const writeStream = createWriteStream(localPath);
    body1.pipe(writeStream);

    return body2;
  }

  public async fetchHdtFile(baseUri: string, fileName: string, context: ActionContext): Promise<string> {
    // Determine the file location in the local file system.
    const localPath: string = join(this.cacheFolder, fileName);

    // If the file already exists, don't fetch it
    if (existsSync(localPath)) {
      return localPath;
    }

    // Otherwise, fetch it and store it into our cache
    const httpResponse = await this.mediatorHttp.mediate({ context, input: baseUri + '/' + fileName });
    const bodyStream = ActorHttp.toNodeReadable(httpResponse.body);
    return new Promise((resolve, reject) => {
      bodyStream
        .pipe(createWriteStream(localPath))
        .on('error', reject)
        .on('finish', () => resolve(localPath));
    });
  }

  public async getStarPatternSmartKgSources(patterns: Algebra.Pattern[], smartKgData: ISmartKgData,
                                            baseUri: string, context: ActionContext): Promise<DataSources> {
    // Determine all predicates of the star pattern
    const predicatesHash: {[predicate: string]: boolean} = {};
    for (const pattern of patterns) {
      predicatesHash[termToString(pattern.predicate)] = true;
    }
    const predicates: string[] = Object.keys(predicatesHash);

    // Determine the families that contain *at least* all the given predicates
    let families: ISmartKgFamily[] = [];
    let atLeastOneGroupedFamily: boolean = false;
    for (const family of smartKgData.families) {
      if (predicates.every((predicate) => family.predicateSet[predicate])) {
        families.push(family);
        if (family.grouped) {
          atLeastOneGroupedFamily = true;
        }
      }
    }

    this.logDebug(context, `Found ${families.length} SmartKG families.`);

    // If there is a family that is grouped, we can optimize
    if (atLeastOneGroupedFamily) {
      // Remove all non-grouped families
      families = families.filter((family) => family.grouped);

      // Determine the minimum group size within all families (group size is predicateSet size)
      // Also collect those families with the minimum group size
      let minGroupSize = Infinity;
      let minGroupSizeFamilies: ISmartKgFamily[];
      for (const family of families) {
        const groupSize = Object.keys(family.predicateSet).length;
        if (groupSize < minGroupSize) {
          minGroupSize = groupSize;
          minGroupSizeFamilies = [family];
        } else if (groupSize === minGroupSize) {
          minGroupSizeFamilies.push(family);
        }
      }

      families = minGroupSizeFamilies;

      this.logDebug(context, `Filtered down to ${families.length} SmartKG families.`);
    }

    // Optimization: skip handling when number of families is too large.
    if (families.length > this.maxFamilies) {
      this.logDebug(context, `Skipping SmartKG handling because number of families is above the threshold (${
        this.maxFamilies}).`);
      return null;
    }

    // Optimization: skip original families, as these are too large
    if (families.some((family) => family.originalFamily)) {
      this.logDebug(context, `Skipping SmartKG handling because we found an original family`);
      return null;
    }

    // Determine HDT files for all applicable families
    const hdtSources = await Promise.all(families.map(async (family) =>
      ({ type: 'hdtFile', value: await this.fetchHdtFile(baseUri, family.name, context) })));
    return AsyncReiterableArray.fromFixedData(hdtSources);
  }

  public async testOperation(pattern: Algebra.Bgp, context: ActionContext): Promise<IActorTest> {
    if (context.has(KEY_CONTEXT_SMARTKG_FLAG)) {
      throw new Error('Actor ' + this.name + ' can only operate once on SmartKG BGPs.');
    }
    if (pattern.patterns.length < 2) {
      throw new Error('Actor ' + this.name + ' can only operate on BGPs with at least two patterns.');
    }
    if (!ActorQueryOperationBgpSmartkg.getSingleSmartKgSourceUri(context)) {
      throw new Error('Actor ' + this.name + ' requires at least one SmartKG-enabled source.');
    }
    return { httpRequests: pattern.patterns.length }; // TODO: calc this based on number of HDT files + estimate TPF
  }

  public async runOperation(pattern: Algebra.Bgp, context: ActionContext): Promise<IActorQueryOperationOutput> {
    const algebraFactory = new Factory();

    // Determine SmartKG source
    const sourceUri = ActorQueryOperationBgpSmartkg.getSingleSmartKgSourceUri(context);
    const sourceUriSmartKg = sourceUri.replace('watdiv', 'molecule/watdiv'); // TODO: don't hardcode
    // TODO: fetch SmartKG metadata in test to validate earlier, and cache the response
    const smartKgDataRaw = JSON.parse(await stringifyStream(await this.fetchCached(sourceUriSmartKg, context)));
    const smartKgData: ISmartKgData = {
      // Convert predicate array to a hash for more efficient membership checking
      families: smartKgDataRaw.families.map((family: any) => {
        return {
          grouped: family.grouped,
          name: family.name,
          originalFamily: family.originalFamily,
          // Convert predicate array to a hash for more efficient membership checking
          predicateSet: family.predicateSet.reduce((acc: any, v: any) => { acc[v] = true; return acc; }, {}),
        };
      }),
      infrequentPredicates: smartKgDataRaw.infrequentPredicates
        .reduce((acc: any, v: any) => { acc[v] = true; return acc; }, {}),
    };

    // Determine stars, and distribute them to SmartKG or TPF
    const starPatternsSmartKg: Algebra.Pattern[][] = [];
    let patternsTpf: Algebra.Pattern[] = [];
    for (const starPattern of ActorQueryOperationBgpSmartkg.getStarPatterns(pattern)) {
      if (ActorQueryOperationBgpSmartkg.isStarPatternSmartKg(starPattern, smartKgData)) {
        starPatternsSmartKg.push(starPattern);
      } else {
        patternsTpf = patternsTpf.concat(starPattern);
      }
    }
    this.logDebug(context, `Identified ${starPatternsSmartKg.length} SmartKG star patterns and ${
      patternsTpf.length} remaining triple patterns.`);

    // Check if any of the triple patterns returns 0 with TPF, then return empty stream
    if (this.testEmptyPatterns) {
      // Get the total number of items for all patterns by resolving the quad patterns
      const patternOutputs: IActorQueryOperationOutputBindings[] = (await Promise.all(pattern.patterns
        .map((subPattern: Algebra.Pattern) => this.mediatorQueryOperation.mediate(
          { operation: subPattern, context }))))
        .map(ActorQueryOperation.getSafeBindings);

      // If a triple pattern has no matches, the entire graph pattern has no matches.
      if (await ActorQueryOperationBgpLeftDeepSmallest.hasOneEmptyPatternOutput(patternOutputs)) {
        return <IActorQueryOperationOutput> {
          bindingsStream: new EmptyIterator(),
          metadata: () => Promise.resolve({ totalItems: 0 }),
          type: 'bindings',
          variables: [],
        };
      }
    }

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

export interface IActorQueryOperationBgpSmartkgArgs extends IActorQueryOperationTypedMediatedArgs {
  mediatorHttp: Mediator<Actor<IActionHttp, IActorTest, IActorHttpOutput>,
    IActionHttp, IActorTest, IActorHttpOutput>;
  mediatorJoin: Mediator<ActorRdfJoin,
    IActionRdfJoin, IMediatorTypeIterations, IActorQueryOperationOutput>;
  testEmptyPatterns: boolean;
  maxFamilies: number;
}

export interface ISmartKgData {
  infrequentPredicates: {[predicate: string]: boolean};
  families: ISmartKgFamily[];
}

export interface ISmartKgFamily {
  name: string;
  predicateSet: {[predicate: string]: boolean};
  grouped: boolean;
  originalFamily: boolean;
}

/**
 * @type {string} Context flag for indicating when a BGP has been checked for SmartKG applicability.
 */
export const KEY_CONTEXT_SMARTKG_FLAG = '@comunica/actor-query-operation-bgp-smartkg:smartkg-passed';
