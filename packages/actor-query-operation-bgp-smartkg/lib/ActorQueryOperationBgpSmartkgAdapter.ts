import { createReadStream, createWriteStream, existsSync, mkdirSync } from 'fs';
import { join } from 'path';
import { PassThrough } from 'stream';
import { ActorQueryOperationBgpLeftDeepSmallest } from '@comunica/actor-query-operation-bgp-left-deep-smallest';
import type { IActionHttp, IActorHttpOutput } from '@comunica/bus-http';
import { ActorHttp } from '@comunica/bus-http';
import type { IActorQueryOperationOutput,
  IActorQueryOperationOutputBindings,
  IActorQueryOperationTypedMediatedArgs } from '@comunica/bus-query-operation';
import {
  ActorQueryOperation,
  ActorQueryOperationTypedMediated,
} from '@comunica/bus-query-operation';
import type { ActorRdfJoin, IActionRdfJoin } from '@comunica/bus-rdf-join';
import type { DataSources } from '@comunica/bus-rdf-resolve-quad-pattern';
import {
  getDataSourceValue,
  KEY_CONTEXT_SOURCES,
} from '@comunica/bus-rdf-resolve-quad-pattern';
import type { ActionContext, Actor, IActorTest, Mediator } from '@comunica/core';
import type { IMediatorTypeIterations } from '@comunica/mediatortype-iterations';
import { EmptyIterator } from 'asynciterator';
import { termToString } from 'rdf-string';
import type { Algebra } from 'sparqlalgebrajs';
import stringifyStream = require('stream-to-string');

/**
 * A comunica BGP SmartKG Query Operation Actor.
 */
export abstract class ActorQueryOperationBgpSmartkgAdapter extends ActorQueryOperationTypedMediated<Algebra.Bgp> {
  public readonly mediatorHttp: Mediator<Actor<IActionHttp, IActorTest, IActorHttpOutput>,
  IActionHttp, IActorTest, IActorHttpOutput>;

  public readonly mediatorJoin: Mediator<ActorRdfJoin,
  IActionRdfJoin, IMediatorTypeIterations, IActorQueryOperationOutput>;

  public readonly testEmptyPatterns: boolean;
  public readonly maxFamilies: number;
  public readonly fetchHdtIndexFiles: boolean;

  private readonly cacheFolder: string;
  private readonly smartKgSources: Record<string, string>;

  public constructor(args: IActorQueryOperationBgpSmartkgArgs) {
    super(args, 'bgp');

    // Initialize SmartKG cache for HDT files
    this.cacheFolder = join(process.cwd(), '.smartkg-cache');
    if (!existsSync(this.cacheFolder)) {
      mkdirSync(this.cacheFolder);
    }
    this.smartKgSources = {};
  }

  /**
   * Determine all star patterns inside the given BGP.
   * @param {Bgp} pattern a BGP pattern.
   * @return {Pattern[][]} A double array of quad patterns, grouped as separate star pattern.
   */
  public static getStarPatterns(pattern: Algebra.Bgp): Algebra.Pattern[][] {
    const stars: Record<string, Algebra.Pattern[]> = {};
    for (const quadPattern of pattern.patterns) {
      const subjectKey = termToString(quadPattern.subject);
      if (!stars[subjectKey]) {
        stars[subjectKey] = [];
      }
      stars[subjectKey].push(quadPattern);
    }
    return Object.keys(stars).map(key => stars[key]);
  }

  /**
   * Check if the given star pattern should be handled by SmartKG.
   * @param {Pattern[]} patterns A star pattern.
   * @param {ISmartKgData} smartKgData A SmartKG index.
   * @return {boolean} If the given star pattern should be handled by SmartKG.
   */
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

  /**
   * If present, get the SmartKG source from the current context.
   * If multiple sources are present, or a non-SmartKG source is present, then null will be returned.
   * @param {ActionContext} context A context, possibly containing a SmartKG source.
   * @return {string} A SmartKG source URI.
   */
  public getSingleSmartKgSourceUri(context: ActionContext): string | undefined {
    // Determine the current source
    if (context.has(KEY_CONTEXT_SOURCES)) {
      const dataSources: DataSources = context.get(KEY_CONTEXT_SOURCES);
      // Only allow a single source
      if (dataSources.length === 1) {
        const sourceValue = getDataSourceValue(dataSources[0]);
        // Require a source by URI
        if (typeof sourceValue === 'string') {
          return this.getSmartKgSource(sourceValue);
        }
      }
    }
  }

  /**
   * Determine the SmartKG source URI from the given (TPF) source URI.
   * @param {string} sourceUri A TPF source URI.
   * @return {string} A SmartKG source URI, or undefined if it is not a SmartKG source.
   */
  public getSmartKgSource(sourceUri: string): string | undefined {
    // If cached, return from cache
    if (sourceUri in this.smartKgSources) {
      return this.smartKgSources[sourceUri];
    }

    // TODO: do this with hypermedia in the future (fetch page, and find ex:smartKgIndex predicate link)
    let smartKgSourceUri: string | undefined;
    if (sourceUri.includes('quantum')) {
      smartKgSourceUri = sourceUri.replace('watdiv', 'molecule/watdiv');
      this.smartKgSources[sourceUri] = smartKgSourceUri;
    }

    return smartKgSourceUri;
  }

  /**
   * Either retrieve the given URI from local cache,
   * or fetch and cache it.
   *
   * It will return the cached file name.
   *
   * @param {string} uri A URI.
   * @param {ActionContext} context A context.
   * @return {Promise<string>} A promise resolving to the cached file name.
   */
  public async fetchCachedLocation(uri: string, context: ActionContext): Promise<string> {
    // Determine the file location in the local file system.
    const localPath: string = join(this.cacheFolder, encodeURIComponent(uri));

    // If the file already exists, don't fetch it
    if (existsSync(localPath)) {
      return localPath;
    }

    // Fetch the URL
    const httpResponse: IActorHttpOutput = await this.mediatorHttp.mediate({ context, input: uri });
    const out = ActorHttp.toNodeReadable(httpResponse.body);

    return new Promise<string>((resolve, reject) => {
      out.pipe(createWriteStream(localPath))
        .on('error', reject)
        .on('finish', () => resolve(localPath));
    });
  }

  /**
   * Either retrieve the given URI from local cache,
   * or fetch and cache it.
   *
   * It will return a read stream to the cached file.
   *
   * @param {string} uri A URI.
   * @param {ActionContext} context A context.
   * @return {Promise<NodeJS.ReadableStream>} A promise resolving to the cached or fetched stream.
   */
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

  /**
   * Either retrieve the given HDT file from local cache,
   * or fetch and cache it.
   * @param {string} baseUri A base IRI.
   * @param {string} fileName A relative HDT file name.
   * @param {ActionContext} context A context.
   * @return {Promise<string>} A promise resolving to a local file name.
   */
  public async fetchHdtFile(baseUri: string, fileName: string, context: ActionContext): Promise<string> {
    // Fetch HDT index file
    const fetchIndexPromise: Promise<string> = this.fetchHdtIndexFiles ?
      this.fetchCachedLocation(`${baseUri}/${fileName}.index.v1-1`, context) :
      new Promise(resolve => resolve(''));

    // Fetch HDT file
    const fetchHdtPromise = this.fetchCachedLocation(`${baseUri}/${fileName}`, context);

    const [ _, localPath ] = await Promise.all([ fetchIndexPromise, fetchHdtPromise ]);
    return localPath;
  }

  /**
   * Determine the HDT data sources for the given SmartKG star pattern.
   * @param {Pattern[]} patterns A star pattern containing quad patterns.
   * @param {ISmartKgData} smartKgData A SmartKG index.
   * @param {string} baseUri The SmartKG base URI.
   * @param {ActionContext} context A context.
   * @return {Promise<DataSources>} A promise resolving to HDT data source definitions.
   */
  public async getStarPatternSmartKgSources(patterns: Algebra.Pattern[], smartKgData: ISmartKgData,
    baseUri: string, context: ActionContext): Promise<DataSources | undefined> {
    // Determine all predicates of the star pattern
    const predicatesHash: Record<string, boolean> = {};
    for (const pattern of patterns) {
      predicatesHash[termToString(pattern.predicate)] = true;
    }
    const predicates: string[] = Object.keys(predicatesHash);

    // Determine the families that contain *at least* all the given predicates
    let families: ISmartKgFamily[] = [];
    let atLeastOneGroupedFamily = false;
    for (const family of smartKgData.families) {
      if (predicates.every(predicate => family.predicateSet[predicate])) {
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
      families = families.filter(family => family.grouped);

      // Determine the minimum group size within all families (group size is predicateSet size)
      // Also collect those families with the minimum group size
      let minGroupSize = Infinity;
      let minGroupSizeFamilies: ISmartKgFamily[] = [];
      for (const family of families) {
        const groupSize = Object.keys(family.predicateSet).length;
        if (groupSize < minGroupSize) {
          minGroupSize = groupSize;
          minGroupSizeFamilies = [ family ];
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
      return;
    }

    // Optimization: skip original families, as these are too large
    if (families.some(family => family.originalFamily)) {
      this.logDebug(context, `Skipping SmartKG handling because we found an original family`);
      return;
    }

    // Determine HDT files for all applicable families
    const hdtSources = await Promise.all(families.map(async family =>
      ({ type: 'hdtFile', value: await this.fetchHdtFile(baseUri, family.name, context) })));
    return hdtSources;
  }

  public async testOperation(pattern: Algebra.Bgp, context: ActionContext): Promise<IActorTest> {
    if (context.has(KEY_CONTEXT_SMARTKG_FLAG)) {
      throw new Error(`Actor ${this.name} can only operate once on SmartKG BGPs.`);
    }
    if (pattern.patterns.length < 2) {
      throw new Error(`Actor ${this.name} can only operate on BGPs with at least two patterns.`);
    }
    if (!this.getSingleSmartKgSourceUri(context)) {
      throw new Error(`Actor ${this.name} requires at least one SmartKG-enabled source.`);
    }
    // TODO: calc this based on number of HDT files + estimate TPF
    return { httpRequests: pattern.patterns.length };
  }

  public async runOperation(pattern: Algebra.Bgp, context: ActionContext): Promise<IActorQueryOperationOutput> {
    // Determine SmartKG source
    // Guaranteed to be defined due to our testOperation, so we can cast
    const sourceUriSmartKg = <string> this.getSingleSmartKgSourceUri(context);
    const smartKgDataRaw = JSON.parse(await stringifyStream(await this.fetchCached(sourceUriSmartKg, context)));
    const smartKgData: ISmartKgData = {
      // Convert predicate array to a hash for more efficient membership checking
      families: smartKgDataRaw.families.map((family: any) => ({
        grouped: family.grouped,
        name: family.name,
        originalFamily: family.originalFamily,
        // Convert predicate array to a hash for more efficient membership checking
        predicateSet: family.predicateSet.reduce((acc: any, value: any) => {
          acc[value] = true;
          return acc;
        }, {}),
      })),
      infrequentPredicates: smartKgDataRaw.infrequentPredicates
        .reduce((acc: any, value: any) => {
          acc[value] = true;
          return acc;
        }, {}),
    };

    // Determine stars, and distribute them to SmartKG or TPF
    const starPatternsSmartKg: Algebra.Pattern[][] = [];
    let patternsTpf: Algebra.Pattern[] = [];
    for (const starPattern of ActorQueryOperationBgpSmartkgAdapter.getStarPatterns(pattern)) {
      if (ActorQueryOperationBgpSmartkgAdapter.isStarPatternSmartKg(starPattern, smartKgData)) {
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
          { operation: subPattern, context },
        ))))
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

      // Otherwise, close all streams
      for (const patternOutput of patternOutputs) {
        patternOutput.bindingsStream.close();
      }
    }

    return await this.executePatterns(
      starPatternsSmartKg,
      patternsTpf,
      smartKgData,
      sourceUriSmartKg,
      context,
      pattern,
    );
  }

  protected abstract executePatterns(starPatternsSmartKg: Algebra.Pattern[][], patternsTpf: Algebra.Pattern[],
    smartKgData: ISmartKgData, sourceUriSmartKg: string, context: ActionContext,
    patternOriginal: Algebra.Bgp): Promise<IActorQueryOperationOutput>;
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
  infrequentPredicates: Record<string, boolean>;
  families: ISmartKgFamily[];
}

export interface ISmartKgFamily {
  name: string;
  predicateSet: Record<string, boolean>;
  grouped: boolean;
  originalFamily: boolean;
}

/**
 * @type {string} Context flag for indicating when a BGP has been checked for SmartKG applicability.
 */
export const KEY_CONTEXT_SMARTKG_FLAG = '@comunica/actor-query-operation-bgp-smartkg:smartkg-passed';
