import { ActorQueryOperation, Bindings } from '@comunica/bus-query-operation';
import { Bus } from '@comunica/core';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { ActorQueryOperationBgpSmartkgAdapter } from '../lib/ActorQueryOperationBgpSmartkgAdapter';

const DF = new DataFactory();

describe('ActorQueryOperationBgpSmartkg', () => {
  let bus: any;
  let mediatorQueryOperation: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    mediatorQueryOperation = {
      mediate: (arg: any) => Promise.resolve({
        bindingsStream: new ArrayIterator([
          Bindings({ a: DF.literal('1') }),
          Bindings({ a: DF.literal('2') }),
          Bindings({ a: DF.literal('3') }),
        ]),
        metadata: () => Promise.resolve({ totalItems: 3 }),
        operated: arg,
        type: 'bindings',
        variables: [ 'a' ],
      }),
    };
  });

  describe('The ActorQueryOperationBgpSmartkg module', () => {
    it('should be a function', () => {
      expect(ActorQueryOperationBgpSmartkgAdapter).toBeInstanceOf(Function);
    });

    it('should be a ActorQueryOperationBgpSmartkg constructor', () => {
      expect(new (<any> ActorQueryOperationBgpSmartkgAdapter)({ name: 'actor', bus, mediatorQueryOperation }))
        .toBeInstanceOf(ActorQueryOperationBgpSmartkgAdapter);
      expect(new (<any> ActorQueryOperationBgpSmartkgAdapter)({ name: 'actor', bus, mediatorQueryOperation }))
        .toBeInstanceOf(ActorQueryOperation);
    });

    it('should not be able to create new ActorQueryOperationBgpSmartkg objects without \'new\'', () => {
      expect(() => { (<any> ActorQueryOperationBgpSmartkgAdapter)(); }).toThrow();
    });
  });
});
