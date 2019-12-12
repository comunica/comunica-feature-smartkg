import {ActorQueryOperation, Bindings} from "@comunica/bus-query-operation";
import {Bus} from "@comunica/core";
import {literal} from "@rdfjs/data-model";
import {ArrayIterator} from "asynciterator";
import {ActorQueryOperationBgpSmartkgAdapter} from "../lib/ActorQueryOperationBgpSmartkgAdapter";

describe('ActorQueryOperationBgpSmartkg', () => {
  let bus;
  let mediatorQueryOperation;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    mediatorQueryOperation = {
      mediate: (arg) => Promise.resolve({
        bindingsStream: new ArrayIterator([
          Bindings({ a: literal('1') }),
          Bindings({ a: literal('2') }),
          Bindings({ a: literal('3') }),
        ]),
        metadata: () => Promise.resolve({ totalItems: 3 }),
        operated: arg,
        type: 'bindings',
        variables: ['a'],
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
