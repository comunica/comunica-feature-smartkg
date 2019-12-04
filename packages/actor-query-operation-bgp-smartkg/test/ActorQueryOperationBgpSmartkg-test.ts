import {ActorQueryOperation, Bindings} from "@comunica/bus-query-operation";
import {Bus} from "@comunica/core";
import {ArrayIterator} from "asynciterator";
import {literal, variable} from "@rdfjs/data-model";
import {ActorQueryOperationBgpSmartkgAdapter} from "../lib/ActorQueryOperationBgpSmartkg";
const arrayifyStream = require('arrayify-stream');

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

  describe('An ActorQueryOperationBgpSmartkg instance', () => {
    let actor: ActorQueryOperationBgpSmartkgAdapter;

    beforeEach(() => {
      actor = new ActorQueryOperationBgpSmartkgAdapter({ name: 'actor', bus, mediatorQueryOperation });
    });

    it('should test on bgp', () => {
      const op = { operation: { type: 'bgp' } };
      return expect(actor.test(op)).resolves.toBeTruthy();
    });

    it('should not test on non-bgp', () => {
      const op = { operation: { type: 'some-other-type' } };
      return expect(actor.test(op)).rejects.toBeTruthy();
    });

    it('should run', () => {
      const op = { operation: { type: 'bgp' } };
      return expect(actor.run(op)).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
