import { strictEqual } from 'node:assert'
import { isReadableStream } from 'is-stream'
import { describe, it } from 'mocha'
import rdf from 'rdf-ext'
import { datasetEqual } from 'rdf-test/assert.js'
import eventToPromise from 'rdf-utils-stream/eventToPromise.js'
import DatasetStore from '../index.js'
import * as ns from './support/namespaces.js'

const example = {}

example.dataset = rdf.dataset([
  rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object1, ns.ex.graph1),
  rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object2, ns.ex.graph2)
])
example.datasetDefaultGraph = rdf.dataset(example.dataset, rdf.defaultGraph())

describe('rdf-store-dataset', () => {
  it('should be a constructor', () => {
    strictEqual(typeof DatasetStore, 'function')
  })

  it('should assign the given factory', () => {
    const factory = {
      dataset: () => {}
    }
    const store = new DatasetStore({ factory })

    strictEqual(store.factory, factory)
  })

  it('should import the given dataset into the wrapped dataset', () => {
    const store = new DatasetStore({ dataset: example.dataset })

    datasetEqual(store.dataset, example.dataset)
  })

  describe('.deleteGraph', () => {
    it('should be a method', () => {
      const store = new DatasetStore()

      strictEqual(typeof store.deleteGraph, 'function')
    })

    it('should remove all quads with the given graph', async () => {
      const expected = example.dataset.match(null, null, ns.ex.object2)
      const store = new DatasetStore({ dataset: example.dataset })

      await eventToPromise(store.deleteGraph(ns.ex.graph1))

      datasetEqual(store.dataset, expected)
    })
  })

  describe('.import', () => {
    it('should be a method', () => {
      const store = new DatasetStore()

      strictEqual(typeof store.import, 'function')
    })

    it('should add the quads given as stream', async () => {
      const store = new DatasetStore()

      await eventToPromise(store.import(example.dataset.toStream()))

      datasetEqual(store.dataset, example.dataset)
    })

    it('should clear the dataset if truncate is true', async () => {
      const store = new DatasetStore({
        dataset: rdf.dataset([rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object)])
      })

      await eventToPromise(store.import(example.dataset.toStream(), { truncate: true }))

      datasetEqual(store.dataset, example.dataset)
    })
  })

  describe('.match', () => {
    it('should be a method', () => {
      const store = new DatasetStore()

      strictEqual(typeof store.match, 'function')
    })

    it('should return a Readable stream', () => {
      const store = new DatasetStore()

      const result = store.match()

      strictEqual(isReadableStream(result), true)
    })

    it('should return all matching quads via the stream', async () => {
      const expected = example.dataset.match(null, null, ns.ex.object2)
      const store = new DatasetStore({ dataset: example.dataset })

      const result = await rdf.dataset().import(store.match(null, null, ns.ex.object2))

      datasetEqual(result, expected)
    })
  })

  describe('.remove', () => {
    it('should be a method', () => {
      const store = new DatasetStore()

      strictEqual(typeof store.remove, 'function')
    })

    it('should remove all quads given via the stream', async () => {
      const expected = example.dataset.match(null, null, ns.ex.object2)
      const remove = example.dataset.match(null, null, ns.ex.object1)
      const store = new DatasetStore({ dataset: example.dataset })

      await eventToPromise(store.remove(remove.toStream()))

      datasetEqual(store.dataset, expected)
    })
  })

  describe('.removeMatches', () => {
    it('should be a method', () => {
      const store = new DatasetStore()

      strictEqual(typeof store.removeMatches, 'function')
    })

    it('should remove all matching quads', async () => {
      const expected = example.dataset.match(null, null, ns.ex.object2)
      const store = new DatasetStore({ dataset: example.dataset })

      await eventToPromise(store.removeMatches(null, null, ns.ex.object1))

      datasetEqual(store.dataset, expected)
    })
  })
})
