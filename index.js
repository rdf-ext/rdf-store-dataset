import rdf from 'rdf-ext'
import promiseToEvent from 'rdf-utils-stream/promiseToEvent.js'

class Store {
  constructor ({ dataset, factory = rdf } = {}) {
    this.factory = factory
    this.dataset = this.factory.dataset(dataset)
  }

  async _import (stream, { truncate } = {}) {
    const other = await rdf.dataset().import(stream)

    if (truncate) {
      this.dataset = other
    } else {
      this.dataset.addAll(other)
    }
  }

  async _remove (stream) {
    const other = await rdf.dataset().import(stream)

    this.dataset = this.dataset.difference(other)
  }

  async _removeMatches (subject, predicate, object, graph) {
    this.dataset.deleteMatches(subject, predicate, object, graph)
  }

  deleteGraph (graph) {
    return this.removeMatches(null, null, null, graph)
  }

  import (stream, { truncate } = {}) {
    return promiseToEvent(this._import(stream, { truncate }))
  }

  match (subject, predicate, object, graph) {
    return this.dataset.match(subject, predicate, object, graph).toStream()
  }

  remove (stream) {
    return promiseToEvent(this._remove(stream))
  }

  removeMatches (subject, predicate, object, graph) {
    return promiseToEvent(this._removeMatches(subject, predicate, object, graph))
  }
}

export default Store
