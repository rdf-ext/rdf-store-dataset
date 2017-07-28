const rdf = require('rdf-ext')

class Store {
  constructor (options) {
    options = options || {}

    this.factory = options.factory || rdf
    this.dataset = options.dataset || this.factory.dataset()
  }

  match (subject, predicate, object, graph) {
    return this.dataset.match(subject, predicate, object, graph).toStream()
  }

  import (stream, options) {
    options = options || {}

    return rdf.asEvent(() => {
      return rdf.dataset().import(stream).then((other) => {
        if (options.truncate) {
          this.dataset = other
        } else {
          this.dataset.addAll(other)
        }
      })
    })
  }

  remove (stream) {
    return rdf.asEvent(() => {
      return rdf.dataset().import(stream).then((other) => {
        this.dataset = this.dataset.difference(other)
      })
    })
  }

  removeMatches (subject, predicate, object, graph) {
    return rdf.asEvent(() => {
      return this.dataset.removeMatches(subject, predicate, object, graph)
    })
  }

  deleteGraph (graph) {
    return this.removeMatches(null, null, null, graph)
  }
}

module.exports = Store
