var H = require('highland')
var normalizer = require('histograph-uri-normalizer')
var elasticsearch = require('histograph-db-elasticsearch')
var turf = {
  centroid: require('turf-centroid')
}

function isFunction (functionToCheck) {
  var getType = {}
  return functionToCheck && getType.toString.call(functionToCheck) === '[object Function]'
}

// Expand Histograph URNs
function expandURN (id) {
  try {
    id = normalizer.URNtoURL(id)
  } catch (e) {
    // TODO: use function from uri-normalizer
    id = id.replace('urn:hgid:', '')
  }

  return id
}

function executeQuery (query, data, callback) {
  try {
    var rule = data.rule
    var pit = data.pit
    var urn = normalizer.normalize(pit.id || pit.uri, data.ruleDatasetId)

    // Check if PIT's URI or ID is present in rule's override list
    if (data.normalizedOverride[urn] !== undefined) {
      // if (data.normalizedOverride[urn]) {
      //
      //   var relation = {
      //     from: getUriOrId(data.dataset, pit.uri, pit.id),
      //     to: data.normalizedOverride[urn].to,
      //     type: rule.relation
      //   }
      //
      //   var result = {
      //     relation: relation,
      //     from: pit,
      //     to: {
      //       override: data.normalizedOverride[urn].to
      //     }
      //   }
      //
      //   callback(null, result)
      //
      // } else {
      //   callback()
      // }
    } else {
      // Create Elasticsearch query
      var centroid
      if (pit.geometry) {
        centroid = turf.centroid(pit.geometry).geometry.coordinates
      }

      query.query.filtered.filter.bool.must = [
        {
          term: {
            type: rule.types.to
          }
        }
      ]

      if (rule.geoDistance && centroid) {
        query.query.filtered.filter.bool.must.push({
          geo_distance: {
            distance: `${rule.geoDistance}m`,
            centroid: centroid
          }
        })
      }

      // TODO: stop als wel georule maar geen centroid!    //
      // TODO: alles in rules: of default, of constant, of function(pit)

      var name
      if (isFunction(rule.name)) {
        name = rule.name(pit)
      } else if (rule.name) {
        name = rule.name
      } else {
        name = pit.name
      }

      // TODO: Oh ja??
      name = name.replace(')', '').replace('(', '')

      var textDistance = rule.textDistance ? rule.textDistance : 0
      query.query.filtered.query.query_string.query = `"${name}"~${textDistance}`

      elasticsearch.query({
        index: data.ruleDatasetId,
        // type: 'hg:Place',
        body: query
      }, (err, results) => {
        if (err) {
          callback(err)
          return
        }

        if (results.hits.hits.length > 0) {
          var hit = results.hits.hits[0]

          var relation = {
            from: expandURN(pit.uri || pit.id),
            to: expandURN(hit._source.uri || hit._source.id),
            type: rule.relation
          }

          callback(null, {
            type: 'relation',
            obj: relation
          })
        } else {
          // TODO: log het log het LOG HET
          callback()
        }
      })
    }
  } catch (err) {
    callback(err)
  }
}

module.exports = function (options) {
  const rules = options.rules
  const baseQuery = require('./query.json')

  return (pit) => {
    return H(Object.keys(rules)).map((ruleDatasetId) => {
      return H(rules[ruleDatasetId])
        .filter((rule) => {
          // Filter on PIT type for which rule is defined
          return rule.types.from === pit.type ||
          (rule.types.from.constructor === Array && rule.types.from.indexOf(pit.type) > -1)
        })
        .filter((rule) => {
          // Apply the rule's general filter function (if defined)
          if (rule.filter && isFunction(rule.filter)) {
            return rule.filter(pit)
          }

          return true
        })
        .map((rule) => {
          // Normalize URLs and IDs in rule's override list to URNs
          // using histograph-uri-normalizer
          var normalizedOverride = {}
          if (rule.override) {
            //     rule.override.forEach(function(o) {
            //       var from = normalizer.normalize(o.from, obj.dataset)
            //       var to = {
            //         to: o.to
            //       }
            //
            //       if (o.to) {
            //         to.urn = normalizer.normalize(o.to, ruleDatasetid)
            //       }
            //
            //       normalizedOverride[from] = to
            //     })
          }

          return {
            dataset: 'vissen', // obj.dataset,
            pit: pit,
            ruleDatasetId: ruleDatasetId,
            rule: rule,
            normalizedOverride: normalizedOverride
          }
        })
    })
      .flatten()
      .map(H.curry(executeQuery, Object.assign({}, baseQuery)))
      .nfcall([])
      .series()
  }
}
