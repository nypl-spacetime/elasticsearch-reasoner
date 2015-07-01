module.exports = {
  geonames: [
    {
      types: {
        from: 'hg:Place',
        to: 'hg:Place'
      },
      geoDistance: 4000,
      textDistance: 2,
      relation: 'hg:sameHgConcept',
      filter: function(pit) {
        // Do not use TGN term objects
        return pit.uri.indexOf('term') == -1;
      }
    },

    {
      types: {
        from: 'hg:Province',
        to: 'hg:Province'
      },
      textDistance: 1,
      relation: 'hg:sameHgConcept',
      filter: function(pit) {
        // Do not use TGN term objects
        return pit.uri.indexOf('term') == -1;
      },
      override: [
        // Force inferred relation between the following three provinces:
        // (This may not be needed later when proper ES tokenizer is in place)
        {
          from: 'http://vocab.getty.edu/tgn/7003632',
          to: 'http://sws.geonames.org/2743698'
        },
        {
          from: 'http://vocab.getty.edu/tgn/7003624',
          to: 'http://sws.geonames.org/2749990'
        },
        {
          from: 'http://vocab.getty.edu/tgn/7006951',
          to: 'http://sws.geonames.org/2749879'
        }
      ]
    },

    {
      types: {
        from: 'hg:Country',
        to: 'hg:Country'
      },
      textDistance: 0,
      name: function(pit) {
        return 'Kingdom of the Netherlands';
      },
      relation: 'hg:sameHgConcept',
      filter: function(pit) {
        // Do not use TGN term objects
        return pit.uri.indexOf('term') == -1;
      }
    }

  ]
}
