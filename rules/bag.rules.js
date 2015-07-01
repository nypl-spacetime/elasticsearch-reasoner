module.exports = {
  tgn: [
    {
      types: {
        from: 'hg:Place',
        to: 'hg:Place'
      },
      textDistance: 2,
      geoDistance: 10000,
      relation: 'hg:sameHgConcept',
      filter: null
    },
  ]
}