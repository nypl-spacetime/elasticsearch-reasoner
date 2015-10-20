# histograph-reasoner

histograph-reasoner finds and creates relations between PITs from two datasets, based on their attributes (name, type, geographic location). The current version is for creating relations files on a client, but needs a local instance of Elasticsearch with Histograph data, as well as access to PIT NDJSON files.

A later version will run on server as a standalone agent, and automatically update relations when data sources change.

## Installation

    git clone https://github.com/histograph/reasoner.git
    npm install

## Usage

    node index.js tgn
    
Copyright (C) 2015 [Waag Society](http://waag.org).

