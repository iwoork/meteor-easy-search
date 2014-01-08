EasySearch = (function () {
    var indexes = {},
        config = {
            host: 'localhost',
            port: 9200,
            secure: false
        },
        conditions = {
            'onChangeProperty' : function () {
                return true;
            }
        },
        defaultQuery = function (searchFields, searchString) {
            return {
                "fuzzy_like_this" : {
                    "fields" : searchFields,
                    "like_text" : searchString
                }
            };
        },
        Future = Npm.require('fibers/future'),
        ElasticSearchInstance = Npm.require('elasticsearchclient'),
        ElasticSearchClient = new ElasticSearchInstance(config);

    /**
     * Return Elastic Search indexable data.
     *
     * @param {Object} doc
     * @returns {Object}
     */
    function getESFields(doc) {
        var newDoc = {};

        _.each(doc, function (value, key) {
            //newDoc[key] = "string" === typeof value ? value : JSON.stringify(value);
            // Force JSON
            newDoc[key] = value;
        });

        return newDoc;
    }

    /**
     * Write a document to a specified index.
     *
     * @param {String} name
     * @param {Object} doc
     * @param {String} id
     */
    function writeToIndex(name, doc, id) {
        // add to index
        doc._mid = id;
        ElasticSearchClient.index(name, 'document', doc, id)
            .on('data', function (data) {
                if (config.debug && console) {
                    console.log('EasySearch: Added / Replaced document to Elastic Search:');
                    console.log('EasySearch: ' + data + "\n");
                }
            })
            .exec();
    }

    return {
        /**
         * Override the config for Elastic Search.
         *
         * @param {object} newConfig
         */
        'config' : function (newConfig) {
            if ("undefined" === typeof newConfig) {
                return config;
            }

            config = _.extend(config, newConfig);
            ElasticSearchClient = new ElasticSearchInstance(config);
        },
        /**
         * Override conditions or return conditions if no parameter passed.
         *
         * @param newConditions
         * @returns {object}
         */
        'conditions' : function (newConditions) {
            if ("undefined" === typeof newConditions) {
                return conditions;
            }

            conditions = _.extend(conditions, newConditions);
        },
        /**
         * Create a search index for Elastic Search, which resembles a MongoDB Collection.
         *
         * @param {String} name
         * @param {Object} options
         */
        'createSearchIndex' : function (name, options) {
            options.format = "string" === typeof options.format ? options.format : "mongo";
            options.limit = "number" === typeof options.limit ? options.limit : 10;
            options.query = "function" === typeof options.query ? options.query : defaultQuery;
            options.facets = [];
            options.filters = [];

            indexes[name] = options;

            options.collection.find().observeChanges({
                added: function (id, fields) {
                    writeToIndex(name, getESFields(fields), id);
                },
                changed: function (id, fields) {
                    // Overwrites the current document with the new doc
                    writeToIndex(name, getESFields(options.collection.findOne(id)), id);
                },
                removed: function (id) {
                    ElasticSearchClient.deleteDocument(name, 'default_type', id)
                        .on('data', function (data) {
                            if (config.debug && console) {
                                console.log('EasySearch: Removed document off Elastic Search:');
                                console.log('EasySearch: ' + data + "\n");
                            }
                        })
                        .exec();
                }
            });
        },
        /**
         * Get a fake representation of a mongo document.
         *
         * @param {Object} data
         * @returns {Array}
         */
        'getMongoDocumentObject' : function (data) {
            data = _.isString(data) ? JSON.parse(data) : data;
            // data = JSON.parse(data);


            return _.map(data.hits.hits, function (resultSet) {
                var mongoDbDocFake = resultSet['_source'];

                mongoDbDocFake['_id'] = resultSet['_id'];
                return resultSet['_source'];
            });
        },
        /**
         * Perform a really simple search.
         *
         * @param {String} name
         * @param {String} searchString
         * @param {Array} fields
         * @param {Function} callback
         */
        'search' : function (name, searchString, fields, callback) {
            var queryObj,
                that = this,
                searchFields,
                fut = new Future(),
                index = indexes[name];

            if ("function" === typeof fields) {
                callback = fields;
                fields = [];
            }

            if (!_.isObject(index)) {
                return;
            }

            searchFields = _.isArray(index.field) ? index.field : [index.field];

            /*queryObj = {
                "query" : index.query(searchFields, searchString),
                "size" : index.limit
            };*/

            _facets = {}
            facets = _(index.facets).each(function(facet) {
                _facets[facet.title] = {
                    terms: facet.terms
                }
            });

            _filters = _(index.filters).map(function(filter) {
                // TODO:
                tmp = {}
                tmp[filter.field] = filter.term
                return {
                    term: tmp
                };
            });

            queryObj = index.query(searchString, _facets, _filters);

            // Append options
            queryObj = _.extend(queryObj, _.pick(index, [
                'size'
            ]));

            console.log(queryObj);

            if ("function" === typeof callback) {
                ElasticSearchClient.search(name, 'document', queryObj, callback);
                return;
            }

            // Most likely client call, return data set
            ElasticSearchClient.search(name, 'document', queryObj, function (error, data) {
                resultDetails = _.without(JSON.parse(data), 'hits');

                if ("mongo" === index.format) {
                    data = that.getMongoDocumentObject(data);
                }

                _data = {}
                _data.results = data;
                _data.resultDetails = resultDetails

                if (_.isArray(fields) && fields.length > 0) {
                    _data.results = _.map(_data.results, function (doc) {
                        var i,
                            newDoc = {};

                        for (i = 0; i < fields.length; i += 1) {
                            newDoc[fields[i]] = doc[fields[i]];
                        }

                        return newDoc;
                    });
                }

                fut['return'](_data);
            });

            return fut.wait();
        },
        /**
         * Change a property specified for the index.
         *
         * @param {String} name
         * @param {String} key
         * @param {String} value
         */
        'changeProperty' : function(name, key, value) {
            if (!_.isString(name) || !_.isString(key)) {
                throw new Meteor.Error('name and key of the property have to be strings!');
            }

            indexes[name][key] = value;
        },

        'getFacets' : function(index, callback) {
            EasySearch.search(index, '*', [], function(err, data) {
                console.log(data)
            });
        },
        //
        // @MODIFIED
        'addFacet': function(indexName, name, title, terms) {
            if(_(indexes[indexName]['facets']).findWhere({name: name})) {
                _(indexes[indexName]['facets']).map(function(facet) {
                    if(facet.name === name) {
                        facet.title = title
                        facet.terms = terms
                    }
                    return facet
                });
            } else {
                indexes[indexName]['facets'].push({
                    name: name,
                    title: title,
                    terms: terms
                });
            }
        },
        'addFilter': function(indexName, field, term) {
            existing = _(indexes[indexName]['filters']).findWhere({field: field, term: term});
            if(existing) {
                _(indexes[indexName]['filters']).map(function(filter) {
                    if(filter.term === term) {
                        filter.term = term
                    }
                    return filter
                });
            } else {
                indexes[indexName]['filters'].push({
                    field: field,
                    term: term
                });
            }
        },


        /**
         * Get the ElasticSearchClient
         * @see https://github.com/phillro/node-elasticsearch-client
         *
         * @return {ElasticSearchInstance}
         */
        'getElasticSearchClient' : function () {
            return ElasticSearchClient;
        }
    };
})();

Meteor.methods({
    /**
     * Make search possible on the client.
     *
     * @param {String} name
     * @param {String} searchString
     */
    easySearch: function (name, searchString) {
        return EasySearch.search(name, searchString);
    },
    /**
     * Make changing properties possible on the client.
     *
     * @param {String} name
     * @param {String} key
     * @param {String} value
     */
    easySearchChangeProperty: function(name, key, value) {
        if (EasySearch.conditions().onChangeProperty(name, key, value)) {
            EasySearch.changeProperty(name, key, value);
        }
    },
    easySearchAddFacet: function(indexName, name, title, terms) {
        EasySearch.addFacet(indexName, name, title, terms)
    },
    easySearchAddFacets: function(indexName, facets) {
        _.each(facets, function(facet) {
            EasySearch.addFacet(indexName, facet.name, facet.title, facet.terms)
        });
    },
    easySearchAddFilter: function(indexName, field, term) {
        EasySearch.addFilter(indexName, field, term)
    }
});
