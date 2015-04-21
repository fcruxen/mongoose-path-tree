var Schema = require('mongoose').Schema;
var streamWorker = require('stream-worker');

module.exports = exports = tree;


/**
 * @class Tree
 * Tree Behavior for Mongoose
 *
 * Implements the materialized ancestry strategy with cascade child re-parenting
 * on delete for storing a hierarchy of documents with Mongoose
 *
 * @param  {Mongoose.Schema} schema
 * @param  {Object} options
 */
function tree(schema, options) {

    var ancestrySeparator = options && options.ancestrySeparator || '/'
        , wrapChildrenTree = options && options.wrapChildrenTree
        , onDelete = options && options.onDelete || 'DELETE' //'REPARENT'
        , numWorkers = options && options.numWorkers || 5
        , idType = options && options.idType || Schema.ObjectId
        , ancestrySeparatorRegex = '[' + ancestrySeparator + ']';

    /**
     * Add parent and ancestry properties
     *
     * @property {ObjectID} parent
     * @property {String} ancestry
     */
    schema.add({
        parent: {
            type: idType,
            set: function (val) {
                return (val instanceof Object && val._id) ? val._id : val;
            },
            index: true
        },
        ancestry: {
            type: String,
            index: true
        }
    });


    /**
     * Pre-save middleware
     * Build or rebuild ancestry when needed
     *
     * @param  {Function} next
     */
    schema.pre('save', function preSave(next) {
        var isParentChange = this.isModified('parent');

        if (this.isNew || isParentChange) {
            if (!this.parent) {
              return next();
            }

            var self = this;
            this.collection.findOne({ _id: this.parent }, function (err, doc) {

                if (err) {
                    return next(err);
                }

                var previousPath = self.ancestry
                if(doc.ancestry){
                  self.ancestry = doc.ancestry + ancestrySeparator + doc._id
                } else {
                  self.ancestry = doc._id
                }

                if (isParentChange) {
                    // When the parent is changed we must rewrite all children ancestrys as well
                    self.collection.find({ ancestry: { '$regex': '^' + previousPath + ancestrySeparatorRegex } }, function (err, cursor) {

                        if (err) {
                            return next(err);
                        }

                        streamWorker(cursor.stream(), numWorkers, function streamOnData(doc, done) {

                            var newPath = self.ancestry + doc.ancestry.substr(previousPath.length);
                            self.collection.update({ _id: doc._id }, { $set: { ancestry: newPath } }, done);
                        },
                        next);
                    });
                }
                else {
                    next();
                }
            });
        }
        else {
            next();
        }
    });


    /**
     * Pre-remove middleware
     *
     * @param  {Function} next
     */
    schema.pre('remove', function preRemove(next) {

        if (!this.ancestry)
            return next();

        if (onDelete == 'DELETE') {
            this.collection.remove({ ancestry: { '$regex': '^' + this.ancestry + ancestrySeparatorRegex } }, next);
        }
        else {
            var self = this,
                newParent = this.parent,
                previousParent = this._id;

            // Update parent property from children
            this.collection.find({ parent: previousParent }, function (err, cursor) {

                if (err) {
                    return next(err);
                }

                streamWorker(cursor.stream(), numWorkers, function streamOnData(doc, done) {

                        self.collection.update({ _id: doc._id }, { $set: { parent: newParent } }, done);
                },
                function streamOnClose(err) {

                    if (err) {
                        return next(err);
                    }

                    self.collection.find({ ancestry: { $regex: previousParent + ancestrySeparatorRegex} }, function (err, cursor) {

                        var subStream = cursor.stream();
                        streamWorker(subStream, numWorkers, function subStreamOnData(doc, done) {

                            var newPath = doc.ancestry.replace(previousParent + ancestrySeparator, '');
                            self.collection.update({ _id: doc._id }, { $set: { ancestry: newPath } }, done);
                        },
                        next);
                    });
                });
            });
        }
    });


    /**
     * @method getChildren
     *
     *         {Object}        filters (like for mongo find) (optional)
     *  {Object} or {String}   fields  (like for mongo find) (optional)
     *         {Object}        options (like for mongo find) (optional)
     * @param  {Boolean}       recursive, default false      (optional)
     * @param  {Function}      next
     * @return {Model}
     */
    schema.methods.getChildren = function getChildren(filters, fields, options, recursive, next) {

        // normalize the arguments
        if ('function' === typeof filters) {
            next = filters;
            filters = {};
        }
        else if ('function' === typeof fields) {
            next = fields;
            fields = null;

            if ('boolean' === typeof filters) {
                recursive = filters;
                filters = {}
            }
        }
        else if ('function' === typeof options) {
            next = options;
            options = {};

            if ('boolean' === typeof fields) {
                recursive = fields;
                fields = null;
            }
        }
        else if ('function' === typeof recursive) {
            next = recursive;

            if ('boolean' === typeof options) {
                recursive = options;
                options = {}
            }
            else {
                recursive = false
            }
        }

        filters = filters || {};
        fields = fields || null;
        options = options || {};
        recursive = recursive || false;

        if (recursive) {
            if(filters['$query']){
                filters['$query']['ancestry'] = {$regex: '^' + this.ancestry + ancestrySeparatorRegex};
            } else {
                filters['ancestry'] = {$regex: '^' + this.ancestry + ancestrySeparatorRegex};
            }
        } else {
            if(filters['$query']){
                filters['$query']['parent'] = this._id;
            } else {
                filters['parent'] = this._id;
            }
        }

        return this.model(this.constructor.modelName).find(filters, fields, options, next);
    };


    /**
     * @method getParent
     *
     * @param  {Function} next
     * @return {Model}
     */
    schema.methods.getParent = function getParent(next) {

        return this.model(this.constructor.modelName).findOne({ _id: this.parent }, next);
    };


    /**
     * @method getAncestors
     *
     * @param  {Object}   args
     * @param  {Function} next
     * @return {Model}
     */
    schema.methods.getAncestors = function getAncestors(filters, fields, options, next) {

        if ('function' === typeof filters) {
            next = filters;
            filters = {};
        }
        else if ('function' === typeof fields) {
            next = fields;
            fields = null;
        }
        else if ('function' === typeof options) {
            next = options;
            options = {};
        }

        filters = filters || {};
        fields = fields || null;
        options = options || {};

        var ids = [];

        if (this.ancestry) {
            ids = this.ancestry.split(ancestrySeparator);
            ids.pop();
        }

        if(filters['$query']){
            filters['$query']['_id'] = {$in: ids};
        } else {
            filters['_id'] = {$in: ids};
        }

        return this.model(this.constructor.modelName).find(filters, fields, options, next);
    };


    /**
     * @method getChildrenTree
     *
     * @param  {Document} root (optional)
     * @param  {Object}   args (optional)
     *         {Object}        .filters (like for mongo find)
     *  {Object} or {String}   .fields  (like for mongo find)
     *         {Object}        .options (like for mongo find)
     *         {Number}        .minLevel, default 1
     *         {Boolean}       .recursive
     *         {Boolean}       .allowEmptyChildren
     * @param  {Function} next
     * @return {Model}
     */
    schema.statics.getChildrenTree = function getChildrenTree(root, args, next) {

        if ("function" === typeof(root))
        {
            next = root;
            root = null;
            args = {};
        }
        else if ("function" === typeof(args)) {
            next = args;

            if ("model" in root) {
                args = {};
            }
            else
            {
                args = root;
                root = null
            }
        }

        var filters = args.filters || {};
        var fields = args.fields || null;
        var options = args.options || {};
        var minLevel = args.minLevel || 1;
        var recursive = args.recursive != undefined ? args.recursive : true;
        var allowEmptyChildren = args.allowEmptyChildren != undefined ? args.allowEmptyChildren : true;

        if (!next)
            throw new Error('no callback defined when calling getChildrenTree');

        // filters: Add recursive ancestry filter or not
        if (recursive) {
            if (root) {
                filters.ancestry = { $regex: '^' + root.ancestry + ancestrySeparatorRegex };
            }

            if (filters.parent === null) {
                delete filters.parent;
            }

        } else {
            if (root) {
                filters.parent = root._id;
            }
            else {
                filters.parent = null
            }
        }

        // fields: Add ancestry and parent in the result if not already specified
        if (fields) {
            if (fields instanceof Object) {
                if (!fields.hasOwnProperty('ancestry')) {
                    fields['ancestry'] = 1;
                }
                if (!fields.hasOwnProperty('parent')) {
                    fields['parent'] = 1;
                }
            }
            else {
                if (!fields.match(/ancestry/)) {
                    fields += ' ancestry';
                }
                if (!fields.match(/parent/)) {
                    fields += ' parent';
                }
            }
        }

        // options:sort , ancestry sort is mandatory
        if (!options.sort) {
            options.sort = {};
        }
        options.sort.ancestry = 1;

        if (options.lean == null) {
            options.lean = !wrapChildrenTree;
        }

        return this.find(filters, fields, options, function (err, results) {

            if (err) {
                return next(err);
            }

            var getLevel = function (ancestry) {

                return ancestry ? ancestry.split(ancestrySeparator).length : 0;
            };

            var createChildren = function createChildren(arr, node, level) {

                if (level == minLevel) {
                    if (allowEmptyChildren) {
                        node.children = [];
                    }
                    return arr.push(node);
                }

                var nextIndex = arr.length - 1;
                var myNode = arr[nextIndex];

                if (!myNode) {
                    //console.log("Tree node " + node.name + " filtered out. Level: " + level + " minLevel: " + minLevel);
                    return []
                } else {
                    createChildren(myNode.children, node, level - 1);
                }
            };

            var finalResults = [];
            var rootLevel = 1;

            if (root) {
                rootLevel = getLevel(root.ancestry) + 1;
            }

            if (minLevel < rootLevel) {
                minLevel = rootLevel
            }

            for (var r in results) {
                var level = getLevel(results[r].ancestry);
                createChildren(finalResults, results[r], level);
            }

            next(err, finalResults);

        });
    };


    schema.methods.getChildrenTree = function(args, next) {

        this.constructor.getChildrenTree(this, args, next)
    };


    /**
     * @property {Number} level <virtual>
     */
    schema.virtual('level').get(function virtualPropLevel() {

        return this.ancestry ? this.ancestry.split(ancestrySeparator).length : 0;
    });
}