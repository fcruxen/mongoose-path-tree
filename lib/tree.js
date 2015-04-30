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

  


     

  /**
  * @property {Number} level <virtual>
  */
  schema.virtual('level').get(function virtualPropLevel() {

    return this.ancestry ? this.ancestry.split(ancestrySeparator).length : 0;
  });
}