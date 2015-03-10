var Malcolm = {};
(function(Malcolm){
  var Result = function() {};

  /**
   * Constructor for a driver to manage db persistence
   */
  Malcolm.driver = function(name,driver) {
    this.name = name;
    for (var i in driver) {
      this[i] = driver[i];
    }
  };

  /**
   * Constructor for new db.
   */
  Malcolm.db = function() {
    var db = this;
    var drivers = [];
    var buffers = {};

    /**
     * Constructor for a new collection.
     * @param name Name of collection
     */
    this.collection = function(name) {

      /**
       * Insert object into collection.
       * @param obj Object to insert
       * @return a query object
       */
      this.insert = function(obj) {
        return db.run(new Query({
          collection: name,
          insert: arguments
        }));
      };

      /**
       * Select object(s) from collection.
       * @param shape Shape of object to select or undefined for the full object.
       * @return a query object
       *
       * e.g. Given:
       *  var shape = { name: { first: true } }
       *  collection.select(shape)
       *
       * The query will only return the parts of the object that fit the shape.
       * This is useful when retrieving all of the object from a remote location
       * can be expensive.
       */
      this.select = function(shape) {
        return new Query({
          collection: name,
          select: shape
        });
      };

      /**
       * Updates the selected objects
       * @param modification Either an object or a function. See below.
       * @return Optional value to be used as object replacement.
       *
       * If `modification` is a function then it takes one argument, the object
       * to be updated, and modifies it in place. Alternatively it can return a
       * result if the object is a value not a reference.
       *
       * If `modification` is an object then it  takes an object shape to
       * overlay on top of the object.
       */
      this.update = function(modification) {
        return new Query({
          collection: name,
          update: modification
        });
      };

      /**
       * Remove selected objects.
       */
      this.remove = function() {
        return new Query({
          collection: name,
          remove: true
        });
      };
    };

    /**
     * Method for adding a new db driver.
     */
    this.addDriver = function(name, driverConstructor) {
      var driver = new driverConstructor();
      drivers.push(new Malcolm.driver(name,driver));
      return db;
    };

    /**
     * Method for passing a query to the drivers.
     * @param query Query to give to all the drivers
     * @return Result
     */
    this.run = function(query) {
      var result = new Result();
      for (var i in drivers) {
        var driverName = drivers[i].name;
        if ('label' in query.config && drivers[i].buffer) {
          // let the buffer manager handle it
          var label = query.config.label;
          if (!buffers[label]) {
            buffers[label] = new Buffer(drivers[i], query);
          }
          result[driverName] = buffers[label].getPromise();
        } else {
          result[driverName] = drivers[i].run(query);
        }
      }
      return result;
    };

    function Buffer(driver, query) {
      this.driver = driver;
      this.query = query;
      this.q = Q.defer();
      this.timeoutID = setTimeout(this.flush.bind(this),100);
    }
    Buffer.prototype.getPromise = function() { return this.q.promise; };
    Buffer.prototype.flush = function() {
      this.q.resolve(this.driver.run(this.query));
      delete buffers[this.query.config.label];
    };

    function Query(config) {
      this.predicates = [];
      this.config = config;
      this.limitTo = -1;
    }
    Query.prototype.as = function(label) { // label for buffering purposes
      this.config.label = label;
      return this;
    };
    Query.prototype.where = function(predicate) {
      this.predicates.push(predicate);
      return this;
    };
    Query.prototype.limit = function(n) {
      this.limitTo = n;
      return db.run(this);
    };
    Query.prototype.first = function( ) { return this.limit( 1); };
    Query.prototype.all   = function( ) { return this.limit(-1); };

  };
})(Malcolm);
