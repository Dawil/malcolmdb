var Malcolm = {};
(function(Malcolm){
  var Result = function() {};
  var Query = function(config) {
    for (var i in config) {
      this[i] = config[i];
    }
  };

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
        return new QueryBuilder({
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
        return new QueryBuilder({
          collection: name,
          update: modification
        });
      };

      /**
       * Remove selected objects.
       */
      this.remove = function() {
        return new QueryBuilder({
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
    this.run = function(query, predicates, limit) {
      var result = new Result();
      for (var i in drivers) {
        var driverName = drivers[i].name;
        result[driverName] = drivers[i].run(query, predicates, limit);
      }
      return result;
    };

    var QueryBuilder = function(query) {
      var predicates = [];

      query.where = function(predicate) { predicates.push(predicate); return query; };

      query.limit = function(n) {
        return db.run(new Query(query),predicates, n);
      };
      query.first = function( ) { return query.limit( 1); };
      query.all   = function( ) { return query.limit(-1); };

      return query;
    };

    this.synchronizationManager = undefined;
  };
})(Malcolm);
