/**
 * @copyright Maichong Software Ltd. 2016 http://maichong.it
 * @date 2016-03-13
 * @author Liang <liang@maichong.it>
 */

'use strict';

const MongoClient = require('mongodb').MongoClient;

class MongoCacheDriver {
  constructor(options) {
    let me = this;
    this._maxAge = options.maxAge || 86400 * 365;
    this._connecting = MongoClient.connect(options.url, {
      uri_decode_auth: options.uri_decode_auth,
      db: options.db,
      server: options.server,
      replSet: options.replSet,
      mongos: options.mongos
    });
    this._connecting.then(function (db) {
      me._connecting = null;
      me._driver = db.collection(options.collection || 'mongo_cache');
    }, function (error) {
      console.error(error.stack);
      process.exit(1);
    });
    this.type = 'mongo';
    //标识已经是缓存对象实例
    this.isCacheDriver = true;
    //标识本驱动不会序列化数据
    this.noSerialization = false;
  }

  driver() {
    return this._driver;
  }

  set(key, value, lifetime) {
    if (this._connecting) {
      return this._connecting.then(() => {
        return this.set(key, value, lifetime);
      });
    }
    let expiredAt = new Date(Date.now() + (lifetime || this._maxAge) * 1000);

    return this._driver.findOneAndReplace({
      _id: key
    }, {
      _id: key,
      value: value,
      expiredAt: expiredAt
    }, {
      upsert: true,
      returnOriginal: false
    });
  }

  get(key) {
    if (this._connecting) {
      return this._connecting.then(() => {
        return this.get(key);
      });
    }
    return this._driver.findOne({
      _id: key
    }).then(function (doc) {
      if (!doc) {
        return Promise.resolve();
      }
      if (!doc.expiredAt || doc.expiredAt < new Date) {
        //已过期
        return this.del(key);
      }
      return Promise.resolve(doc.value);
    });
  }

  del(key) {
    if (this._connecting) {
      return this._connecting.then(() => {
        return this.del(key);
      });
    }
    return this._driver.deleteOne({
      _id: key
    }).then(function () {
      return Promise.resolve();
    });
  }

  has(key) {
    if (this._connecting) {
      return this._connecting.then(() => {
        return this.has(key);
      });
    }
    return this._driver.findOne({
      _id: key
    }).then(function (doc) {
      if (!doc) {
        return Promise.resolve(false);
      }
      if (!doc.expiredAt || doc.expiredAt < new Date) {
        //已过期
        return this.del(key);
      }
      return Promise.resolve(true);
    });
  }

  size() {
    if (this._connecting) {
      return this._connecting.then(function () {
        return this.size();
      });
    }
    return this._driver.count();
  }

  flush() {
    if (this._connecting) {
      return this._connecting.then(function () {
        return this.flush();
      });
    }
    return this._driver.drop();
  }
}

MongoCacheDriver.default = MongoCacheDriver;

module.exports = MongoCacheDriver;
