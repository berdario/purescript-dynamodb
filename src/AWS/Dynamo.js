"use strict"

var AWS = require('aws-sdk')
var documentClient = new AWS.DynamoDB.DocumentClient();

exports.invokeForeign = function(name, params, errcb, cb){
    return function(){
        documentClient[name](params, function(err, data){
            if (err){
                errcb(new Error(err + err.stack))()
            } else {
                cb(data)()
            }
        })
    }
}