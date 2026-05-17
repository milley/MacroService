// GENERATED CODE -- DO NOT EDIT!

'use strict';
var grpc = require('@grpc/grpc-js');
var kv_pb = require('./kv_pb.js');

function serialize_kv_DeleteRequest(arg) {
  if (!(arg instanceof kv_pb.DeleteRequest)) {
    throw new Error('Expected argument of type kv.DeleteRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_kv_DeleteRequest(buffer_arg) {
  return kv_pb.DeleteRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_kv_DeleteResponse(arg) {
  if (!(arg instanceof kv_pb.DeleteResponse)) {
    throw new Error('Expected argument of type kv.DeleteResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_kv_DeleteResponse(buffer_arg) {
  return kv_pb.DeleteResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_kv_GetRequest(arg) {
  if (!(arg instanceof kv_pb.GetRequest)) {
    throw new Error('Expected argument of type kv.GetRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_kv_GetRequest(buffer_arg) {
  return kv_pb.GetRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_kv_GetResponse(arg) {
  if (!(arg instanceof kv_pb.GetResponse)) {
    throw new Error('Expected argument of type kv.GetResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_kv_GetResponse(buffer_arg) {
  return kv_pb.GetResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_kv_PutRequest(arg) {
  if (!(arg instanceof kv_pb.PutRequest)) {
    throw new Error('Expected argument of type kv.PutRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_kv_PutRequest(buffer_arg) {
  return kv_pb.PutRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_kv_PutResponse(arg) {
  if (!(arg instanceof kv_pb.PutResponse)) {
    throw new Error('Expected argument of type kv.PutResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_kv_PutResponse(buffer_arg) {
  return kv_pb.PutResponse.deserializeBinary(new Uint8Array(buffer_arg));
}


// 客户端 KV 存储服务
var KVServiceService = exports.KVServiceService = {
  get: {
    path: '/kv.KVService/Get',
    requestStream: false,
    responseStream: false,
    requestType: kv_pb.GetRequest,
    responseType: kv_pb.GetResponse,
    requestSerialize: serialize_kv_GetRequest,
    requestDeserialize: deserialize_kv_GetRequest,
    responseSerialize: serialize_kv_GetResponse,
    responseDeserialize: deserialize_kv_GetResponse,
  },
  put: {
    path: '/kv.KVService/Put',
    requestStream: false,
    responseStream: false,
    requestType: kv_pb.PutRequest,
    responseType: kv_pb.PutResponse,
    requestSerialize: serialize_kv_PutRequest,
    requestDeserialize: deserialize_kv_PutRequest,
    responseSerialize: serialize_kv_PutResponse,
    responseDeserialize: deserialize_kv_PutResponse,
  },
  delete: {
    path: '/kv.KVService/Delete',
    requestStream: false,
    responseStream: false,
    requestType: kv_pb.DeleteRequest,
    responseType: kv_pb.DeleteResponse,
    requestSerialize: serialize_kv_DeleteRequest,
    requestDeserialize: deserialize_kv_DeleteRequest,
    responseSerialize: serialize_kv_DeleteResponse,
    responseDeserialize: deserialize_kv_DeleteResponse,
  },
};

exports.KVServiceClient = grpc.makeGenericClientConstructor(KVServiceService, 'KVService');
